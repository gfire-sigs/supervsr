package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrReleaseUnavailable      = errors.New("replication: checkpoint release is unavailable")
	ErrReleaseExecutorReturned = errors.New("replication: release executor returned")
)

type ReleaseExecutor interface {
	Releases() []protocol.Release
	Execute(target protocol.Release) error
}

type releaseReport struct {
	valid    bool
	view     protocol.View
	count    uint16
	releases []protocol.Release
}

type recoveredUpgradeState struct {
	target protocol.Release
	window upgradeWindow
}

func (state *recoveredUpgradeState) observe(config ClusterConfig, checkpoint protocol.Op, checkpointRelease protocol.Release, header protocol.Header, body []byte) error {
	op := prepareOp(&header)
	operation := prepareOperation(&header)
	if operation == protocol.OperationUpgrade {
		if len(body) != 16 {
			return ErrWALRecovery
		}
		target := protocol.Release(binary.LittleEndian.Uint32(body[:4]))
		if target != checkpointRelease {
			if target < checkpointRelease || target <= header.Release {
				return ErrWALRecovery
			}
			if state.target != 0 && state.target != target {
				return ErrWALRecovery
			}
			state.target = target
		}
	}
	next, ok := checkpointAfter(config, checkpoint)
	if !ok {
		return ErrWALRecovery
	}
	trigger, ok := checkpointTrigger(config, next)
	if !ok {
		return ErrWALRecovery
	}
	if op <= next || op > trigger {
		return nil
	}
	if !state.window.started {
		state.window = upgradeWindow{checkpoint: next, valid: op == next+1, started: true}
	}
	if !state.window.valid || operation != protocol.OperationUpgrade {
		state.window.valid = false
		return nil
	}
	target := protocol.Release(binary.LittleEndian.Uint32(body[:4]))
	if state.window.target == 0 {
		state.window.target = target
	}
	if target == 0 || target != state.window.target {
		state.window.valid = false
	}
	return nil
}

func executableReleases(config Config, executor ReleaseExecutor) ([]protocol.Release, error) {
	if executor == nil {
		return []protocol.Release{config.CurrentRelease}, nil
	}
	source := executor.Releases()
	if len(source) == 0 || len(source) > int(config.Cluster.ReleaseHistoryMax) {
		return nil, ErrReleaseUnavailable
	}
	releases := make([]protocol.Release, len(source))
	copy(releases, source)
	containsCurrent := false
	var previous protocol.Release
	for _, release := range releases {
		if release == 0 || release <= previous {
			return nil, ErrReleaseUnavailable
		}
		containsCurrent = containsCurrent || release == config.CurrentRelease
		previous = release
	}
	if !containsCurrent {
		return nil, ErrReleaseUnavailable
	}
	return releases, nil
}

func containsRelease(releases []protocol.Release, target protocol.Release) bool {
	for _, release := range releases {
		if release == target {
			return true
		}
	}
	return false
}

func (replica *Replica) refreshLocalReleaseReport() {
	if uint8(replica.local) >= replica.membership.ActiveCount {
		return
	}
	report := &replica.releaseReports[replica.local]
	clear(report.releases)
	copy(report.releases, replica.releaseHistory)
	report.valid = true
	report.view = replica.view
	report.count = uint16(len(replica.releaseHistory))
}

func (replica *Replica) invalidateReleaseReports() {
	for index := range replica.releaseReports {
		report := &replica.releaseReports[index]
		report.valid = false
		report.view = 0
		report.count = 0
		clear(report.releases)
	}
}

func (replica *Replica) observeReleaseReport(header protocol.Header, body []byte) {
	if uint8(header.Author) >= replica.membership.ActiveCount || header.View != replica.view || replica.status != StatusNormal {
		return
	}
	count := binary.LittleEndian.Uint16(header.Fields[32:34])
	report := &replica.releaseReports[header.Author]
	clear(report.releases)
	for index := range int(count) {
		report.releases[index] = protocol.Release(binary.LittleEndian.Uint32(body[index*4:]))
	}
	report.valid = true
	report.view = header.View
	report.count = count
	replica.maybeSelectUpgrade()
}

func (replica *Replica) maybeSelectUpgrade() {
	if !replica.isPrimary() || replica.status != StatusNormal || replica.upgradeTarget != 0 {
		return
	}
	replica.refreshLocalReleaseReport()
	for index := len(replica.releaseHistory) - 1; index >= 0; index-- {
		target := replica.releaseHistory[index]
		if target <= replica.config.CurrentRelease {
			continue
		}
		present := true
		for member := range replica.membership.ActiveCount {
			report := &replica.releaseReports[member]
			if !report.valid || report.view != replica.view || !containsRelease(report.releases[:report.count], target) {
				present = false
				break
			}
		}
		if present {
			replica.upgradeTarget = target
			return
		}
	}
}

func (replica *Replica) createUpgrade(sample TimeSample) error {
	if replica.upgradeTarget == 0 || replica.status != StatusNormal || !replica.isPrimary() || replica.durableView != replica.view {
		return ErrReplicaInvariant
	}
	prepare, err := replica.frames.Acquire(16)
	if err != nil {
		return err
	}
	body, err := prepare.Body()
	if err != nil {
		prepare.Release()
		return err
	}
	binary.LittleEndian.PutUint32(body[:4], uint32(replica.upgradeTarget))
	timestamp := max(uint64(1), sample.Wall, replica.lastPrepareTimestamp+1, replica.lastCommitTimestamp+1)
	header := protocol.Header{
		Group: replica.config.Group, View: replica.view, Release: replica.config.CurrentRelease,
		Protocol: protocol.ProtocolVersion, Command: protocol.CommandPrepare, Author: replica.local,
	}
	copy(header.Fields[:16], replica.headChecksum[:])
	copy(header.Fields[64:80], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(replica.headOp+1))
	binary.LittleEndian.PutUint64(header.Fields[104:112], uint64(replica.commitMin))
	binary.LittleEndian.PutUint64(header.Fields[112:120], timestamp)
	header.Fields[124] = byte(protocol.OperationUpgrade)
	if err := prepare.Seal(&header); err != nil {
		prepare.Release()
		return err
	}
	if !replica.acceptPrepare(prepare, header) {
		prepare.Release()
		return ErrReplicaBackpressure
	}
	replica.metrics.preparesCreated.Add(1)
	replica.deps.MessageBus.BroadcastReplicas(prepare)
	return nil
}

func (replica *Replica) executeUpgrade(entry *pipelineEntry) error {
	frame, err := entry.prepare.Bytes()
	if err != nil {
		return err
	}
	target := protocol.Release(binary.LittleEndian.Uint32(frame[protocol.HeaderSize : protocol.HeaderSize+4]))
	if target == replica.checkpoint.Release || target == replica.config.CurrentRelease {
		return nil
	}
	if target < replica.config.CurrentRelease || !containsRelease(replica.releaseHistory, target) {
		return ErrReleaseUnavailable
	}
	if replica.upgradeTarget != 0 && replica.upgradeTarget != target {
		return ErrReplicaInvariant
	}
	replica.upgradeTarget = target
	return nil
}

func (replica *Replica) trackUpgradeWindow(entry *pipelineEntry) {
	checkpoint, ok := checkpointAfter(replica.config.Cluster, replica.checkpoint.PrepareOp())
	if !ok {
		replica.fail(ErrReplicaInvariant)
		return
	}
	trigger, ok := checkpointTrigger(replica.config.Cluster, checkpoint)
	if !ok {
		replica.fail(ErrReplicaInvariant)
		return
	}
	op := prepareOp(&entry.header)
	if op <= checkpoint || op > trigger {
		return
	}
	if !replica.upgradeWindow.started || replica.upgradeWindow.checkpoint != checkpoint {
		replica.upgradeWindow = upgradeWindow{checkpoint: checkpoint, valid: op == checkpoint+1, started: true}
	}
	if !replica.upgradeWindow.valid || prepareOperation(&entry.header) != protocol.OperationUpgrade {
		replica.upgradeWindow.valid = false
		return
	}
	frame, err := entry.prepare.Bytes()
	if err != nil {
		replica.fail(err)
		return
	}
	target := protocol.Release(binary.LittleEndian.Uint32(frame[protocol.HeaderSize : protocol.HeaderSize+4]))
	if replica.upgradeWindow.target == 0 {
		replica.upgradeWindow.target = target
	}
	if target == 0 || target != replica.upgradeWindow.target {
		replica.upgradeWindow.valid = false
	}
}

func (replica *Replica) checkpointRelease(target protocol.Op) protocol.Release {
	window := replica.upgradeWindow
	if window.started && window.valid && window.checkpoint == target && window.target == replica.upgradeTarget {
		return window.target
	}
	return replica.config.CurrentRelease
}

func (replica *Replica) beginReleaseActivation(target protocol.Release) {
	if replica.releaseActivation != 0 || target == replica.config.CurrentRelease {
		replica.fail(ErrReplicaInvariant)
		return
	}
	replica.accepting.Store(false)
	replica.popPipeline()
	replica.stage = CommitStageIdle
	replica.releaseActivation = target
	replica.releaseResetGeneration++
	replica.releaseReset.prepare(replica.releaseResetGeneration, replica)
	result, err := replica.deps.StateMachine.StartReset(&replica.releaseReset)
	if err != nil {
		replica.releaseReset.release(replica.releaseResetGeneration)
		replica.fail(errors.Join(ErrStateMachine, err))
		return
	}
	if result.IsReady() {
		replica.releaseReset.release(replica.releaseResetGeneration)
		replica.releaseResetDone = true
	}
}

func (replica *Replica) processReleaseActivation(limit int) (int, error) {
	processed := 0
	drained := false
	for processed < limit {
		var completion IOCompletion
		if replica.io.Poll(&completion) {
			processed++
			continue
		}
		var event replicaEvent
		if !replica.events.TryPop(&event) {
			if replica.submitters.Load() != 0 {
				break
			}
			if !replica.events.TryPop(&event) {
				drained = true
				break
			}
		}
		switch event.kind {
		case replicaEventMessage:
			event.message.Release()
		case replicaEventSMCompletion:
			result, ok := event.completion.take(event.generation)
			if event.completion == &replica.releaseReset && event.generation == replica.releaseResetGeneration {
				if !ok || result.Kind != SMCompletionReset || result.Err != nil {
					replica.fail(errors.Join(ErrStateMachine, result.Err))
				} else {
					replica.releaseResetDone = true
				}
			}
		default:
			replica.fail(ErrReplicaInvariant)
		}
		processed++
	}
	if replica.releaseReadyToExecute(drained) {
		target := replica.releaseActivation
		replica.releaseOwnedFrames()
		replica.executeRelease(target)
	}
	return processed, replica.fatalErr
}

func (replica *Replica) releaseReadyToExecute(eventsDrained bool) bool {
	if replica.fatalErr != nil || !eventsDrained || !replica.releaseResetDone {
		return false
	}
	if !replica.io.Drained() {
		return false
	}
	return replica.submitters.Load() == 0
}

func (replica *Replica) executeRelease(target protocol.Release) {
	executor := replica.deps.ReleaseExecutor
	if executor == nil || !containsRelease(replica.releaseHistory, target) {
		replica.fail(ErrReleaseUnavailable)
		return
	}
	if err := executor.Execute(target); err != nil {
		replica.fail(errors.Join(ErrReleaseUnavailable, err))
		return
	}
	replica.fail(ErrReleaseExecutorReturned)
}
