package replication

import (
	"encoding/binary"
	"errors"

	"z.gfire.dev/supervsr/replication/protocol"
)

func (replica *Replica) tickTimers(sample TimeSample) {
	if replica.tickTimer(&replica.timers.ping) {
		replica.sendPing(sample)
		replica.timers.ping.Reset()
	}
	if replica.tickTimer(&replica.timers.prepare) {
		replica.handlePrepareTimeout()
	}
	if replica.primaryStalled(sample) {
		if !replica.abdicated && replica.tickTimer(&replica.timers.abdication) {
			replica.handleAbdicationTimeout(sample)
			replica.timers.abdication.Reset()
		}
	} else {
		replica.abdicated = false
		replica.timers.abdication.Reset()
	}
	commitFired := replica.tickTimer(&replica.timers.commit)
	if replica.status == StatusNormal && replica.isPrimary() && !replica.abdicated {
		if commitFired || replica.failureDetector.Level(sample.Monotonic) >= failureYellow {
			replica.sendCommit(sample)
			replica.timers.commit.Reset()
		}
	} else if commitFired {
		replica.timers.commit.Reset()
	}
	if replica.tickTimer(&replica.timers.exit) {
		replica.handleExitTimeout(sample)
		replica.timers.exit.Reset()
	}
	if replica.tickTimer(&replica.timers.join) {
		replica.handleJoinTimeout()
	}
	if replica.viewChangeProgressTracked() {
		if replica.tickTimer(&replica.timers.viewChange) {
			replica.handleViewChangeTimeout(sample)
			replica.timers.viewChange.Reset()
		}
	} else {
		replica.viewChangeStalled = false
		replica.timers.viewChange.Reset()
	}
	if replica.tickTimer(&replica.timers.pulse) {
		replica.handlePulseTimeout(sample)
		replica.timers.pulse.Reset()
	}
	if replica.tickTimer(&replica.timers.repair) {
		replica.handleRepairTimeout(sample)
		replica.timers.repair.Reset()
	}
}

func (replica *Replica) primaryStalled(sample TimeSample) bool {
	if replica.status != StatusNormal || !replica.isPrimary() || replica.durableView != replica.view || replica.membership.ActiveCount <= 1 {
		return false
	}
	return (replica.pipelineLen > 0 && !replica.pipelineEntry(0).quorum) || !sample.Synchronized
}

func (replica *Replica) viewChangeProgressTracked() bool {
	if uint8(replica.local) >= replica.membership.ActiveCount {
		return false
	}
	if replica.status == StatusViewChange && replica.durableView == replica.view {
		return true
	}
	return replica.status == StatusRecoveringHead && !replica.stateSyncAwaitingOpen()
}

func (replica *Replica) tickTimer(timeout *tickTimeout) bool {
	fired, err := timeout.Tick()
	if err != nil {
		replica.fail(err)
		return false
	}
	return fired
}

func (replica *Replica) handlePrepareTimeout() {
	if replica.status != StatusNormal || !replica.isPrimary() || replica.pipelineLen == 0 || replica.abdicated {
		replica.timers.prepare.Reset()
		return
	}
	for offset := range replica.pipelineLen {
		entry := replica.pipelineEntry(offset)
		if entry.quorum {
			continue
		}
		for member := range replica.membership.ActiveCount {
			bit := uint16(1) << member
			if entry.acks&bit == 0 && protocol.ReplicaIndex(member) != replica.local {
				replica.deps.MessageBus.SendReplica(protocol.ReplicaIndex(member), entry.prepare)
			}
		}
	}
	if err := replica.timers.prepare.Backoff(replica.config.Process, replica.config.Process.InitialRTT, &replica.random); err != nil {
		replica.fail(err)
	}
}

func (replica *Replica) handleAbdicationTimeout(sample TimeSample) {
	if !replica.primaryStalled(sample) {
		return
	}
	replica.abdicated = true
	if replica.metrics != nil {
		replica.metrics.primaryAbdications.Add(1)
		if replica.pipelineLen > 0 && !replica.pipelineEntry(0).quorum {
			replica.metrics.quorumUnavailable.Add(1)
		}
		if !sample.Synchronized {
			replica.metrics.clockDisagreements.Add(1)
		}
	}
	replica.sendExitView(sample.Monotonic)
}

func (replica *Replica) handleJoinTimeout() {
	if replica.status != StatusViewChange || replica.durableView != replica.view {
		replica.timers.join.Reset()
		return
	}
	replica.sendJoinView()
	if err := replica.timers.join.Backoff(replica.config.Process, replica.config.Process.InitialRTT, &replica.random); err != nil {
		replica.fail(err)
	}
}

func (replica *Replica) handleViewChangeTimeout(sample TimeSample) {
	if !replica.viewChangeProgressTracked() {
		return
	}
	replica.viewChangeStalled = true
	replica.sendExitView(sample.Monotonic)
}

func (replica *Replica) handleExitTimeout(sample TimeSample) {
	if uint8(replica.local) >= replica.membership.ActiveCount {
		return
	}
	switch replica.status {
	case StatusNormal:
		if replica.isPrimary() {
			if replica.abdicated {
				replica.sendExitView(sample.Monotonic)
			}
			return
		}
		if replica.failureDetector.Level(sample.Monotonic) == failureRed {
			replica.sendExitView(sample.Monotonic)
		}
	case StatusViewChange, StatusRecoveringHead:
		if replica.viewChangeStalled && replica.viewChangeProgressTracked() {
			replica.sendExitView(sample.Monotonic)
		}
	}
}

func (replica *Replica) handlePulseTimeout(sample TimeSample) {
	if replica.status != StatusNormal || !replica.isPrimary() || replica.durableView != replica.view || replica.abdicated {
		return
	}
	if replica.membership.ActiveCount > 1 && !sample.Synchronized {
		replica.metrics.clockDisagreements.Add(1)
		return
	}
	if replica.upgradeTarget != 0 {
		if err := replica.createUpgrade(sample); err != nil && !transientControlAdmission(err) {
			replica.fail(err)
		}
		return
	}
	if replica.pipelineLen == uint32(len(replica.pipeline)) || !replica.deps.StateMachine.PulseNeeded(replica.lastPrepareTimestamp) {
		return
	}
	for offset := range replica.pipelineLen {
		if prepareOperation(&replica.pipelineEntry(offset).header) == protocol.OperationPulse {
			return
		}
	}
	if err := replica.createPulse(sample); err != nil && !transientControlAdmission(err) {
		replica.fail(err)
	}
}

func transientControlAdmission(err error) bool {
	return errors.Is(err, ErrReplicaBackpressure) ||
		errors.Is(err, ErrIOBackpressure) ||
		errors.Is(err, protocol.ErrFramePoolEmpty)
}

func (replica *Replica) sendPing(sample TimeSample) {
	bodySize := uint32(replica.config.Cluster.ReleaseHistoryMax * 4)
	message, err := replica.frames.Acquire(bodySize)
	if err != nil {
		return
	}
	body, err := message.Body()
	if err != nil {
		message.Release()
		return
	}
	for index, release := range replica.releaseHistory {
		binary.LittleEndian.PutUint32(body[index*4:], uint32(release))
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.durableView, Release: replica.config.CurrentRelease, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing, Author: replica.local}
	copy(header.Fields[:16], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(replica.checkpoint.PrepareOp()))
	binary.LittleEndian.PutUint64(header.Fields[24:32], max(uint64(1), sample.Monotonic))
	binary.LittleEndian.PutUint16(header.Fields[32:34], uint16(len(replica.releaseHistory)))
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.BroadcastReplicas(message)
	}
	message.Release()
}

func (replica *Replica) handlePing(header protocol.Header, body []byte) {
	replica.observePeerCheckpoint(header.Author, protocol.Op(binary.LittleEndian.Uint64(header.Fields[16:24])))
	replica.observeReleaseReport(header, body)
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	response := protocol.Header{Group: replica.config.Group, View: replica.durableView, Release: replica.config.CurrentRelease, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPong, Author: replica.local}
	copy(response.Fields[:8], header.Fields[24:32])
	binary.LittleEndian.PutUint64(response.Fields[8:16], max(uint64(1), replica.deps.Clock.Now().Wall))
	if message.Seal(&response, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.SendReplica(header.Author, message)
	}
	message.Release()
}

func (replica *Replica) handlePong(header protocol.Header, sample TimeSample) {
	pingMonotonic := binary.LittleEndian.Uint64(header.Fields[:8])
	if replica.metrics != nil && sample.Monotonic >= pingMonotonic {
		replica.metrics.peerRTTNanos.Store(sample.Monotonic - pingMonotonic)
	}
	if replica.clockObserver == nil {
		return
	}
	_ = replica.clockObserver.Observe(header.Author, pingMonotonic, binary.LittleEndian.Uint64(header.Fields[8:16]), sample.Monotonic)
}

func (replica *Replica) sendCommit(sample TimeSample) {
	headerAtCommit, _, _, found := replica.localHeaderEvidence(replica.commitMin)
	if !found {
		replica.fail(ErrReplicaInvariant)
		return
	}
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandCommit, Author: replica.local}
	copy(header.Fields[:16], headerAtCommit.HeaderChecksum[:])
	copy(header.Fields[32:48], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[48:56], uint64(replica.checkpoint.PrepareOp()))
	binary.LittleEndian.PutUint64(header.Fields[56:64], uint64(replica.commitMin))
	binary.LittleEndian.PutUint64(header.Fields[64:72], max(uint64(1), sample.Monotonic))
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.deps.MessageBus.BroadcastReplicas(message)
		replica.failureDetector.Signal(sample.Monotonic)
	}
	message.Release()
}

func (replica *Replica) sendExitView(now uint64) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: replica.local}
	if message.Seal(&header, replica.config.Cluster.ChecksumAlgorithm) == nil {
		replica.handleExitView(header, now)
		replica.deps.MessageBus.BroadcastReplicas(message)
	}
	message.Release()
}

func (replica *Replica) createPulse(sample TimeSample) error {
	prepare, err := replica.frames.Acquire(0)
	if err != nil {
		return err
	}
	timestamp := max(uint64(1), sample.Wall, replica.lastPrepareTimestamp+1, replica.lastCommitTimestamp+1)
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Release: replica.config.CurrentRelease, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPrepare, Author: replica.local}
	copy(header.Fields[:16], replica.headChecksum[:])
	copy(header.Fields[64:80], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[96:104], uint64(replica.headOp+1))
	binary.LittleEndian.PutUint64(header.Fields[104:112], uint64(replica.commitMin))
	binary.LittleEndian.PutUint64(header.Fields[112:120], timestamp)
	header.Fields[124] = byte(protocol.OperationPulse)
	if err := prepare.Seal(&header, replica.config.Cluster.ChecksumAlgorithm); err != nil {
		prepare.Release()
		return err
	}
	if !replica.acceptPrepare(prepare, header) {
		prepare.Release()
		return ErrReplicaBackpressure
	}
	replica.metrics.preparesCreated.Add(1)
	replica.deps.MessageBus.BroadcastReplicas(prepare)
	replica.failureDetector.Signal(sample.Monotonic)
	return nil
}
