package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type clockSampleObserver interface {
	Observe(source protocol.ReplicaIndex, pingMonotonic, peerWall, receiveMonotonic uint64) error
}

func (replica *Replica) tickTimers(sample TimeSample) {
	if replica.tickTimer(&replica.timers.ping) {
		replica.sendPing(sample)
		replica.timers.ping.Reset()
	}
	if replica.tickTimer(&replica.timers.prepare) {
		replica.handlePrepareTimeout()
	}
	if replica.tickTimer(&replica.timers.abdication) {
		replica.handleAbdicationTimeout()
		replica.timers.abdication.Reset()
	}
	if replica.tickTimer(&replica.timers.commit) {
		if replica.status == StatusNormal && replica.isPrimary() {
			replica.sendCommit(sample)
		}
		replica.timers.commit.Reset()
	}
	if replica.tickTimer(&replica.timers.exit) {
		replica.handleExitTimeout(sample)
		replica.timers.exit.Reset()
	}
	if replica.tickTimer(&replica.timers.join) {
		if replica.status == StatusViewChange && replica.durableView == replica.view {
			replica.sendJoinView()
		}
		replica.timers.join.Reset()
	}
	if replica.tickTimer(&replica.timers.pulse) {
		replica.handlePulseTimeout(sample)
		replica.timers.pulse.Reset()
	}
}

func (replica *Replica) tickTimer(timeout *Timeout) bool {
	fired, err := timeout.Tick()
	if err != nil {
		replica.fail(err)
		return false
	}
	return fired
}

func (replica *Replica) handlePrepareTimeout() {
	if replica.status != StatusNormal || !replica.isPrimary() || replica.pipelineLen == 0 {
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

func (replica *Replica) handleAbdicationTimeout() {
	if replica.status == StatusNormal && replica.isPrimary() && replica.pipelineLen > 0 && !replica.pipelineEntry(0).quorum {
		replica.sendExitView()
	}
}

func (replica *Replica) handleExitTimeout(sample TimeSample) {
	if replica.status != StatusNormal || replica.isPrimary() {
		return
	}
	if replica.failureDetector.Level(sample.Monotonic) == FailureRed {
		replica.sendExitView()
	}
}

func (replica *Replica) handlePulseTimeout(sample TimeSample) {
	if replica.status != StatusNormal || !replica.isPrimary() || replica.durableView != replica.view {
		return
	}
	if replica.membership.ActiveCount > 1 && !sample.Synchronized {
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
	if err := replica.createPulse(sample); err != nil && err != ErrReplicaBackpressure && err != ErrIOBackpressure {
		replica.fail(err)
	}
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
	binary.LittleEndian.PutUint32(body[:4], uint32(replica.config.CurrentRelease))
	header := protocol.Header{Group: replica.config.Group, View: replica.durableView, Release: replica.config.CurrentRelease, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPing, Author: replica.local}
	copy(header.Fields[:16], replica.checkpointID[:])
	binary.LittleEndian.PutUint64(header.Fields[16:24], uint64(replica.checkpoint.PrepareOp()))
	binary.LittleEndian.PutUint64(header.Fields[24:32], max(uint64(1), sample.Monotonic))
	binary.LittleEndian.PutUint16(header.Fields[32:34], 1)
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.BroadcastReplicas(message)
	}
	message.Release()
}

func (replica *Replica) handlePing(header protocol.Header) {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	response := protocol.Header{Group: replica.config.Group, View: replica.durableView, Release: replica.config.CurrentRelease, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPong, Author: replica.local}
	copy(response.Fields[:8], header.Fields[24:32])
	binary.LittleEndian.PutUint64(response.Fields[8:16], max(uint64(1), replica.deps.Clock.Now().Wall))
	if message.Seal(&response) == nil {
		replica.deps.MessageBus.SendReplica(header.Author, message)
	}
	message.Release()
}

func (replica *Replica) handlePong(header protocol.Header, sample TimeSample) {
	observer, ok := replica.deps.Clock.(clockSampleObserver)
	if !ok {
		return
	}
	_ = observer.Observe(header.Author, binary.LittleEndian.Uint64(header.Fields[:8]), binary.LittleEndian.Uint64(header.Fields[8:16]), sample.Monotonic)
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
	if message.Seal(&header) == nil {
		replica.deps.MessageBus.BroadcastReplicas(message)
	}
	message.Release()
}

func (replica *Replica) sendExitView() {
	message, err := replica.frames.Acquire(0)
	if err != nil {
		return
	}
	header := protocol.Header{Group: replica.config.Group, View: replica.view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: replica.local}
	if message.Seal(&header) == nil {
		replica.handleExitView(header)
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
