package replication

import (
	"encoding/binary"
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type replyRepairRuntime struct {
	faults      []protocol.Checksum
	buffer      []byte
	handle      IOHandle
	kind        IOKind
	header      protocol.Header
	plan        SessionCommitPlan
	message     *Message
	requested   protocol.Header
	nextRequest uint64
	delay       uint64
	cursor      uint32
	scanning    bool
}

func (replica *Replica) initReplyRepair() error {
	buffer, err := NewAlignedBuffer(replica.wal.Layout().ReplyStride, SectorSize)
	if err != nil {
		return err
	}
	replica.replyRepair = replyRepairRuntime{
		faults: make([]protocol.Checksum, replica.config.Cluster.ClientsMax),
		buffer: buffer, scanning: true,
	}
	return nil
}

func (replica *Replica) markReplyFault(header protocol.Header) {
	record, slot, found := replica.sessions.lookup(replyClient(&header))
	if !found || record.Reply.HeaderChecksum != header.HeaderChecksum || header.Size == protocol.HeaderSize {
		return
	}
	replica.replyRepair.faults[slot] = header.HeaderChecksum
	if replica.membership.ActiveCount+replica.membership.StandbyCount == 1 {
		replica.fail(ErrInvalidReplyStore)
	}
}

func (replica *Replica) markReplyIdentityFault(client protocol.ClientID, op protocol.Op, checksum protocol.Checksum) {
	header, _, found := replica.sessions.RepairReply(client, op, checksum)
	if found {
		replica.markReplyFault(header)
	}
}

func (replica *Replica) replyRepairBlocksCommit() bool {
	return replica.replyRepair.kind == IOReplyWrite
}

func (replica *Replica) continueReplyRepair(now uint64) bool {
	repair := &replica.replyRepair
	waitingForBlocks := replica.stateSync.repairing && !replica.stateSync.repliesPending
	waitingForReplay := replica.stateSync.replayRunning || (replica.stateSync.repliesPending && replica.commitMin < max(replica.stateSync.commit, replica.commitMax))
	if replica.stateSync.stage != SyncStageIdle || waitingForBlocks || waitingForReplay || repair.handle != (IOHandle{}) {
		return false
	}
	if repair.scanning {
		for repair.cursor < uint32(len(replica.sessions.records)) {
			slot := repair.cursor
			record := replica.sessions.records[slot]
			if replica.sessions.busy[slot] {
				return false
			}
			if record.Session == 0 || record.Reply.Size == protocol.HeaderSize {
				repair.cursor++
				continue
			}
			handle, err := replica.io.Submit(IOOperation{Kind: IOReplyRead, Offset: uint64(slot), Buffer: repair.buffer, ReplyStore: replica.replies, ExpectedHeader: record.Reply})
			if err != nil {
				return false
			}
			repair.cursor++
			repair.handle, repair.kind, repair.header = handle, IOReplyRead, record.Reply
			return true
		}
		repair.scanning = false
	}
	for slot, checksum := range repair.faults {
		if checksum.IsZero() {
			continue
		}
		record := replica.sessions.records[slot]
		if record.Session == 0 || record.Reply.HeaderChecksum != checksum {
			repair.faults[slot] = protocol.Checksum{}
			continue
		}
		if replica.sessions.busy[slot] {
			return false
		}
		if repair.requested.HeaderChecksum == checksum && now < repair.nextRequest {
			return false
		}
		if repair.requested.HeaderChecksum != checksum {
			repair.delay = uint64(min(max(replica.config.Process.InitialRTT, replica.config.Process.BackoffMin), replica.config.Process.BackoffMax))
		}
		memberCount := replica.membership.ActiveCount + replica.membership.StandbyCount
		if memberCount <= 1 {
			replica.fail(ErrInvalidReplyStore)
			return false
		}
		peer := protocol.ReplicaIndex(replica.random.Uniform(uint64(memberCount - 1)))
		if peer >= replica.local {
			peer++
		}
		message, err := replica.frames.Acquire(0)
		if err != nil {
			return false
		}
		header := protocol.Header{Group: replica.config.Group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandGetReply, Author: replica.local}
		copy(header.Fields[:16], checksum[:])
		client := replyClient(&record.Reply)
		copy(header.Fields[32:48], client[:])
		binary.LittleEndian.PutUint64(header.Fields[48:56], uint64(replyOp(&record.Reply)))
		if err := message.Seal(&header); err != nil {
			message.Release()
			return false
		}
		repair.requested = record.Reply
		repair.nextRequest = saturatingAdd(now, repair.delay)
		repair.delay = min(repair.delay*2, uint64(replica.config.Process.BackoffMax))
		replica.deps.MessageBus.SendReplica(peer, message)
		message.Release()
		return true
	}
	repair.requested = protocol.Header{}
	if replica.stateSync.repliesPending {
		replica.stateSync.repliesPending = false
		replica.stateSync.repairing = false
		replica.syncRangeRepaired = true
	}
	return false
}

func (replica *Replica) handleReplyRepair(message *Message, header protocol.Header) bool {
	repair := &replica.replyRepair
	if replica.stateSync.stage != SyncStageIdle || repair.handle != (IOHandle{}) || replica.stage != CommitStageIdle || repair.requested.HeaderChecksum != header.HeaderChecksum {
		return false
	}
	expected, slot, found := replica.sessions.RepairReply(replyClient(&header), replyOp(&header), header.HeaderChecksum)
	if !found || repair.faults[slot] != header.HeaderChecksum || expected.Size == protocol.HeaderSize {
		return false
	}
	frame, err := message.Bytes()
	if err != nil {
		return false
	}
	decoded, _, reason := protocol.DecodeFrame(frame, replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), replica.membership.ActiveCount+replica.membership.StandbyCount)
	if reason != protocol.RejectNone || decoded.HeaderChecksum != expected.HeaderChecksum {
		return false
	}
	session, _ := replica.sessions.Session(replyClient(&header))
	plan, err := replica.sessions.PlanCommit(expected, session)
	if err != nil {
		return false
	}
	// Reserve the slot until the physical write finishes, not merely until its callback becomes obsolete.
	repair.faults[slot] = protocol.Checksum{}
	handle, err := replica.io.Submit(IOOperation{Kind: IOReplyWrite, Offset: uint64(slot), Buffer: frame, ReplyStore: replica.replies})
	if err != nil {
		replica.sessions.Abort(plan)
		repair.faults[slot] = header.HeaderChecksum
		return false
	}
	repair.handle, repair.kind, repair.header = handle, IOReplyWrite, expected
	repair.plan, repair.message = plan, message
	return true
}

func (replica *Replica) handleReplyRepairIO(completion IOCompletion) bool {
	repair := &replica.replyRepair
	if repair.handle == (IOHandle{}) || repair.handle != completion.Handle || repair.kind != completion.Kind {
		return false
	}
	header, kind := repair.header, repair.kind
	repair.handle, repair.kind, repair.header = IOHandle{}, 0, protocol.Header{}
	if kind == IOReplyWrite {
		replica.sessions.Abort(repair.plan)
		repair.plan = SessionCommitPlan{}
		repair.message.Release()
		repair.message = nil
		repair.requested = protocol.Header{}
		if completion.Err != nil {
			replica.fail(completion.Err)
		}
	} else if !errors.Is(completion.Err, ErrIOCanceled) && (completion.Err != nil || completion.Size != uint64(header.Size)) {
		replica.markReplyFault(header)
	}
	return true
}

func (replica *Replica) releaseReplyRepair() {
	repair := &replica.replyRepair
	if repair.message != nil {
		replica.sessions.Abort(repair.plan)
		repair.message.Release()
	}
	buffer, faults := repair.buffer, repair.faults
	clear(faults)
	*repair = replyRepairRuntime{buffer: buffer, faults: faults}
}
