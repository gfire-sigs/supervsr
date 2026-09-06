package replication

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func replyRecoveryFixture(t testing.TB) (*Replica, *crashStorage, *captureBus) {
	t.Helper()
	cluster := compactTestClusterConfig()
	cluster.ClientsMax = 1
	membership := Membership{Members: [MembersMax]MemberID{{1}, {2}}, ActiveCount: 2, LocalMember: MemberID{2}}
	config := Config{Group: GroupID{9}, Membership: membership, Cluster: cluster, Process: DefaultProcessConfig(), CurrentRelease: 1, ClientReleaseMin: 1}
	storage := &crashStorage{}
	base, _ := cluster.BlockBase()
	if err := storage.Resize(base); err != nil {
		t.Fatal(err)
	}
	wal, err := NewWAL(storage, cluster, config.Group, 2)
	if err != nil {
		t.Fatal(err)
	}
	replies, err := NewReplyStore(storage, cluster, config.Group, 2)
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := newOpenSessionTable(config)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := newIOEngine(storage, 4, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := protocol.NewFramePool(4, uint32(cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := NewAlignedBuffer(wal.Layout().ReplyStride, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	bus := &captureBus{}
	replica := &Replica{config: config, membership: membership, local: 1, view: 1, durableView: 1, logView: 1, status: StatusNormal,
		deps: Dependencies{Storage: storage, MessageBus: bus, Clock: fixedClock{}, StateMachine: &testStateMachine{}},
		wal:  wal, replies: replies, sessions: sessions, io: engine, frames: frames, metrics: &ReplicaMetrics{},
		duplicateReads: []duplicateRead{{buffer: buffer}}, random: NewDeterministicRandom(7),
		releaseReports: make([]releaseReport, 2),
	}
	if err := replica.initReplyRepair(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := engine.Close(context.Background()); err != nil {
			t.Error(err)
		}
		replica.releaseReplyRepair()
	})
	return replica, storage, bus
}

func completeReplyRecoveryIO(t testing.TB, replica *Replica) IOCompletion {
	t.Helper()
	var completion IOCompletion
	if !replica.io.Poll(&completion) {
		t.Fatal("reply IO completion missing")
	}
	if !replica.handleReplyRepairIO(completion) {
		t.Fatal("reply repair did not own completion")
	}
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	return completion
}

func TestLatestReplyRepairRestoresDuplicateWithoutExecution(t *testing.T) {
	for _, damage := range []string{"missing", "corrupt"} {
		t.Run(damage, func(t *testing.T) {
			replica, storage, bus := replyRecoveryFixture(t)
			frame, header := makeReplyFrame(t, protocol.ClientID{7}, 5, []byte("committed value"))
			slot, err := replica.sessions.Commit(header, 1)
			if err != nil {
				t.Fatal(err)
			}
			if damage == "corrupt" {
				if err := replica.replies.Write(slot, frame); err != nil {
					t.Fatal(err)
				}
				offset, _ := replica.replies.slotOffset(slot)
				storage.working[offset+protocol.HeaderSize] ^= 0x80
			}
			replica.sendCachedReply(replyClient(&header), 1, replyRequest(&header))
			var duplicate IOCompletion
			if !replica.io.Poll(&duplicate) {
				t.Fatal("duplicate read missing")
			}
			replica.finishDuplicateRead(&replica.duplicateReads[0], duplicate)
			if bus.clientCount() != 0 {
				t.Fatal("returned a corrupt cached reply")
			}
			replica.replyRepair.scanning = false
			if !replica.continueReplyRepair(uint64(time.Second)) {
				t.Fatal("missing reply did not request repair")
			}
			request, _, reason := protocol.DecodeFrame(bus.replicaMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 2)
			if reason != protocol.RejectNone || request.Command != protocol.CommandGetReply || protocol.Checksum(request.Fields[:16]) != header.HeaderChecksum || protocol.ClientID(request.Fields[32:48]) != replyClient(&header) || binary.LittleEndian.Uint64(request.Fields[48:56]) != 5 {
				t.Fatalf("repair request identity: %+v, rejection=%v", request, reason)
			}
			message := makeReplicaCommand(t, replica.frames, header, frame[protocol.HeaderSize:])
			if !replica.handleReplyRepair(message, header) {
				message.Release()
				t.Fatal("exact reply rejected")
			}
			if _, _, ready := replica.sessions.Reply(replyClient(&header), 1, replyRequest(&header)); ready {
				t.Fatal("reply published before durable completion")
			}
			completeReplyRecoveryIO(t, replica)
			replica.sendCachedReply(replyClient(&header), 1, replyRequest(&header))
			if !replica.io.Poll(&duplicate) {
				t.Fatal("repaired duplicate read missing")
			}
			replica.finishDuplicateRead(&replica.duplicateReads[0], duplicate)
			got, body, reason := protocol.DecodeFrame(bus.clientMessage(t, 0), replica.config.Group, uint32(replica.config.Cluster.MessageSizeMax), 2)
			if reason != protocol.RejectNone || !bytes.Equal(body, frame[protocol.HeaderSize:]) || got.View != 1 || got.Author != 1 || replyContext(&got) != replyContext(&header) {
				t.Fatalf("repaired duplicate header=%+v body=%q rejection=%v", got, body, reason)
			}
			stored, err := replica.replies.Read(slot, header, replica.replyRepair.buffer)
			if err != nil || !bytes.Equal(stored, frame) {
				t.Fatalf("original durable identity changed: %v", err)
			}
			if machine := replica.deps.StateMachine.(*testStateMachine); machine.commits != 0 {
				t.Fatalf("duplicate executed %d times", machine.commits)
			}
		})
	}
}

func TestReplyRepairReservesSlotUntilCompletion(t *testing.T) {
	replica, _, _ := replyRecoveryFixture(t)
	frame, header := makeReplyFrame(t, protocol.ClientID{7}, 5, []byte("old reply"))
	if _, err := replica.sessions.Commit(header, 1); err != nil {
		t.Fatal(err)
	}
	replica.replyRepair.scanning = false
	replica.markReplyFault(header)
	replica.continueReplyRepair(uint64(time.Second))
	message := makeReplicaCommand(t, replica.frames, header, frame[protocol.HeaderSize:])
	if !replica.handleReplyRepair(message, header) {
		message.Release()
		t.Fatal("repair rejected")
	}
	newFrame, newer := makeReplyFrame(t, protocol.ClientID{8}, 6, []byte("next session"))
	if _, err := replica.sessions.PlanCommit(newer, 6); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("slot reused during write: %v", err)
	}
	replica.markReplyFault(header)
	oldCompletion := completeReplyRecoveryIO(t, replica)
	if replica.replyRepair.faults[0] != header.HeaderChecksum {
		t.Fatal("old completion erased a newer fault")
	}
	plan, err := replica.sessions.PlanCommit(newer, 6)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.replies.Write(plan.Slot, newFrame); err != nil {
		t.Fatal(err)
	}
	if err := replica.sessions.CommitAt(plan, newer, 6); err != nil {
		t.Fatal(err)
	}
	if replica.handleReplyRepairIO(oldCompletion) {
		t.Fatal("stale completion accepted after slot reuse")
	}
	late := makeReplicaCommand(t, replica.frames, header, frame[protocol.HeaderSize:])
	if replica.handleReplyRepair(late, header) {
		t.Fatal("stale response overwrote next session")
	}
	late.Release()
	stored, err := replica.replies.Read(plan.Slot, newer, replica.replyRepair.buffer)
	if err != nil || !bytes.Equal(stored, newFrame) {
		t.Fatalf("next session reply damaged: %v", err)
	}
}

func TestStateSyncWaitsForLatestReplyBeforePublishingRepairedContent(t *testing.T) {
	replica, _, _ := replyRecoveryFixture(t)
	frame, header := makeReplyFrame(t, protocol.ClientID{7}, 5, []byte("precheckpoint reply"))
	if _, err := replica.sessions.Commit(header, 1); err != nil {
		t.Fatal(err)
	}
	replica.stateSync.repairing = true
	replica.status = StatusRecovering
	replica.finishStateSyncOpen()
	completeReplyRecoveryIO(t, replica)
	replica.continueReplyRepair(uint64(time.Second))
	if replica.syncRangeRepaired || !replica.stateSync.repairing {
		t.Fatal("missing reply declared repaired")
	}
	message := makeReplicaCommand(t, replica.frames, header, frame[protocol.HeaderSize:])
	if !replica.handleReplyRepair(message, header) {
		message.Release()
		t.Fatal("repair rejected")
	}
	completeReplyRecoveryIO(t, replica)
	replica.continueReplyRepair(uint64(2 * time.Second))
	if !replica.syncRangeRepaired || replica.stateSync.repairing || replica.status != StatusNormal {
		t.Fatal("durable latest reply did not release sync")
	}
}

func TestStateSyncScansLatestRepliesAfterCommittedReplay(t *testing.T) {
	replica, _, bus := replyRecoveryFixture(t)
	_, checkpointReply := makeReplyFrame(t, protocol.ClientID{7}, 5, []byte("overwritten checkpoint reply"))
	if _, err := replica.sessions.Commit(checkpointReply, 1); err != nil {
		t.Fatal(err)
	}
	replica.stateSync.repairing = true
	replica.stateSync.repliesPending = true
	replica.stateSync.commit = 6
	replica.commitMin, replica.commitMax = 5, 6
	if replica.continueReplyRepair(uint64(time.Second)) || !replica.io.Drained() || bus.replicaCount() != 0 {
		t.Fatal("requested an obsolete reply before replaying the committed suffix")
	}
	latestFrame, latest := makeReplyFrame(t, protocol.ClientID{7}, 6, []byte("latest committed reply"))
	slot, err := replica.sessions.Commit(latest, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := replica.replies.Write(slot, latestFrame); err != nil {
		t.Fatal(err)
	}
	replica.commitMin = 6
	if !replica.continueReplyRepair(uint64(2 * time.Second)) {
		t.Fatal("latest reply scan did not start after committed replay")
	}
	completeReplyRecoveryIO(t, replica)
	replica.continueReplyRepair(uint64(3 * time.Second))
	if !replica.syncRangeRepaired || replica.stateSync.repairing || bus.replicaCount() != 0 {
		t.Fatal("latest durable reply did not complete synchronization without obsolete repair")
	}
}
