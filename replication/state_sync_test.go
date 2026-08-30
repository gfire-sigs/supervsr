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

func TestStateSyncPersistsRangeRepairsBlocksAndOpensCheckpoint(t *testing.T) {
	sourceConfig, sourceStorage, sourceInitial, sourceWAL, sourceReplies, sourceSessions, sourceSuperblocks := replicaFixture(t)
	capacities := StateMachineCapacities{
		RequestBytes:  uint32(sourceConfig.Cluster.ApplicationBatchSizeMax),
		ReplyBytes:    uint32(sourceConfig.Cluster.ApplicationReplySizeMax),
		PrefetchMax:   uint32(sourceConfig.Cluster.PipelineMax),
		CheckpointMax: 1,
	}
	source, err := newReplica(sourceConfig, Dependencies{
		Storage: sourceStorage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: &testStateMachine{capacities: capacities, pulseNeeded: true},
	}, sourceInitial, sourceWAL, sourceReplies, sourceSessions, sourceSuperblocks)
	if err != nil {
		t.Fatal(err)
	}
	checkpointOp, ok := checkpointAfter(sourceConfig.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	trigger, ok := checkpointTrigger(sourceConfig.Cluster, checkpointOp)
	if !ok {
		t.Fatal("checkpoint trigger unavailable")
	}
	for op := protocol.Op(1); op <= trigger; op++ {
		source.handlePulseTimeout(TimeSample{Wall: uint64(op), Monotonic: uint64(op), Synchronized: true})
		processReplicaUntil(t, source, op)
	}
	sourceDeadline := time.NewTimer(3 * time.Second)
	defer sourceDeadline.Stop()
	for source.checkpoint.PrepareOp() != checkpointOp {
		if _, err := source.Process(64); err != nil {
			t.Fatal(err)
		}
		select {
		case <-source.io.Ready():
		case <-source.notify:
		case <-sourceDeadline.C:
			t.Fatalf("source checkpoint did not finish: %+v", source.Snapshot())
		default:
		}
	}
	checkpoint := source.checkpoint
	if checkpoint.PrepareOp() != checkpointOp {
		t.Fatalf("source checkpoint = %d, want %d", checkpoint.PrepareOp(), checkpointOp)
	}
	viewBody := makeStateSyncViewBody(t, sourceConfig, checkpoint)
	closeReplica(t, source)
	sourceStorage.Crash()

	targetConfig, targetStorage, targetInitial, targetWAL, targetReplies, targetSessions, targetSuperblocks := replicaFixture(t)
	blockBase, _ := targetConfig.Cluster.BlockBase()
	if err := targetStorage.Resize(uint64(len(sourceStorage.durable))); err != nil {
		t.Fatal(err)
	}
	copy(targetStorage.working[blockBase:], sourceStorage.durable[blockBase:])
	if err := targetStorage.Sync(); err != nil {
		t.Fatal(err)
	}
	targetMachine := &syncOrderMachine{testStateMachine: testStateMachine{capacities: capacities}, store: targetSuperblocks}
	target, err := newReplica(targetConfig, Dependencies{
		Storage: targetStorage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 100, Monotonic: 100, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{2}), StateMachine: targetMachine,
	}, targetInitial, targetWAL, targetReplies, targetSessions, targetSuperblocks)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if target != nil {
			closeReplica(t, target)
		}
	}()

	pool, err := protocol.NewFramePool(1, uint32(targetConfig.Cluster.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	view := protocol.Header{
		Group: targetConfig.Group, View: target.view, Protocol: protocol.ProtocolVersion,
		Command: protocol.CommandView, Author: target.membership.Primary(target.view),
	}
	binary.LittleEndian.PutUint64(view.Fields[16:24], uint64(checkpointOp))
	binary.LittleEndian.PutUint64(view.Fields[24:32], uint64(checkpointOp))
	message := makeReplicaCommand(t, pool, view, viewBody)
	if err := target.Submit(message); err != nil {
		t.Fatal(err)
	}
	if _, err := target.Process(64); err != nil {
		t.Fatal(err)
	}
	if target.stateSync.stage != SyncStageCancelingGrid {
		t.Fatalf("stage after selection = %d", target.stateSync.stage)
	}
	if _, err := target.Process(64); err != nil {
		t.Fatal(err)
	}
	if target.stateSync.stage != SyncStageUpdatingCheckpoint {
		t.Fatalf("stage after grid drain = %d", target.stateSync.stage)
	}

	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	monotonic := uint64(time.Second)
	for target.stateSync.stage != SyncStageIdle || target.stateSync.repairing {
		target.handleRepairTimeout(TimeSample{Monotonic: monotonic})
		monotonic += uint64(100 * time.Millisecond)
		if _, err := target.Process(64); err != nil {
			t.Fatal(err)
		}
		select {
		case <-target.io.Ready():
		case <-target.notify:
		case <-deadline.C:
			t.Fatalf("state sync stalled at stage=%d repairing=%t", target.stateSync.stage, target.stateSync.repairing)
		default:
		}
	}
	if target.status != StatusNormal || target.checkpointID != mustCheckpointID(t, checkpoint) || target.commitMin != checkpointOp {
		t.Fatalf("target status=%d checkpoint=%x commit=%d", target.status, target.checkpointID, target.commitMin)
	}
	durable := target.superblocks.Current().State
	if durable.SyncMin != 1 || durable.SyncMax != checkpointOp || durable.Checkpoint.PrepareOp() != checkpointOp {
		t.Fatalf("durable sync range=%d..%d checkpoint=%d", durable.SyncMin, durable.SyncMax, durable.Checkpoint.PrepareOp())
	}
	if !target.syncRangeRepaired || targetMachine.commits != 0 || targetMachine.resetSyncMin != 1 {
		t.Fatalf("sync repaired=%t replay commits=%d reset sync min=%d", target.syncRangeRepaired, targetMachine.commits, targetMachine.resetSyncMin)
	}
	targetMachine.pulseNeeded = true
	target.handlePulseTimeout(TimeSample{Wall: 150, Monotonic: monotonic, Synchronized: true})
	targetMachine.pulseNeeded = false
	for target.commitMin != checkpointOp+1 {
		if _, err := target.Process(64); err != nil {
			t.Fatal(err)
		}
	}
	closeReplica(t, target)
	target = nil
	pending, err := Open(context.Background(), targetConfig, Dependencies{
		Storage: targetStorage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 175, Monotonic: 175, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{3}), StateMachine: &pendingReplayMachine{testStateMachine: testStateMachine{capacities: capacities}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for !pending.stateSync.replayRunning {
		pending.handleRepairTimeout(TimeSample{Monotonic: monotonic})
		monotonic += uint64(100 * time.Millisecond)
		if _, err := pending.Process(64); err != nil {
			t.Fatal(err)
		}
	}
	closeContext, cancelClose := context.WithTimeout(context.Background(), time.Second)
	defer cancelClose()
	if err := pending.Close(closeContext); err != nil {
		t.Fatalf("close during pending replay: %v", err)
	}

	replayMachine := &testStateMachine{capacities: capacities}
	reopened, err := Open(context.Background(), targetConfig, Dependencies{
		Storage: targetStorage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 200, Monotonic: 200, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{3}), StateMachine: replayMachine,
	})
	if err != nil {
		t.Fatal(err)
	}
	deadline.Reset(3 * time.Second)
	for reopened.stateSync.repairing {
		reopened.handleRepairTimeout(TimeSample{Monotonic: monotonic})
		monotonic += uint64(100 * time.Millisecond)
		if _, err := reopened.Process(64); err != nil {
			t.Fatal(err)
		}
		select {
		case <-deadline.C:
			t.Fatal("reopened state sync did not finish")
		default:
		}
	}
	if reopened.status != StatusNormal || reopened.commitMin != checkpointOp+1 || replayMachine.commits != 1 {
		t.Fatalf("reopened status=%d commit=%d replay commits=%d", reopened.status, reopened.commitMin, replayMachine.commits)
	}
	closeReplica(t, reopened)

	corruptAddress := checkpoint.AcquiredTrailerLastAddress
	if corruptAddress == 0 {
		t.Fatal("checkpoint has no acquired trailer")
	}
	corruptOffset, ok := targetConfig.Cluster.BlockOffset(corruptAddress)
	if !ok {
		t.Fatal("corrupt block offset overflow")
	}
	if err := targetStorage.WriteAt(make([]byte, targetConfig.Cluster.BlockSize), corruptOffset); err != nil {
		t.Fatal(err)
	}
	if err := targetStorage.Sync(); err != nil {
		t.Fatal(err)
	}
	damaged, err := Open(context.Background(), targetConfig, Dependencies{
		Storage: targetStorage, MessageBus: &captureBus{},
		Clock:   fixedClock{sample: TimeSample{Wall: 300, Monotonic: 300, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{4}), StateMachine: &testStateMachine{capacities: capacities},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer closeReplica(t, damaged)
	repairDeadline := time.NewTimer(3 * time.Second)
	defer repairDeadline.Stop()
	for !damaged.blockRepairActive() {
		if _, err := damaged.Process(64); err != nil {
			t.Fatal(err)
		}
		if damaged.blockRepairActive() || !damaged.stateSync.repairing {
			break
		}
		select {
		case <-damaged.io.Ready():
		case <-damaged.notify:
		case <-repairDeadline.C:
			t.Fatalf("damaged checkpoint repair did not start: stage=%d io=%v phase=%d done=%t", damaged.stateSync.stage, damaged.stateSync.io, damaged.stateSync.persistPhase, damaged.stateSync.persistDone)
		}
	}
	if damaged.status != StatusRecovering || !damaged.stateSync.repairing || !damaged.blockRepairActive() {
		t.Fatalf("damaged status=%d repairing=%t active=%t", damaged.status, damaged.stateSync.repairing, damaged.blockRepairActive())
	}
}

func TestStateSyncDrainDoesNotAcknowledgeCompletedWALAppend(t *testing.T) {
	handle := IOHandle{Index: 1, Generation: 2}
	replica := Replica{pipeline: make([]pipelineEntry, 1), pipelineLen: 1}
	replica.pipeline[0].io = handle
	replica.pipeline[0].ioKind = IOWALAppend
	replica.handleStateSyncDrainIO(IOCompletion{Handle: handle})
	entry := replica.pipeline[0]
	if entry.io != (IOHandle{}) || entry.ioKind != 0 || entry.durable || entry.acks != 0 {
		t.Fatalf("drained entry=%+v", entry)
	}
}

func TestStateSyncDrainFailsOnViewPersistenceError(t *testing.T) {
	handle := IOHandle{Index: 1, Generation: 2}
	failure := errors.New("view persistence failed")
	replica := Replica{viewIO: handle}
	replica.handleStateSyncDrainIO(IOCompletion{Handle: handle, Err: failure})
	if !errors.Is(replica.fatalErr, failure) {
		t.Fatalf("fatal error=%v, want %v", replica.fatalErr, failure)
	}
}

type syncOrderMachine struct {
	testStateMachine
	store        *SuperblockStore
	resetSyncMin protocol.Op
}

func (machine *syncOrderMachine) StartReset(completion *SMCompletion) (StartResult[ResetResult], error) {
	machine.resetSyncMin = machine.store.Current().State.SyncMin
	return machine.testStateMachine.StartReset(completion)
}

type pendingReplayMachine struct {
	testStateMachine
}

func (*pendingReplayMachine) StartPrefetch(PrefetchInput, *SMCompletion) (StartResult[PrefetchToken], error) {
	return Pending[PrefetchToken](), nil
}

func makeStateSyncViewBody(t testing.TB, config Config, checkpoint CheckpointState) []byte {
	t.Helper()
	body := make([]byte, CheckpointStateSize+protocol.HeaderSize)
	if err := checkpoint.Encode(body[:CheckpointStateSize]); err != nil {
		t.Fatal(err)
	}
	header, reason := protocol.DecodeHeader(checkpoint.Header[:], config.Group, uint32(config.Cluster.MessageSizeMax), config.Membership.ActiveCount+config.Membership.StandbyCount)
	if reason != protocol.RejectNone {
		t.Fatalf("checkpoint header reason = %d", reason)
	}
	if err := protocol.EncodeHeader(body[CheckpointStateSize:], &header); err != nil {
		t.Fatal(err)
	}
	return body
}

func mustCheckpointID(t testing.TB, checkpoint CheckpointState) protocol.CheckpointID {
	t.Helper()
	checkpointID, err := checkpoint.ID()
	if err != nil {
		t.Fatal(err)
	}
	return checkpointID
}
