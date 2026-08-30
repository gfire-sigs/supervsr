package replication

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestCheckpointCrashSelectsOnlyDurableManifestAndFreeSet(t *testing.T) {
	probeOperations := runCheckpointCrashCase(t, 0, false)
	for failAt := 1; failAt <= probeOperations; failAt++ {
		t.Run(operationName(failAt), func(t *testing.T) {
			runCheckpointCrashCase(t, failAt, true)
		})
	}
}

func runCheckpointCrashCase(t testing.TB, failAt int, expectFailure bool) int {
	t.Helper()
	config, storage, initial, wal, replies, sessions, superblocks := replicaFixture(t)
	capacities := StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 2,
	}
	machine := &testStateMachine{capacities: capacities, pulseNeeded: true, writeCheckpoint: true}
	validator := manifestTestValidator{}
	replica, err := newReplica(config, Dependencies{
		Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 1, Monotonic: 1, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{1}), StateMachine: machine, BlockValidator: validator,
	}, initial, wal, replies, sessions, superblocks)
	if err != nil {
		t.Fatal(err)
	}
	target, ok := checkpointAfter(config.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	trigger, ok := checkpointTrigger(config.Cluster, target)
	if !ok {
		t.Fatal("checkpoint trigger unavailable")
	}
	for op := Op(1); op < trigger; op++ {
		replica.handlePulseTimeout(TimeSample{Wall: uint64(op), Monotonic: uint64(op), Synchronized: true})
		processReplicaUntil(t, replica, op)
	}
	storage.operation = 0
	storage.failAt = failAt
	replica.handlePulseTimeout(TimeSample{Wall: uint64(trigger), Monotonic: uint64(trigger), Synchronized: true})
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	for replica.fatalErr == nil && (replica.checkpoint.PrepareOp() != target || replica.pipelineLen != 0) {
		_, _ = replica.Process(64)
		select {
		case <-replica.io.Ready():
		case <-replica.notify:
		case <-deadline.C:
			t.Fatalf("checkpoint stalled failAt=%d snapshot=%+v", failAt, replica.Snapshot())
		default:
		}
	}
	operationCount := storage.operation
	if expectFailure && replica.fatalErr == nil {
		t.Fatalf("checkpoint succeeded across storage failure %d", failAt)
	}
	if !expectFailure && replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	closeContext, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	_ = replica.Close(closeContext)
	cancel()
	storage.Crash()

	validation := SuperblockValidation{
		Group: config.Group, Membership: config.Membership,
		ConfigurationChecksum: config.Cluster.Fingerprint(), Cluster: config.Cluster,
	}
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	wantCheckpoint := Op(0)
	if store.Current().Sequence == 2 {
		wantCheckpoint = target
	}
	reopened, err := Open(context.Background(), config, Dependencies{
		Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{sample: TimeSample{Wall: 200, Monotonic: 200, Synchronized: true}},
		Entropy: bytes.NewReader([]byte{2}), StateMachine: &testStateMachine{capacities: capacities}, BlockValidator: validator,
	})
	if err != nil {
		t.Fatalf("reopen sequence %d: %v", store.Current().Sequence, err)
	}
	snapshot := reopened.Snapshot()
	if snapshot.Checkpoint.PrepareOp() != wantCheckpoint {
		t.Fatalf("sequence %d checkpoint=%d want=%d", store.Current().Sequence, snapshot.Checkpoint.PrepareOp(), wantCheckpoint)
	}
	if wantCheckpoint == 0 {
		if snapshot.Checkpoint.ManifestBlockCount != 0 || snapshot.Checkpoint.SnapshotRootAddress != 0 {
			t.Fatalf("old checkpoint exposed application references: %+v", snapshot.Checkpoint)
		}
	} else if snapshot.Checkpoint.ManifestBlockCount != 1 || snapshot.Checkpoint.SnapshotRootAddress == 0 {
		t.Fatalf("new checkpoint lost application references: %+v", snapshot.Checkpoint)
	}
	closeReplica(t, reopened)
	return operationCount
}
