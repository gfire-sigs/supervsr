package replication

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestOpenRejectsStateMachineCapacitiesBeforeSideEffects(t *testing.T) {
	config, storage, _, _, _, _, _ := replicaFixture(t)
	before := append([]byte(nil), storage.working...)
	operations := storage.operation
	machine := &testStateMachine{capacities: StateMachineCapacities{
		RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax),
		PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 0,
	}}
	replica, err := Open(context.Background(), config, Dependencies{Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{}, Entropy: bytes.NewReader(make([]byte, 8)), StateMachine: machine})
	if replica != nil || !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("Open = %v, %v", replica, err)
	}
	if storage.operation != operations || !bytes.Equal(before, storage.working) || machine.openBlocks || machine.commits != 0 {
		t.Fatalf("invalid startup had side effects: IO=%d want=%d open=%t commits=%d", storage.operation, operations, machine.openBlocks, machine.commits)
	}
}

func TestOpenResumesInstalledCheckpointWithMissingCommittedSuffix(t *testing.T) {
	format, validation := formatFixture(t)
	storage := &crashStorage{}
	if err := Format(context.Background(), format, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	next := store.Current()
	root, reason := protocol.DecodeHeader(next.State.Checkpoint.Header[:], format.Group, uint32(format.Cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	checkpointOp, ok := checkpointAfter(format.Cluster, 0)
	if !ok {
		t.Fatal("checkpoint unavailable")
	}
	checkpointFrame := connectedPrepareFrame(t, format.Cluster, root, checkpointOp)
	checkpointHeader, _, reason := protocol.DecodeFrame(checkpointFrame, format.Group, uint32(format.Cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	suffix := connectedPrepareFrame(t, format.Cluster, checkpointHeader, checkpointOp+1)
	suffixHeader, _, reason := protocol.DecodeFrame(suffix, format.Group, uint32(format.Cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	parentID, err := next.State.Checkpoint.ID()
	if err != nil {
		t.Fatal(err)
	}
	copy(next.State.Checkpoint.Header[:], checkpointFrame[:protocol.HeaderSize])
	next.State.Checkpoint.ParentID = parentID
	next.State.SyncMin = 1
	next.State.SyncMax = checkpointOp
	next.State.CommitMax = checkpointOp + 1
	next.ViewHeaderCount = 2
	if err := protocol.EncodeHeader(next.ViewHeaders[0][:], &suffixHeader); err != nil {
		t.Fatal(err)
	}
	copy(next.ViewHeaders[1][:], checkpointFrame[:protocol.HeaderSize])
	next.ParentChecksum = next.Checksum
	next.Sequence++
	if err := store.Persist(next); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	config := Config{Group: format.Group, Membership: format.Membership, Cluster: format.Cluster, Process: DefaultProcessConfig(), CurrentRelease: 1, ClientReleaseMin: 1}
	machine := &testStateMachine{capacities: StateMachineCapacities{RequestBytes: uint32(config.Cluster.ApplicationBatchSizeMax), ReplyBytes: uint32(config.Cluster.ApplicationReplySizeMax), PrefetchMax: uint32(config.Cluster.PipelineMax), CheckpointMax: 1}}
	replica, err := Open(context.Background(), config, Dependencies{Storage: storage, MessageBus: &captureBus{}, Clock: fixedClock{}, Entropy: bytes.NewReader(make([]byte, 8)), StateMachine: machine, SynchronousIO: true})
	if err != nil {
		t.Fatalf("installed checkpoint rejected before suffix repair: %v", err)
	}
	defer closeReplica(t, replica)
	for step := 0; step < 30 && replica.stateSync.repairing; step++ {
		replica.handleRepairTimeout(TimeSample{Monotonic: uint64(step+1) * 1_000_000_000})
		if _, err := replica.Process(64); err != nil {
			t.Fatal(err)
		}
	}
	if !replica.stateSync.repairing || replica.syncRangeRepaired || replica.commitMin != checkpointOp || replica.commitMax != checkpointOp+1 || machine.commits != 0 {
		t.Fatalf("resume repairing=%t commit=%d..%d executions=%d", replica.stateSync.repairing, replica.commitMin, replica.commitMax, machine.commits)
	}
	if replica.status != StatusRecoveringHead {
		t.Fatalf("missing suffix status = %v", replica.status)
	}
}

func TestStateSyncAcceptsEvenlySplitClientTrailer(t *testing.T) {
	cluster := compactTestClusterConfig()
	cluster.ClientsMax = 16
	cluster.BlockSize = 4096
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 8*cluster.BlockSize
	source := &crashStorage{}
	if err := source.Resize(base); err != nil {
		t.Fatal(err)
	}
	state := CheckpointState{LogicalStorageSize: base}
	acquired, released, err := EmptyBlockSets(state, cluster)
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := OpenBlockAllocator(source, cluster, process, state, acquired, released)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := NewBlockStore(source, cluster, protocol.GroupID{9}, 1)
	if err != nil {
		t.Fatal(err)
	}
	trailers, err := NewTrailerStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, 16*(protocol.HeaderSize+8))
	for index := range encoded {
		encoded[index] = byte(index*31 + 3)
	}
	reference, err := trailers.Write(allocator, protocol.BlockClientSessions, 7, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if reference.BlockCount != 2 {
		t.Fatalf("blocks=%d, want 2", reference.BlockCount)
	}
	target := &crashStorage{}
	if err := target.Resize(uint64(len(source.working))); err != nil {
		t.Fatal(err)
	}
	engine, err := newIOEngine(target, 2, 1, true)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)
	budget, err := newBlockRepairBudget(2, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := NewAlignedBuffer(cluster.BlockSize, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Group: protocol.GroupID{9}, Cluster: cluster, Process: process, CurrentRelease: 1}
	replica := &Replica{config: config, membership: Membership{ActiveCount: 2}, io: engine, blockRepairBudget: budget,
		blockRepairTargets: make([]blockRepairTarget, 4), blockRepairIO: []blockRepairIO{{buffer: buffer}}}
	// Keep another checkpoint root pending while this chain is fetched.
	replica.blockRepairTargets[3] = blockRepairTarget{state: blockRepairMissing, reference: BlockReference{Address: 8, Checksum: protocol.Checksum{8}}}
	if !replica.queueStateSyncTrailer(reference.Last, protocol.BlockClientSessions, 7, reference.EncodedSize) {
		t.Fatal("trailer rejected")
	}
	for remaining := reference.BlockCount; remaining > 0; remaining-- {
		index := -1
		for candidate := range 3 {
			if replica.blockRepairTargets[candidate].state == blockRepairQueued {
				index = candidate
				break
			}
		}
		if index < 0 {
			t.Fatal("predecessor not queued")
		}
		repair := &replica.blockRepairTargets[index]
		offset, _ := cluster.BlockOffset(repair.reference.Address)
		physical := source.working[offset : offset+cluster.BlockSize]
		frame, header, ok := decodeRepairFrame(physical, config.Group, uint32(cluster.BlockSize), 2)
		if !ok {
			t.Fatal("source trailer invalid")
		}
		repair.state = blockRepairMissing
		if !replica.blockRepairBudget.Reserve(1, []BlockReference{repair.reference}, uint64(remaining)*blockRepairExpires) {
			t.Fatal("budget unavailable")
		}
		replica.handleBlock(header, frame[protocol.HeaderSize:])
		drainBlockRepairIO(t, replica)
		if replica.fatalErr != nil {
			t.Fatal(replica.fatalErr)
		}
	}
	targetBlocks, err := NewBlockStore(target, cluster, config.Group, 1)
	if err != nil {
		t.Fatal(err)
	}
	targetTrailers, err := NewTrailerStore(targetBlocks)
	if err != nil {
		t.Fatal(err)
	}
	decoded := make([]byte, len(encoded))
	if err := targetTrailers.Read(reference, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, encoded) {
		t.Fatal("multi-block sync changed trailer bytes")
	}
}
