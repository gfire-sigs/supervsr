package replication

import (
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestCheckpointPreparationIncludesOwnTrailerReservations(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 8*cluster.BlockSize
	storage := &growingStorage{}
	if err := storage.Resize(base); err != nil {
		t.Fatal(err)
	}
	state := CheckpointState{LogicalStorageSize: base}
	acquired, released, err := EmptyBlockSets(state, cluster)
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := OpenBlockAllocator(storage, cluster, process, state, acquired, released)
	if err != nil {
		t.Fatal(err)
	}
	blocks, err := NewBlockStore(storage, cluster, protocol.GroupID{4}, 1)
	if err != nil {
		t.Fatal(err)
	}
	trailers, err := NewTrailerStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	sessions := []byte("session-checkpoint")
	payload := cluster.BlockSize - protocol.HeaderSize
	candidate, err := allocator.PrepareCheckpoint(uint64(len(sessions)), payload)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Acquired().Count() != uint64(len(candidate.addresses)) {
		t.Fatalf("own trailers omitted: acquired=%d addresses=%d", candidate.Acquired().Count(), len(candidate.addresses))
	}
	snapshot := uint64(31)
	if _, err := trailers.WriteReserved(allocator, candidate.AcquiredAddresses(), protocol.BlockFreeSet, snapshot, candidate.AcquiredEncoded()); err != nil {
		t.Fatal(err)
	}
	if _, err := trailers.WriteReserved(allocator, candidate.ReleasedAddresses(), protocol.BlockFreeSet, snapshot, candidate.ReleasedEncoded()); err != nil {
		t.Fatal(err)
	}
	if _, err := trailers.WriteReserved(allocator, candidate.SessionAddresses(), protocol.BlockClientSessions, snapshot, sessions); err != nil {
		t.Fatal(err)
	}
	reachable := candidate.Acquired()
	if err := allocator.CheckpointDurable(candidate, &reachable); err != nil {
		t.Fatal(err)
	}
	if allocator.AcquiredCount() != uint64(len(candidate.addresses)) {
		t.Fatalf("published acquired=%d addresses=%d", allocator.AcquiredCount(), len(candidate.addresses))
	}
}

func TestCheckpointPreparationSteadyStateHasNoAllocations(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 8*cluster.BlockSize
	storage := &growingStorage{}
	if err := storage.Resize(base); err != nil {
		t.Fatal(err)
	}
	state := CheckpointState{LogicalStorageSize: base}
	acquired, released, err := EmptyBlockSets(state, cluster)
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := OpenBlockAllocator(storage, cluster, process, state, acquired, released)
	if err != nil {
		t.Fatal(err)
	}
	payload := cluster.BlockSize - protocol.HeaderSize
	run := func() {
		candidate, prepareErr := allocator.PrepareCheckpoint(64, payload)
		if prepareErr != nil {
			panic(prepareErr)
		}
		if abortErr := allocator.AbortCheckpoint(candidate); abortErr != nil {
			panic(abortErr)
		}
	}
	run()
	if allocations := testing.AllocsPerRun(1_000, run); allocations != 0 {
		t.Fatalf("steady-state checkpoint allocations = %f", allocations)
	}
}

func TestEmptyCheckpointReachabilitySteadyStateHasNoAllocations(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + cluster.BlockSize
	storage := &growingStorage{}
	if err := storage.Resize(base); err != nil {
		t.Fatal(err)
	}
	state := CheckpointState{LogicalStorageSize: base}
	acquired, released, err := EmptyBlockSets(state, cluster)
	if err != nil {
		t.Fatal(err)
	}
	allocator, err := OpenBlockAllocator(storage, cluster, process, state, acquired, released)
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := newCheckpointGraphScratch(1, allocator.acquired.Len(), cluster.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	replica := Replica{config: Config{Cluster: cluster}, blockAllocator: allocator, checkpointScratch: scratch}
	run := func() {
		reachable, protected, superseded, resolveErr := replica.resolveCheckpointReachability(CheckpointManifest{})
		if resolveErr != nil || reachable.Count() != 0 || protected.Count() != 0 || len(superseded) != 0 {
			panic(ErrInvalidCheckpoint)
		}
	}
	run()
	if allocations := testing.AllocsPerRun(1_000, run); allocations != 0 {
		t.Fatalf("steady-state reachability allocations = %f", allocations)
	}
}
