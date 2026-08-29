package replication

import (
	"errors"
	"testing"
)

func TestBlockAllocatorPublishesAndReusesOnlyAfterDurableRelease(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 2*cluster.BlockSize
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
	first, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if first != base || allocator.AcquiredCount() != 0 {
		t.Fatalf("reservation address=%d acquired=%d", first, allocator.AcquiredCount())
	}
	if err := allocator.Publish(first); err != nil {
		t.Fatal(err)
	}
	if err := allocator.Release(first); err != nil {
		t.Fatal(err)
	}
	second, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if second != base+cluster.BlockSize {
		t.Fatalf("pending release reused as %d", second)
	}
	if err := allocator.Forfeit(second); err != nil {
		t.Fatal(err)
	}
	candidate, err := allocator.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Acquired().Count() != 1 || candidate.Released().Count() != 1 {
		t.Fatalf("checkpoint snapshot acquired=%d released=%d", candidate.Acquired().Count(), candidate.Released().Count())
	}
	late, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if late != second {
		t.Fatalf("late reservation = %d want %d", late, second)
	}
	if err := allocator.Publish(late); err != nil {
		t.Fatal(err)
	}
	if err := allocator.Release(late); err != nil {
		t.Fatal(err)
	}
	reachable, err := NewFixedBitSet(allocator.blockCount)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Released().Count() != 1 {
		t.Fatalf("late release changed checkpoint snapshot: %d", candidate.Released().Count())
	}
	if err := allocator.CheckpointDurable(candidate, &reachable); err != nil {
		t.Fatal(err)
	}
	if allocator.ReleasedCount() != 1 {
		t.Fatalf("released count = %d", allocator.ReleasedCount())
	}
	reused, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if reused != first {
		t.Fatalf("reused address = %d want %d", reused, first)
	}
	if err := allocator.Forfeit(reused); err != nil {
		t.Fatal(err)
	}
	reusedAgain, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if reusedAgain != first {
		t.Fatalf("forfeited free address leaked: got %d want %d", reusedAgain, first)
	}
}

func TestBlockAllocatorRejectsReachableReleaseAtomically(t *testing.T) {
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
	address, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if err := allocator.Publish(address); err != nil {
		t.Fatal(err)
	}
	if err := allocator.Release(address); err != nil {
		t.Fatal(err)
	}
	candidate, err := allocator.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	reachable, err := NewFixedBitSet(1)
	if err != nil {
		t.Fatal(err)
	}
	reachable.Set(0)
	if err := allocator.CheckpointDurable(candidate, &reachable); !errors.Is(err, ErrBlockReservation) {
		t.Fatalf("reachable release error = %v", err)
	}
	if allocator.ReleasedCount() != 0 {
		t.Fatalf("reachable address released")
	}
	if _, err := allocator.Reserve(); !errors.Is(err, ErrStorageExhausted) {
		t.Fatalf("exhaustion error = %v", err)
	}
}

type growingStorage struct{ crashStorage }

func (storage *growingStorage) Resize(size uint64) error {
	if size < uint64(len(storage.working)) {
		storage.working = storage.working[:size]
		return nil
	}
	growth := make([]byte, int(size)-len(storage.working))
	storage.working = append(storage.working, growth...)
	return nil
}
