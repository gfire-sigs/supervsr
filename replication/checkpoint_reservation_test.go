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
