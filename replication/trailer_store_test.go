package replication

import (
	"bytes"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestTrailerStoreRoundTripsMultipleBlocksBackward(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 4*cluster.BlockSize
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
	blocks, err := NewBlockStore(storage, cluster, protocol.GroupID{3}, 1)
	if err != nil {
		t.Fatal(err)
	}
	trailers, err := NewTrailerStore(blocks)
	if err != nil {
		t.Fatal(err)
	}
	capacity := int(cluster.BlockSize - protocol.HeaderSize)
	encoded := make([]byte, capacity+17)
	for index := range encoded {
		encoded[index] = byte(index*29 + 7)
	}
	reference, err := trailers.Write(allocator, protocol.BlockClientSessions, 11, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if reference.BlockCount != 2 || allocator.AcquiredCount() != 2 {
		t.Fatalf("reference blocks=%d acquired=%d", reference.BlockCount, allocator.AcquiredCount())
	}
	decoded := make([]byte, len(encoded))
	if err := trailers.Read(reference, decoded); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded, encoded) {
		t.Fatal("trailer content changed")
	}
	candidate, err := allocator.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Acquired().Count() != 2 {
		t.Fatalf("checkpoint omitted trailer blocks: %d", candidate.Acquired().Count())
	}
	if err := allocator.AbortCheckpoint(candidate); err != nil {
		t.Fatal(err)
	}
}
