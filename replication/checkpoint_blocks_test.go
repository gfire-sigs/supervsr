package replication

import (
	"encoding/binary"
	"errors"
	"testing"
)

func TestCheckpointBlockTransactionPublishesDurableBlock(t *testing.T) {
	transaction, allocator, store := checkpointBlockTransactionFixture(t, 1)
	if err := transaction.begin(7); err != nil {
		t.Fatal(err)
	}
	block, err := transaction.Reserve(BlockManifest)
	if err != nil {
		t.Fatal(err)
	}
	if block.Address() != 1 || block.Type() != BlockManifest {
		t.Fatalf("reservation address=%d type=%d", block.Address(), block.Type())
	}
	if _, err := transaction.Reserve(BlockManifest); !errors.Is(err, ErrCheckpointBlockLimit) {
		t.Fatalf("second reservation error = %v", err)
	}
	if err := transaction.seal(); err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Reserve(BlockManifest); !errors.Is(err, ErrCheckpointBlocksClosed) {
		t.Fatalf("sealed reservation error = %v", err)
	}
	var metadata [96]byte
	binary.LittleEndian.PutUint32(metadata[40:44], 1)
	body := []byte("entry")
	reference, err := block.Write(metadata, body)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := block.Write(metadata, body); !errors.Is(err, ErrCheckpointBlocksClosed) {
		t.Fatalf("second write error = %v", err)
	}
	if err := transaction.commit(); err != nil {
		t.Fatal(err)
	}
	if allocator.AcquiredCount() != 1 {
		t.Fatalf("acquired count = %d", allocator.AcquiredCount())
	}
	reader := newCheckpointBlockReader(allocator, store)
	destination := make([]byte, store.cluster.BlockSize)
	result, err := reader.Read(reference, BlockManifest, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.Snapshot != 7 || string(destination[:result.BodySize]) != string(body) {
		t.Fatalf("read snapshot=%d body=%q", result.Snapshot, destination[:result.BodySize])
	}
	if _, err := reader.Read(reference, BlockFreeSet, destination); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("internal block read error = %v", err)
	}
	if _, err := reader.Read(BlockReference{Address: 2, Checksum: Checksum{1}}, BlockManifest, destination); !errors.Is(err, ErrBlockMissing) {
		t.Fatalf("unacquired block read error = %v", err)
	}
}

func TestCheckpointBlockTransactionAbortReusesUnpublishedAddress(t *testing.T) {
	transaction, _, _ := checkpointBlockTransactionFixture(t, 2)
	if err := transaction.begin(9); err != nil {
		t.Fatal(err)
	}
	block, err := transaction.Reserve(BlockValue)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transaction.Reserve(BlockFreeSet); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("internal block type error = %v", err)
	}
	if err := transaction.seal(); err != nil {
		t.Fatal(err)
	}
	if err := transaction.abort(); err != nil {
		t.Fatal(err)
	}
	if _, err := block.Write([96]byte{}, []byte{1}); !errors.Is(err, ErrCheckpointBlocksClosed) {
		t.Fatalf("stale block write error = %v", err)
	}
	if err := transaction.begin(10); err != nil {
		t.Fatal(err)
	}
	reused, err := transaction.Reserve(BlockIndex)
	if err != nil {
		t.Fatal(err)
	}
	if reused.Address() != block.Address() {
		t.Fatalf("reused address = %d, want %d", reused.Address(), block.Address())
	}
	if err := transaction.abort(); err != nil {
		t.Fatal(err)
	}
}

func TestZeroCheckpointBlockCannotWrite(t *testing.T) {
	if _, err := (CheckpointBlock{}).Write([96]byte{}, []byte{1}); !errors.Is(err, ErrCheckpointBlocksClosed) {
		t.Fatalf("zero block write error = %v", err)
	}
}

func checkpointBlockTransactionFixture(t *testing.T, limit uint32) (*CheckpointBlockTransaction, *BlockAllocator, *BlockStore) {
	t.Helper()
	cluster := compactTestClusterConfig()
	base, ok := cluster.BlockBase()
	if !ok {
		t.Fatal("block base overflow")
	}
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
	store, err := NewBlockStore(storage, cluster, GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	return newCheckpointBlockTransaction(allocator, store, limit), allocator, store
}
