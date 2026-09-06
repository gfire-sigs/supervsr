package replication

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type heldRepairStorage struct {
	Storage
	offset  uint64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (storage *heldRepairStorage) WriteAt(buffer []byte, offset uint64) error {
	if offset == storage.offset {
		storage.once.Do(func() { close(storage.started); <-storage.release })
	}
	return storage.Storage.WriteAt(buffer, offset)
}

func TestCheckpointJoinsLiveRepairBeforeReusingAddress(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	process := DefaultProcessConfig()
	process.StorageSizeLimit = base + 4*cluster.BlockSize
	backing := &crashStorage{}
	if err := backing.Resize(base); err != nil {
		t.Fatal(err)
	}
	storage := &heldRepairStorage{Storage: backing, offset: base, started: make(chan struct{}), release: make(chan struct{})}
	state := CheckpointState{LogicalStorageSize: base, Release: 1}
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
	engine, err := NewIOEngine(storage, 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	var unblock sync.Once
	t.Cleanup(func() { unblock.Do(func() { close(storage.release) }); closeIOEngine(t, engine) })
	budget, err := newBlockRepairBudget(2, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	buffer, err := NewAlignedBuffer(cluster.BlockSize, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Cluster: cluster, Process: process, Group: GroupID{1}, CurrentRelease: 1}
	replica := &Replica{config: config, membership: Membership{ActiveCount: 2}, io: engine, blockAllocator: allocator,
		blockRepairBudget: budget, blockRepairTargets: make([]blockRepairTarget, 1), blockRepairIO: []blockRepairIO{{buffer: buffer}},
		pipeline: make([]pipelineEntry, 1), status: StatusRecovering, metrics: &ReplicaMetrics{},
	}
	header, body := makeRepairBlock(t, config, address, 7)
	reference := BlockReference{Address: address, Checksum: header.HeaderChecksum}
	if !replica.queueBlockRepair(reference, protocol.BlockFreeSet, 7, blockSnapshotExpectation{value: 7, exact: true}, uint32(len(body)), blockRepairScrub) {
		t.Fatal("repair not queued")
	}
	replica.blockRepairTargets[0].state = blockRepairMissing
	if !replica.blockRepairBudget.Reserve(1, []BlockReference{reference}, 1) {
		t.Fatal("repair budget unavailable")
	}
	replica.handleBlock(header, body)
	select {
	case <-storage.started:
	case <-time.After(time.Second):
		t.Fatal("repair write did not start")
	}
	if err := allocator.Release(address); err != nil {
		t.Fatal(err)
	}
	candidate, err := allocator.BeginCheckpoint()
	if err != nil {
		t.Fatal(err)
	}
	candidate.reachable, err = NewFixedBitSet(candidate.blockCount)
	if err != nil {
		t.Fatal(err)
	}
	replica.checkpointCandidate = candidate
	replica.pendingCheckpoint = state
	replica.pendingCheckpoint.LogicalStorageSize = base + cluster.BlockSize
	replica.pipelineLen = 1
	replica.pipeline[0].stage = CommitStageCheckpointSuperblock
	replica.finishCheckpointPersistence(&replica.pipeline[0])
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	reserved, err := allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if reserved == address {
		t.Fatal("checkpoint freed an address while its old repair was running")
	}
	if err := allocator.Forfeit(reserved); err != nil {
		t.Fatal(err)
	}
	unblock.Do(func() { close(storage.release) })
	drainBlockRepairIO(t, replica)
	if replica.fatalErr != nil {
		t.Fatal(replica.fatalErr)
	}
	reused, err := allocator.Reserve()
	if err != nil || reused != address {
		t.Fatalf("drained repair address reuse=%d err=%v", reused, err)
	}
	newValue := bytes.Repeat([]byte{0xA5}, int(cluster.BlockSize))
	if err := storage.WriteAt(newValue, base); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backing.Crash()
	if !bytes.Equal(backing.working[base:base+cluster.BlockSize], newValue) {
		t.Fatal("old repair overwrote reused address")
	}
}
