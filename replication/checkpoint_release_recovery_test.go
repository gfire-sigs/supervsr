package replication

import (
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func checkpointWithUnrecordedRelease(t testing.TB) (*Replica, *crashStorage, uint64) {
	t.Helper()
	cluster, storage, checkpoint := formattedWALFixture(t)
	config := Config{Group: protocol.GroupID{1}, Cluster: cluster, Process: DefaultProcessConfig(), CurrentRelease: 1}
	blocks, err := openBlockRuntime(storage, config, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	old, err := blocks.allocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if err := blocks.allocator.Publish(old); err != nil {
		t.Fatal(err)
	}
	encodedSessions := make([]byte, cluster.ClientsMax*(protocol.HeaderSize+8))
	candidate, err := blocks.allocator.PrepareCheckpoint(uint64(len(encodedSessions)), cluster.BlockSize-protocol.HeaderSize)
	if err != nil {
		t.Fatal(err)
	}
	acquired, err := blocks.trailers.WriteReserved(blocks.allocator, candidate.AcquiredAddresses(), protocol.BlockFreeSet, 7, candidate.AcquiredEncoded())
	if err != nil {
		t.Fatal(err)
	}
	released, err := blocks.trailers.WriteReserved(blocks.allocator, candidate.ReleasedAddresses(), protocol.BlockFreeSet, 7, candidate.ReleasedEncoded())
	if err != nil {
		t.Fatal(err)
	}
	sessions, err := blocks.trailers.WriteReserved(blocks.allocator, candidate.SessionAddresses(), protocol.BlockClientSessions, 7, encodedSessions)
	if err != nil {
		t.Fatal(err)
	}
	root, reason := protocol.DecodeHeader(checkpoint.Header[:], config.Group, uint32(cluster.MessageSizeMax), 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	prepare := connectedPrepareFrame(t, cluster, root, 7)
	copy(checkpoint.Header[:], prepare[:protocol.HeaderSize])
	checkpoint.LogicalStorageSize = blocks.allocator.LogicalStorageSize()
	checkpoint.AcquiredTrailerLastAddress, checkpoint.AcquiredTrailerLastChecksum = acquired.Last.Address, acquired.Last.Checksum
	checkpoint.AcquiredAggregateChecksum, checkpoint.AcquiredTrailerEncodedSize = acquired.Aggregate, acquired.EncodedSize
	checkpoint.ReleasedTrailerLastAddress, checkpoint.ReleasedTrailerLastChecksum = released.Last.Address, released.Last.Checksum
	checkpoint.ReleasedAggregateChecksum, checkpoint.ReleasedTrailerEncodedSize = released.Aggregate, released.EncodedSize
	checkpoint.SessionTrailerLastAddress, checkpoint.SessionTrailerLastChecksum = sessions.Last.Address, sessions.Last.Checksum
	checkpoint.SessionAggregateChecksum, checkpoint.SessionTrailerEncodedSize = sessions.Aggregate, sessions.EncodedSize
	reachable := candidate.Reachable()
	if err := blocks.allocator.CheckpointDurable(candidate, &reachable); err != nil {
		t.Fatal(err)
	}
	if err := blocks.allocator.Release(old); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	reopened, err := openBlockRuntime(storage, config, checkpoint)
	if err != nil {
		t.Fatal(err)
	}
	scratch, err := newCheckpointGraphScratch(1, reopened.allocator.acquired.Len(), cluster.BlockSize)
	if err != nil {
		t.Fatal(err)
	}
	replica := &Replica{config: config, checkpoint: checkpoint, blocks: reopened.store, trailers: reopened.trailers,
		blockAllocator: reopened.allocator, checkpointScratch: scratch, checkpointBlockLimit: 1,
		blockCatalog: make([]BlockRequirement, 8),
	}
	return replica, storage, old
}

func TestCheckpointRecoveryRestoresOnlyProvenUnreachableReleases(t *testing.T) {
	replica, _, old := checkpointWithUnrecordedRelease(t)
	if replica.blockAllocator.pending.Count() != 0 {
		t.Fatal("fixture persisted a volatile release")
	}
	if err := replica.restoreCheckpointReleases(); err != nil {
		t.Fatal(err)
	}
	index, _ := replica.blockAllocator.index(old)
	if !replica.blockAllocator.pending.Test(index) || replica.blockAllocator.released.Test(index) {
		t.Fatal("orphan not restored as pending rather than durable-free")
	}
	for _, address := range []uint64{replica.checkpoint.AcquiredTrailerLastAddress, replica.checkpoint.ReleasedTrailerLastAddress, replica.checkpoint.SessionTrailerLastAddress} {
		liveIndex, _ := replica.blockAllocator.index(address)
		if replica.blockAllocator.pending.Test(liveIndex) {
			t.Fatalf("reachable trailer %d marked pending", address)
		}
	}
	acquired, released := replica.blockAllocator.Acquired(), replica.blockAllocator.Released()
	for range replica.blockAllocator.blockCount {
		got, found := replica.nextScrubIndex(&acquired, &released)
		if !found || got == index {
			t.Fatalf("scrub selected obsolete index %d found=%t", got, found)
		}
	}
	next, err := replica.blockAllocator.Reserve()
	if err != nil {
		t.Fatal(err)
	}
	if next == old {
		t.Fatal("reconstructed release reused before a later durable checkpoint")
	}
	if err := replica.blockAllocator.Forfeit(next); err != nil {
		t.Fatal(err)
	}
}

func TestCheckpointRecoveryRejectsCorruptReachabilityBeforeRelease(t *testing.T) {
	replica, storage, _ := checkpointWithUnrecordedRelease(t)
	offset, _ := replica.config.Cluster.BlockOffset(replica.checkpoint.SessionTrailerLastAddress)
	storage.working[offset] ^= 0x80
	if err := replica.restoreCheckpointReleases(); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("corrupt live graph accepted: %v", err)
	}
	if replica.blockAllocator.pending.Count() != 0 {
		t.Fatal("unvalidated graph classified allocated blocks as obsolete")
	}
}
