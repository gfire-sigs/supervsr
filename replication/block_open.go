package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type blockRuntime struct {
	store     *BlockStore
	trailers  *TrailerStore
	allocator *BlockAllocator
}

func openBlockRuntime(storage Storage, config Config, checkpoint CheckpointState) (*blockRuntime, error) {
	store, err := NewBlockStore(storage, config.Cluster, config.Group, config.CurrentRelease)
	if err != nil {
		return nil, err
	}
	trailers, err := NewTrailerStore(store)
	if err != nil {
		return nil, err
	}
	blockBase, ok := config.Cluster.BlockBase()
	if !ok || checkpoint.LogicalStorageSize < blockBase {
		return nil, ErrInvalidCheckpoint
	}
	blockCount := (checkpoint.LogicalStorageSize - blockBase) / config.Cluster.BlockSize
	acquired, err := readCheckpointFreeSet(trailers, checkpoint.AcquiredTrailerLastAddress, checkpoint.AcquiredTrailerLastChecksum, checkpoint.AcquiredAggregateChecksum, checkpoint.AcquiredTrailerEncodedSize, uint64(checkpoint.PrepareOp()), blockCount)
	if err != nil {
		return nil, err
	}
	released, err := readCheckpointFreeSet(trailers, checkpoint.ReleasedTrailerLastAddress, checkpoint.ReleasedTrailerLastChecksum, checkpoint.ReleasedAggregateChecksum, checkpoint.ReleasedTrailerEncodedSize, uint64(checkpoint.PrepareOp()), blockCount)
	if err != nil {
		return nil, err
	}
	allocator, err := OpenBlockAllocator(storage, config.Cluster, config.Process, checkpoint, acquired, released)
	if err != nil {
		return nil, err
	}
	return &blockRuntime{store: store, trailers: trailers, allocator: allocator}, nil
}

func readCheckpointFreeSet(trailers *TrailerStore, address uint64, lastChecksum, aggregate protocol.Checksum, encodedSize, snapshot, blockCount uint64) (FixedBitSet, error) {
	set, err := NewFixedBitSet(blockCount)
	if err != nil {
		return FixedBitSet{}, err
	}
	if address == 0 {
		if encodedSize != 0 || !lastChecksum.IsZero() || aggregate != protocol.ChecksumBytes(nil) {
			return FixedBitSet{}, ErrInvalidCheckpoint
		}
		return set, nil
	}
	if encodedSize == 0 || encodedSize%8 != 0 || encodedSize > uint64(int(^uint(0)>>1)) {
		return FixedBitSet{}, ErrInvalidCheckpoint
	}
	encoded := make([]byte, encodedSize)
	capacity := trailers.blocks.cluster.BlockSize - protocol.HeaderSize
	count := (encodedSize + capacity - 1) / capacity
	if count == 0 || count > uint64(^uint32(0)) {
		return FixedBitSet{}, ErrInvalidCheckpoint
	}
	reference := TrailerReference{
		Last: BlockReference{Checksum: lastChecksum, Address: address}, Aggregate: aggregate,
		EncodedSize: encodedSize, BlockCount: uint32(count), BlockType: protocol.BlockFreeSet, Snapshot: snapshot,
	}
	if err := trailers.Read(reference, encoded); err != nil {
		return FixedBitSet{}, err
	}
	words := make([]uint64, len(encoded)/8)
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(encoded[index*8:])
	}
	if err := set.DecodeEWAH(words); err != nil {
		return FixedBitSet{}, err
	}
	return set, nil
}

func loadSessionTrailer(runtime *blockRuntime, checkpoint CheckpointState, sessions *SessionTable) error {
	if checkpoint.SessionTrailerLastAddress == 0 {
		return nil
	}
	if checkpoint.SessionTrailerEncodedSize != uint64(sessions.TrailerSize()) {
		return ErrSessionEncoding
	}
	encoded := make([]byte, sessions.TrailerSize())
	capacity := runtime.store.cluster.BlockSize - protocol.HeaderSize
	count := (checkpoint.SessionTrailerEncodedSize + capacity - 1) / capacity
	if count == 0 || count > uint64(^uint32(0)) {
		return ErrSessionEncoding
	}
	reference := TrailerReference{
		Last:      BlockReference{Checksum: checkpoint.SessionTrailerLastChecksum, Address: checkpoint.SessionTrailerLastAddress},
		Aggregate: checkpoint.SessionAggregateChecksum, EncodedSize: checkpoint.SessionTrailerEncodedSize,
		BlockCount: uint32(count), BlockType: protocol.BlockClientSessions, Snapshot: uint64(checkpoint.PrepareOp()),
	}
	if err := runtime.trailers.Read(reference, encoded); err != nil {
		return err
	}
	return sessions.DecodeTrailer(encoded, checkpoint.SessionAggregateChecksum)
}
