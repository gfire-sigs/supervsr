package replication

import (
	"encoding/binary"

	"z.gfire.dev/supervsr/replication/protocol"
)

type blockRuntime struct {
	store     *blockStore
	trailers  *trailerStore
	allocator *blockSpaceAllocator
}

func openBlockRuntime(storage Storage, config Config, checkpoint CheckpointState) (*blockRuntime, error) {
	store, err := newBlockStore(storage, config.Cluster, config.Group, config.CurrentRelease)
	if err != nil {
		return nil, err
	}
	trailers, err := newTrailerStore(store)
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
	allocator, err := openBlockAllocator(storage, config.Cluster, config.Process, checkpoint, acquired, released)
	if err != nil {
		return nil, err
	}
	return &blockRuntime{store: store, trailers: trailers, allocator: allocator}, nil
}

func newBlockRepairRuntime(storage Storage, config Config) (*blockRuntime, error) {
	store, err := newBlockStore(storage, config.Cluster, config.Group, config.CurrentRelease)
	if err != nil {
		return nil, err
	}
	trailers, err := newTrailerStore(store)
	if err != nil {
		return nil, err
	}
	return &blockRuntime{store: store, trailers: trailers}, nil
}

func readCheckpointFreeSet(trailers *trailerStore, address uint64, lastChecksum, aggregate protocol.Checksum, encodedSize, snapshot, blockCount uint64) (fixedBitSet, error) {
	set, err := newFixedBitSet(blockCount)
	if err != nil {
		return fixedBitSet{}, err
	}
	emptyChecksum, err := protocol.ChecksumBytes(trailers.blocks.cluster.ChecksumAlgorithm, nil)
	if err != nil {
		return fixedBitSet{}, ErrInvalidCheckpoint
	}
	if address == 0 {
		if encodedSize != 0 || !lastChecksum.IsZero() || aggregate != emptyChecksum {
			return fixedBitSet{}, ErrInvalidCheckpoint
		}
		return set, nil
	}
	if encodedSize == 0 || encodedSize%8 != 0 || encodedSize > uint64(int(^uint(0)>>1)) {
		return fixedBitSet{}, ErrInvalidCheckpoint
	}
	encoded := make([]byte, encodedSize)
	capacity := trailers.blocks.cluster.BlockSize - protocol.HeaderSize
	count := (encodedSize + capacity - 1) / capacity
	if count == 0 || count > uint64(^uint32(0)) {
		return fixedBitSet{}, ErrInvalidCheckpoint
	}
	reference := trailerReference{
		Last: BlockReference{Checksum: lastChecksum, Address: address}, Aggregate: aggregate,
		EncodedSize: encodedSize, BlockCount: uint32(count), BlockType: protocol.BlockFreeSet, Snapshot: snapshot,
	}
	if err := trailers.Read(reference, encoded); err != nil {
		return fixedBitSet{}, err
	}
	words := make([]uint64, len(encoded)/8)
	for index := range words {
		words[index] = binary.LittleEndian.Uint64(encoded[index*8:])
	}
	if err := set.DecodeEWAH(words); err != nil {
		return fixedBitSet{}, err
	}
	return set, nil
}

func loadSessionTrailer(runtime *blockRuntime, checkpoint CheckpointState, sessions *sessionTable) error {
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
	reference := trailerReference{
		Last:      BlockReference{Checksum: checkpoint.SessionTrailerLastChecksum, Address: checkpoint.SessionTrailerLastAddress},
		Aggregate: checkpoint.SessionAggregateChecksum, EncodedSize: checkpoint.SessionTrailerEncodedSize,
		BlockCount: uint32(count), BlockType: protocol.BlockClientSessions, Snapshot: uint64(checkpoint.PrepareOp()),
	}
	if err := runtime.trailers.Read(reference, encoded); err != nil {
		return err
	}
	return sessions.DecodeTrailer(encoded, checkpoint.SessionAggregateChecksum)
}
