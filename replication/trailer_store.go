package replication

import (
	"encoding/binary"

	"z.gfire.dev/supervsr/replication/protocol"
)

type trailerReference struct {
	Last        BlockReference
	Aggregate   protocol.Checksum
	EncodedSize uint64
	BlockCount  uint32
	BlockType   protocol.BlockType
	Snapshot    uint64
}

type trailerStore struct {
	blocks  *blockStore
	scratch []byte
}

func newTrailerStore(blocks *blockStore) (*trailerStore, error) {
	if blocks == nil {
		return nil, ErrInvalidConfiguration
	}
	capacity := blocks.cluster.BlockSize - protocol.HeaderSize
	return &trailerStore{blocks: blocks, scratch: make([]byte, capacity)}, nil
}

func (store *trailerStore) Write(allocator *blockSpaceAllocator, blockType protocol.BlockType, snapshot uint64, encoded []byte) (trailerReference, error) {
	validType := blockType == protocol.BlockFreeSet || blockType == protocol.BlockClientSessions
	if allocator == nil || len(encoded) == 0 || !validType {
		return trailerReference{}, ErrInvalidBlock
	}
	capacity := int(store.blocks.cluster.BlockSize - protocol.HeaderSize)
	count := (len(encoded) + capacity - 1) / capacity
	addresses := make([]uint64, count)
	for index := range addresses {
		address, err := allocator.Reserve()
		if err != nil {
			store.forfeit(allocator, addresses[:index])
			return trailerReference{}, err
		}
		addresses[index] = address
	}
	return store.WriteReserved(allocator, addresses, blockType, snapshot, encoded)
}

func (store *trailerStore) WriteReserved(allocator *blockSpaceAllocator, addresses []uint64, blockType protocol.BlockType, snapshot uint64, encoded []byte) (trailerReference, error) {
	validType := blockType == protocol.BlockFreeSet || blockType == protocol.BlockClientSessions
	capacity := int(store.blocks.cluster.BlockSize - protocol.HeaderSize)
	if allocator == nil || len(addresses) == 0 || len(encoded) < len(addresses) || len(encoded) > len(addresses)*capacity || !validType {
		return trailerReference{}, ErrInvalidBlock
	}
	for _, address := range addresses {
		index, ok := allocator.index(address)
		if !ok || !allocator.reserved.Test(index) {
			return trailerReference{}, errBlockReservation
		}
	}
	var previous BlockReference
	published := 0
	cursor := 0
	for index, address := range addresses {
		blocksRemaining := len(addresses) - index
		bytesRemaining := len(encoded) - cursor
		chunk := (bytesRemaining + blocksRemaining - 1) / blocksRemaining
		end := cursor + chunk
		var metadata [96]byte
		copy(metadata[:16], previous.Checksum[:])
		binary.LittleEndian.PutUint64(metadata[32:40], previous.Address)
		reference, err := store.blocks.Write(address, snapshot, blockType, metadata, encoded[cursor:end])
		if err != nil {
			store.rollback(allocator, addresses, published)
			return trailerReference{}, err
		}
		if err := allocator.Publish(address); err != nil {
			store.rollback(allocator, addresses, published)
			return trailerReference{}, err
		}
		published++
		previous = reference
		cursor = end
	}
	aggregate, err := protocol.ChecksumBytes(store.blocks.cluster.ChecksumAlgorithm, encoded)
	if err != nil {
		store.rollback(allocator, addresses, published)
		return trailerReference{}, err
	}
	return trailerReference{
		Last: previous, Aggregate: aggregate, EncodedSize: uint64(len(encoded)),
		BlockCount: uint32(len(addresses)), BlockType: blockType, Snapshot: snapshot,
	}, nil
}

func (store *trailerStore) Read(reference trailerReference, destination []byte) error {
	validType := reference.BlockType == protocol.BlockFreeSet || reference.BlockType == protocol.BlockClientSessions
	invalidReference := reference.Last.Address == 0 || reference.Last.Checksum.IsZero() || reference.EncodedSize == 0 || reference.BlockCount == 0
	if invalidReference || !validType || uint64(len(destination)) != reference.EncodedSize {
		return ErrInvalidBlock
	}
	capacity := uint64(store.blocks.cluster.BlockSize - protocol.HeaderSize)
	minimumCount := (reference.EncodedSize + capacity - 1) / capacity
	if uint64(reference.BlockCount) < minimumCount || uint64(reference.BlockCount) > reference.EncodedSize {
		return ErrInvalidBlock
	}
	cursor := reference.EncodedSize
	current := reference.Last
	for remaining := reference.BlockCount; remaining > 0; remaining-- {
		result, err := store.blocks.Read(current, reference.BlockType, store.scratch)
		if err != nil {
			return err
		}
		if result.Snapshot != reference.Snapshot || uint64(result.BodySize) > cursor {
			return ErrInvalidBlock
		}
		start := cursor - uint64(result.BodySize)
		copy(destination[start:cursor], store.scratch[:result.BodySize])
		cursor = start
		var previousChecksum protocol.Checksum
		copy(previousChecksum[:], result.Metadata[:16])
		previousAddress := binary.LittleEndian.Uint64(result.Metadata[32:40])
		current = BlockReference{Checksum: previousChecksum, Address: previousAddress}
	}
	invalidEnd := cursor != 0 || current.Address != 0 || !current.Checksum.IsZero()
	aggregate, err := protocol.ChecksumBytes(store.blocks.cluster.ChecksumAlgorithm, destination)
	if invalidEnd || err != nil || aggregate != reference.Aggregate {
		return ErrInvalidBlock
	}
	return nil
}

func (store *trailerStore) rollback(allocator *blockSpaceAllocator, addresses []uint64, published int) {
	for index, address := range addresses {
		if index < published {
			_ = allocator.Release(address)
		} else {
			_ = allocator.Forfeit(address)
		}
	}
}

func (store *trailerStore) forfeit(allocator *blockSpaceAllocator, addresses []uint64) {
	for _, address := range addresses {
		_ = allocator.Forfeit(address)
	}
}
