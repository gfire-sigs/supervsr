package replication

import (
	"encoding/binary"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type TrailerReference struct {
	Last        BlockReference
	Aggregate   protocol.Checksum
	EncodedSize uint64
	BlockCount  uint32
	BlockType   protocol.BlockType
	Snapshot    uint64
}

type TrailerStore struct {
	blocks  *BlockStore
	scratch []byte
}

func NewTrailerStore(blocks *BlockStore) (*TrailerStore, error) {
	if blocks == nil {
		return nil, ErrInvalidConfiguration
	}
	capacity := blocks.cluster.BlockSize - protocol.HeaderSize
	return &TrailerStore{blocks: blocks, scratch: make([]byte, capacity)}, nil
}

func (store *TrailerStore) Write(allocator *BlockAllocator, blockType protocol.BlockType, snapshot uint64, encoded []byte) (TrailerReference, error) {
	validType := blockType == protocol.BlockFreeSet || blockType == protocol.BlockClientSessions
	if allocator == nil || len(encoded) == 0 || !validType {
		return TrailerReference{}, ErrInvalidBlock
	}
	capacity := int(store.blocks.cluster.BlockSize - protocol.HeaderSize)
	count := (len(encoded) + capacity - 1) / capacity
	addresses := make([]uint64, count)
	for index := range addresses {
		address, err := allocator.Reserve()
		if err != nil {
			store.forfeit(allocator, addresses[:index])
			return TrailerReference{}, err
		}
		addresses[index] = address
	}
	return store.WriteReserved(allocator, addresses, blockType, snapshot, encoded)
}

func (store *TrailerStore) WriteReserved(allocator *BlockAllocator, addresses []uint64, blockType protocol.BlockType, snapshot uint64, encoded []byte) (TrailerReference, error) {
	validType := blockType == protocol.BlockFreeSet || blockType == protocol.BlockClientSessions
	capacity := int(store.blocks.cluster.BlockSize - protocol.HeaderSize)
	if allocator == nil || len(addresses) == 0 || len(encoded) < len(addresses) || len(encoded) > len(addresses)*capacity || !validType {
		return TrailerReference{}, ErrInvalidBlock
	}
	for _, address := range addresses {
		index, ok := allocator.index(address)
		if !ok || !allocator.reserved.Test(index) {
			return TrailerReference{}, ErrBlockReservation
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
			return TrailerReference{}, err
		}
		if err := allocator.Publish(address); err != nil {
			store.rollback(allocator, addresses, published)
			return TrailerReference{}, err
		}
		published++
		previous = reference
		cursor = end
	}
	return TrailerReference{
		Last: previous, Aggregate: protocol.ChecksumBytes(encoded), EncodedSize: uint64(len(encoded)),
		BlockCount: uint32(len(addresses)), BlockType: blockType, Snapshot: snapshot,
	}, nil
}

func (store *TrailerStore) Read(reference TrailerReference, destination []byte) error {
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
	if invalidEnd || protocol.ChecksumBytes(destination) != reference.Aggregate {
		return ErrInvalidBlock
	}
	return nil
}

func (store *TrailerStore) rollback(allocator *BlockAllocator, addresses []uint64, published int) {
	for index, address := range addresses {
		if index < published {
			_ = allocator.Release(address)
		} else {
			_ = allocator.Forfeit(address)
		}
	}
}

func (store *TrailerStore) forfeit(allocator *BlockAllocator, addresses []uint64) {
	for _, address := range addresses {
		_ = allocator.Forfeit(address)
	}
}
