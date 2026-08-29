package replication

import (
	"encoding/binary"
	"errors"
)

var (
	ErrStorageExhausted = errors.New("replication: storage size limit exhausted")
	ErrBlockReservation = errors.New("replication: invalid block reservation")
)

type BlockCheckpointCandidate struct {
	generation      uint64
	reachable       FixedBitSet
	blockCount      uint64
	acquired        FixedBitSet
	released        FixedBitSet
	addresses       []uint64
	acquiredBlocks  int
	releasedBlocks  int
	sessionBlocks   int
	acquiredEncoded []byte
	releasedEncoded []byte
}

func (candidate BlockCheckpointCandidate) Reachable() FixedBitSet { return candidate.reachable }

func (candidate BlockCheckpointCandidate) Acquired() FixedBitSet   { return candidate.acquired }
func (candidate BlockCheckpointCandidate) Released() FixedBitSet   { return candidate.released }
func (candidate BlockCheckpointCandidate) AcquiredEncoded() []byte { return candidate.acquiredEncoded }
func (candidate BlockCheckpointCandidate) ReleasedEncoded() []byte { return candidate.releasedEncoded }

func (candidate BlockCheckpointCandidate) AcquiredAddresses() []uint64 {
	return candidate.addresses[:candidate.acquiredBlocks]
}

func (candidate BlockCheckpointCandidate) ReleasedAddresses() []uint64 {
	start := candidate.acquiredBlocks
	return candidate.addresses[start : start+candidate.releasedBlocks]
}

func (candidate BlockCheckpointCandidate) SessionAddresses() []uint64 {
	start := candidate.acquiredBlocks + candidate.releasedBlocks
	return candidate.addresses[start : start+candidate.sessionBlocks]
}

type BlockAllocator struct {
	storage    Storage
	base       uint64
	blockSize  uint64
	limit      uint64
	logical    uint64
	blockCount uint64
	maximum    uint64
	acquired   FixedBitSet
	released   FixedBitSet
	reserved   FixedBitSet
	pending    FixedBitSet

	reservedReleased     FixedBitSet
	checkpoint           FixedBitSet
	checkpointAcquired   FixedBitSet
	checkpointReleased   FixedBitSet
	checkpointGeneration uint64
	checkpointActive     bool
}

func OpenBlockAllocator(storage Storage, cluster ClusterConfig, process ProcessConfig, state CheckpointState, acquired, released FixedBitSet) (*BlockAllocator, error) {
	base, ok := cluster.BlockBase()
	if storage == nil || !ok || process.StorageSizeLimit < base || cluster.BlockSize == 0 {
		return nil, ErrInvalidConfiguration
	}
	if state.LogicalStorageSize < base || (state.LogicalStorageSize-base)%cluster.BlockSize != 0 || state.LogicalStorageSize > process.StorageSizeLimit {
		return nil, ErrInvalidCheckpoint
	}
	blockCount := (state.LogicalStorageSize - base) / cluster.BlockSize
	maximum := (process.StorageSizeLimit - base) / cluster.BlockSize
	capacity := allocatorBitCapacity(blockCount, maximum)
	if acquired.Len() != blockCount || released.Len() != blockCount {
		return nil, ErrInvalidCheckpoint
	}
	acquiredMax, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	releasedMax, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	reserved, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	pending, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	reservedReleased, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	checkpoint, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	checkpointAcquired, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	checkpointReleased, err := NewFixedBitSet(capacity)
	if err != nil {
		return nil, err
	}
	copy(acquiredMax.words, acquired.words)
	copy(releasedMax.words, released.words)
	for index := uint64(0); index < blockCount; index++ {
		if releasedMax.Test(index) && !acquiredMax.Test(index) {
			return nil, ErrInvalidCheckpoint
		}
	}
	return &BlockAllocator{
		storage: storage, base: base, blockSize: cluster.BlockSize, limit: process.StorageSizeLimit,
		logical: state.LogicalStorageSize, blockCount: blockCount, maximum: maximum,
		acquired: acquiredMax, released: releasedMax, reserved: reserved, pending: pending,
		reservedReleased: reservedReleased, checkpoint: checkpoint,
		checkpointAcquired: checkpointAcquired, checkpointReleased: checkpointReleased,
	}, nil
}

func allocatorBitCapacity(blockCount, maximum uint64) uint64 {
	const chunk = uint64(4096)
	if maximum == 0 {
		return 0
	}
	target := max(blockCount, uint64(1))
	rounded := ((target-1)/chunk + 1) * chunk
	return min(maximum, rounded)
}

func (allocator *BlockAllocator) ensureCapacity(required uint64) error {
	if required <= allocator.acquired.Len() {
		return nil
	}
	capacity := allocatorBitCapacity(required, allocator.maximum)
	sets := [...]*FixedBitSet{
		&allocator.acquired, &allocator.released, &allocator.reserved, &allocator.pending,
		&allocator.reservedReleased, &allocator.checkpoint, &allocator.checkpointAcquired, &allocator.checkpointReleased,
	}
	for _, set := range sets {
		if err := set.grow(capacity); err != nil {
			return err
		}
	}
	return nil
}

func (allocator *BlockAllocator) Reserve() (uint64, error) {
	for index := uint64(0); index < allocator.blockCount; index++ {
		available := !allocator.acquired.Test(index) || allocator.released.Test(index)
		if available && !allocator.reserved.Test(index) {
			allocator.reserved.Set(index)
			if allocator.released.Test(index) {
				allocator.reservedReleased.Set(index)
			}
			allocator.released.Clear(index)
			return allocator.address(index), nil
		}
	}
	if allocator.blockCount >= allocator.maximum {
		return 0, ErrStorageExhausted
	}
	if err := allocator.ensureCapacity(allocator.blockCount + 1); err != nil {
		return 0, err
	}
	index := allocator.blockCount
	logical, ok := checkedAdd(allocator.logical, allocator.blockSize)
	if !ok || logical > allocator.limit {
		return 0, ErrStorageExhausted
	}
	if err := allocator.storage.Resize(logical); err != nil {
		return 0, err
	}
	if err := allocator.storage.Sync(); err != nil {
		return 0, err
	}
	allocator.logical = logical
	allocator.blockCount++
	allocator.reserved.Set(index)
	return allocator.address(index), nil
}

func (allocator *BlockAllocator) Publish(address uint64) error {
	index, ok := allocator.index(address)
	if !ok || !allocator.reserved.Test(index) {
		return ErrBlockReservation
	}
	allocator.reserved.Clear(index)
	allocator.reservedReleased.Clear(index)
	allocator.released.Clear(index)
	allocator.acquired.Set(index)
	return nil
}

func (allocator *BlockAllocator) Forfeit(address uint64) error {
	index, ok := allocator.index(address)
	if !ok || !allocator.reserved.Test(index) {
		return ErrBlockReservation
	}
	allocator.reserved.Clear(index)

	if allocator.reservedReleased.Test(index) {
		allocator.released.Set(index)
		allocator.reservedReleased.Clear(index)
	}
	return nil
}
func (allocator *BlockAllocator) PrepareCheckpoint(sessionEncodedSize, payloadSize uint64) (BlockCheckpointCandidate, error) {
	if allocator.checkpointActive || allocator.prefix(&allocator.reserved).Count() != 0 {
		return BlockCheckpointCandidate{}, ErrBlockReservation
	}
	if payloadSize == 0 || sessionEncodedSize == 0 {
		return BlockCheckpointCandidate{}, ErrBlockReservation
	}
	sessionBlocks := int((sessionEncodedSize + payloadSize - 1) / payloadSize)
	addresses := make([]uint64, 0, sessionBlocks+2)
	for range sessionBlocks + 2 {
		address, err := allocator.Reserve()
		if err != nil {
			allocator.forfeitAddresses(addresses)
			return BlockCheckpointCandidate{}, err
		}
		addresses = append(addresses, address)
	}
	for range 64 {
		acquired, released, err := allocator.checkpointSets(addresses)
		if err != nil {
			allocator.forfeitAddresses(addresses)
			return BlockCheckpointCandidate{}, err
		}
		acquiredEncoded, err := encodeEWAHBytes(acquired)
		if err != nil {
			allocator.forfeitAddresses(addresses)
			return BlockCheckpointCandidate{}, err
		}
		releasedEncoded, err := encodeEWAHBytes(released)
		if err != nil {
			allocator.forfeitAddresses(addresses)
			return BlockCheckpointCandidate{}, err
		}
		acquiredBlocks := int((uint64(len(acquiredEncoded)) + payloadSize - 1) / payloadSize)
		releasedBlocks := int((uint64(len(releasedEncoded)) + payloadSize - 1) / payloadSize)
		required := acquiredBlocks + releasedBlocks + sessionBlocks
		if required > len(addresses) {
			for range required - len(addresses) {
				address, reserveErr := allocator.Reserve()
				if reserveErr != nil {
					allocator.forfeitAddresses(addresses)
					return BlockCheckpointCandidate{}, reserveErr
				}
				addresses = append(addresses, address)
			}
			continue
		}
		if required < len(addresses) {
			allocator.forfeitAddresses(addresses[required:])
			addresses = addresses[:required]
			continue
		}
		reachable, err := NewFixedBitSet(acquired.Len())
		if err != nil {
			allocator.forfeitAddresses(addresses)
			return BlockCheckpointCandidate{}, err
		}
		copy(reachable.words, acquired.words)
		for index := range reachable.words {
			reachable.words[index] &^= released.words[index]
		}
		allocator.installCheckpointSets(acquired, released)
		allocator.checkpointGeneration++
		allocator.checkpointActive = true
		return BlockCheckpointCandidate{
			generation: allocator.checkpointGeneration, blockCount: allocator.blockCount,
			acquired: acquired, released: released, reachable: reachable, addresses: addresses,
			acquiredBlocks: acquiredBlocks, releasedBlocks: releasedBlocks, sessionBlocks: sessionBlocks,
			acquiredEncoded: acquiredEncoded, releasedEncoded: releasedEncoded,
		}, nil
	}
	allocator.forfeitAddresses(addresses)
	return BlockCheckpointCandidate{}, ErrBlockReservation
}

func (allocator *BlockAllocator) checkpointSets(addresses []uint64) (FixedBitSet, FixedBitSet, error) {
	acquired, err := NewFixedBitSet(allocator.blockCount)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, err
	}
	released, err := NewFixedBitSet(allocator.blockCount)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, err
	}
	copy(acquired.words, allocator.prefix(&allocator.acquired).words)
	copy(released.words, allocator.prefix(&allocator.released).words)
	for index := range released.words {
		released.words[index] |= allocator.prefix(&allocator.pending).words[index]
	}
	for _, address := range addresses {
		index, ok := allocator.index(address)
		if !ok {
			return FixedBitSet{}, FixedBitSet{}, ErrBlockReservation
		}
		acquired.Set(index)
		released.Clear(index)
	}
	return acquired, released, nil
}

func (allocator *BlockAllocator) installCheckpointSets(acquired, released FixedBitSet) {
	clear(allocator.checkpoint.words)
	clear(allocator.checkpointAcquired.words)
	clear(allocator.checkpointReleased.words)
	copy(allocator.checkpoint.words, allocator.pending.words)
	copy(allocator.checkpointAcquired.words, acquired.words)
	copy(allocator.checkpointReleased.words, released.words)
}

func (allocator *BlockAllocator) forfeitAddresses(addresses []uint64) {
	for _, address := range addresses {
		_ = allocator.Forfeit(address)
	}
}

func encodeEWAHBytes(set FixedBitSet) ([]byte, error) {
	words := make([]uint64, len(set.words)*2+1)
	count, err := set.EncodeEWAH(words)
	if err != nil {
		return nil, err
	}
	encoded := make([]byte, count*8)
	for index := range count {
		binary.LittleEndian.PutUint64(encoded[index*8:], words[index])
	}
	return encoded, nil
}

func (allocator *BlockAllocator) Release(address uint64) error {
	index, ok := allocator.index(address)
	if !ok || !allocator.acquired.Test(index) || allocator.reserved.Test(index) || allocator.released.Test(index) || allocator.pending.Test(index) {
		return ErrBlockReservation
	}
	allocator.pending.Set(index)
	return nil
}

func (allocator *BlockAllocator) BeginCheckpoint() (BlockCheckpointCandidate, error) {
	if allocator.checkpointActive || allocator.prefix(&allocator.reserved).Count() != 0 {
		return BlockCheckpointCandidate{}, ErrBlockReservation
	}
	clear(allocator.checkpoint.words)
	clear(allocator.checkpointAcquired.words)
	clear(allocator.checkpointReleased.words)
	copy(allocator.checkpoint.words, allocator.pending.words)
	copy(allocator.checkpointAcquired.words, allocator.acquired.words)
	copy(allocator.checkpointReleased.words, allocator.released.words)
	for index := range allocator.checkpointReleased.words {
		allocator.checkpointReleased.words[index] |= allocator.checkpoint.words[index]
	}
	allocator.checkpointGeneration++
	allocator.checkpointActive = true
	return BlockCheckpointCandidate{
		generation: allocator.checkpointGeneration,
		blockCount: allocator.blockCount,
		acquired:   allocator.prefix(&allocator.checkpointAcquired),
		released:   allocator.prefix(&allocator.checkpointReleased),
	}, nil
}

func (allocator *BlockAllocator) CheckpointDurable(candidate BlockCheckpointCandidate, reachable *FixedBitSet) error {
	if !allocator.checkpointActive || candidate.generation != allocator.checkpointGeneration || candidate.blockCount > allocator.blockCount {
		return ErrBlockReservation
	}
	if reachable == nil || reachable.Len() < candidate.blockCount {
		return ErrBlockReservation
	}
	for _, address := range candidate.addresses {
		index, ok := allocator.index(address)
		if !ok || allocator.reserved.Test(index) || !allocator.acquired.Test(index) || allocator.released.Test(index) || allocator.pending.Test(index) {
			return ErrBlockReservation
		}
	}
	for index := uint64(0); index < candidate.blockCount; index++ {
		if allocator.checkpoint.Test(index) && reachable.Test(index) {
			return ErrBlockReservation
		}
	}
	for index := uint64(0); index < candidate.blockCount; index++ {
		if !allocator.checkpoint.Test(index) {
			continue
		}
		allocator.pending.Clear(index)
		allocator.released.Set(index)
	}
	clear(allocator.checkpoint.words)
	clear(allocator.checkpointAcquired.words)
	clear(allocator.checkpointReleased.words)
	allocator.checkpointActive = false
	return nil
}

func (allocator *BlockAllocator) AbortCheckpoint(candidate BlockCheckpointCandidate) error {
	if !allocator.checkpointActive || candidate.generation != allocator.checkpointGeneration {
		return ErrBlockReservation
	}
	for _, address := range candidate.addresses {
		index, ok := allocator.index(address)
		if !ok {
			return ErrBlockReservation
		}
		if allocator.reserved.Test(index) {
			if err := allocator.Forfeit(address); err != nil {
				return err
			}
			continue
		}
		if allocator.acquired.Test(index) && !allocator.pending.Test(index) && !allocator.released.Test(index) {
			if err := allocator.Release(address); err != nil {
				return err
			}
		}
	}
	clear(allocator.checkpoint.words)
	clear(allocator.checkpointAcquired.words)
	clear(allocator.checkpointReleased.words)
	allocator.checkpointActive = false
	return nil
}

func (allocator *BlockAllocator) LogicalStorageSize() uint64 { return allocator.logical }
func (allocator *BlockAllocator) AcquiredCount() uint64 {
	return allocator.prefix(&allocator.acquired).Count()
}
func (allocator *BlockAllocator) ReleasedCount() uint64 {
	return allocator.prefix(&allocator.released).Count()
}

func (allocator *BlockAllocator) Acquired() FixedBitSet { return allocator.prefix(&allocator.acquired) }
func (allocator *BlockAllocator) Released() FixedBitSet { return allocator.prefix(&allocator.released) }

func (allocator *BlockAllocator) prefix(set *FixedBitSet) FixedBitSet {
	words := (allocator.blockCount + 63) / 64
	return FixedBitSet{words: set.words[:int(words)], length: allocator.blockCount}
}

func (allocator *BlockAllocator) address(index uint64) uint64 {
	return allocator.base + index*allocator.blockSize
}

func (allocator *BlockAllocator) index(address uint64) (uint64, bool) {
	if address < allocator.base || (address-allocator.base)%allocator.blockSize != 0 {
		return 0, false
	}
	index := (address - allocator.base) / allocator.blockSize
	return index, index < allocator.blockCount
}

func EmptyBlockSets(state CheckpointState, cluster ClusterConfig) (FixedBitSet, FixedBitSet, error) {
	base, ok := cluster.BlockBase()
	if !ok || state.LogicalStorageSize < base || (state.LogicalStorageSize-base)%cluster.BlockSize != 0 {
		return FixedBitSet{}, FixedBitSet{}, ErrInvalidCheckpoint
	}
	count := (state.LogicalStorageSize - base) / cluster.BlockSize
	acquired, err := NewFixedBitSet(count)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, err
	}
	released, err := NewFixedBitSet(count)
	if err != nil {
		return FixedBitSet{}, FixedBitSet{}, err
	}
	return acquired, released, nil
}
