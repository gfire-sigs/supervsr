package replication

import (
	"errors"
	"sync"
)

var (
	ErrCheckpointBlocksClosed = errors.New("replication: checkpoint block transaction is closed")
	ErrCheckpointBlockLimit   = errors.New("replication: checkpoint block reservation limit reached")
)

type checkpointBlockTransactionState uint8

const (
	checkpointBlockTransactionIdle checkpointBlockTransactionState = iota
	checkpointBlockTransactionOpen
	checkpointBlockTransactionSealed
)

type checkpointBlockState uint8

const (
	checkpointBlockReserved checkpointBlockState = iota + 1
	checkpointBlockWritten
)

type checkpointBlockRecord struct {
	address   uint64
	blockType BlockType
	reference BlockReference
	state     checkpointBlockState
}

// CheckpointBlockTransaction owns the bounded block reservations for one checkpoint.
type CheckpointBlockTransaction struct {
	mu         sync.Mutex
	allocator  *BlockAllocator
	store      *BlockStore
	records    []checkpointBlockRecord
	limit      uint32
	generation uint64
	snapshot   uint64
	state      checkpointBlockTransactionState
}

// CheckpointBlock is an immutable application block reservation.
type CheckpointBlock struct {
	transaction *CheckpointBlockTransaction
	generation  uint64
	slot        uint32
	address     uint64
	blockType   BlockType
}

// CheckpointBlockReader reads immutable application blocks selected by the durable free set.
type CheckpointBlockReader struct {
	allocator *BlockAllocator
	store     *BlockStore
}

func newCheckpointBlockReader(allocator *BlockAllocator, store *BlockStore) *CheckpointBlockReader {
	return &CheckpointBlockReader{allocator: allocator, store: store}
}

// Read verifies ownership, address, checksum, schema, and padding before returning block data.
func (reader *CheckpointBlockReader) Read(reference BlockReference, blockType BlockType, destination []byte) (BlockReadResult, error) {
	if reader == nil || reader.allocator == nil || reader.store == nil || blockType < BlockManifest || blockType > BlockValue {
		return BlockReadResult{}, ErrInvalidBlock
	}
	index, ok := reader.allocator.index(reference.Address)
	if !ok || !reader.allocator.acquired.Test(index) || reader.allocator.released.Test(index) {
		return BlockReadResult{}, ErrBlockMissing
	}
	return reader.store.Read(reference, blockType, destination)
}

func newCheckpointBlockTransaction(allocator *BlockAllocator, store *BlockStore, limit uint32) *CheckpointBlockTransaction {
	return &CheckpointBlockTransaction{
		allocator: allocator,
		store:     store,
		records:   make([]checkpointBlockRecord, 0, limit),
		limit:     limit,
	}
}

func (transaction *CheckpointBlockTransaction) setRuntime(allocator *BlockAllocator, store *BlockStore) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state != checkpointBlockTransactionIdle || allocator == nil || store == nil {
		return ErrCheckpointBlocksClosed
	}
	transaction.allocator = allocator
	transaction.store = store
	return nil
}

func (transaction *CheckpointBlockTransaction) begin(snapshot uint64) error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state != checkpointBlockTransactionIdle || snapshot == 0 {
		return ErrCheckpointBlocksClosed
	}
	transaction.records = transaction.records[:0]
	transaction.generation++
	transaction.snapshot = snapshot
	transaction.state = checkpointBlockTransactionOpen
	return nil
}

// Reserve selects the next deterministic logical address. Reservations are allowed only during StartCheckpoint.
func (transaction *CheckpointBlockTransaction) Reserve(blockType BlockType) (CheckpointBlock, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state != checkpointBlockTransactionOpen {
		return CheckpointBlock{}, ErrCheckpointBlocksClosed
	}
	if blockType < BlockManifest || blockType > BlockValue {
		return CheckpointBlock{}, ErrInvalidBlock
	}
	if uint32(len(transaction.records)) >= transaction.limit {
		return CheckpointBlock{}, ErrCheckpointBlockLimit
	}
	address, err := transaction.allocator.Reserve()
	if err != nil {
		return CheckpointBlock{}, err
	}
	slot := uint32(len(transaction.records))
	transaction.records = append(transaction.records, checkpointBlockRecord{
		address:   address,
		blockType: blockType,
		state:     checkpointBlockReserved,
	})
	return CheckpointBlock{
		transaction: transaction,
		generation:  transaction.generation,
		slot:        slot,
		address:     address,
		blockType:   blockType,
	}, nil
}

// Address returns the one-based logical address selected for the reservation.
func (block CheckpointBlock) Address() uint64 {
	return block.address
}

// Type returns the immutable schema selected for the reservation.
func (block CheckpointBlock) Type() BlockType {
	return block.blockType
}

// Write durably writes the immutable block. Its logical address remains unpublished until checkpoint completion.
func (block CheckpointBlock) Write(metadata [96]byte, body []byte) (BlockReference, error) {
	if block.transaction == nil {
		return BlockReference{}, ErrCheckpointBlocksClosed
	}
	return block.transaction.write(block, metadata, body)
}

func (transaction *CheckpointBlockTransaction) write(block CheckpointBlock, metadata [96]byte, body []byte) (BlockReference, error) {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state == checkpointBlockTransactionIdle || block.generation != transaction.generation || block.slot >= uint32(len(transaction.records)) {
		return BlockReference{}, ErrCheckpointBlocksClosed
	}
	record := &transaction.records[block.slot]
	if record.address != block.address || record.blockType != block.blockType || record.state != checkpointBlockReserved {
		return BlockReference{}, ErrCheckpointBlocksClosed
	}
	reference, err := transaction.store.Write(record.address, transaction.snapshot, record.blockType, metadata, body)
	if err != nil {
		return BlockReference{}, err
	}
	record.reference = reference
	record.state = checkpointBlockWritten
	return reference, nil
}

func (transaction *CheckpointBlockTransaction) seal() error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state != checkpointBlockTransactionOpen {
		return ErrCheckpointBlocksClosed
	}
	transaction.state = checkpointBlockTransactionSealed
	return nil
}

func (transaction *CheckpointBlockTransaction) commit() error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state != checkpointBlockTransactionSealed || !transaction.reservationsValid() {
		return ErrCheckpointBlocksClosed
	}
	for index := range transaction.records {
		record := &transaction.records[index]
		var err error
		if record.state == checkpointBlockWritten {
			err = transaction.allocator.Publish(record.address)
		} else {
			err = transaction.allocator.Forfeit(record.address)
		}
		if err != nil {
			return err
		}
	}
	transaction.finish()
	return nil
}

func (transaction *CheckpointBlockTransaction) abort() error {
	transaction.mu.Lock()
	defer transaction.mu.Unlock()
	if transaction.state == checkpointBlockTransactionIdle {
		return nil
	}
	if !transaction.reservationsValid() {
		return ErrCheckpointBlocksClosed
	}
	for index := range transaction.records {
		if err := transaction.allocator.Forfeit(transaction.records[index].address); err != nil {
			return err
		}
	}
	transaction.finish()
	return nil
}

func (transaction *CheckpointBlockTransaction) reservationsValid() bool {
	for index := range transaction.records {
		if !transaction.allocator.reservationValid(transaction.records[index].address) {
			return false
		}
	}
	return true
}

func (transaction *CheckpointBlockTransaction) finish() {
	transaction.records = transaction.records[:0]
	transaction.snapshot = 0
	transaction.state = checkpointBlockTransactionIdle
}
