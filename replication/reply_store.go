package replication

import (
	"encoding/binary"
	"errors"
	"fmt"
	"sync"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrInvalidReplyStore = errors.New("replication: invalid durable reply")

type ReplyStore struct {
	mu          sync.Mutex
	storage     Storage
	layout      WALLayout
	clientsMax  uint32
	group       protocol.GroupID
	memberCount uint8
	buffer      []byte
	ioActive    bool
}

func NewReplyStore(storage Storage, config ClusterConfig, group protocol.GroupID, memberCount uint8) (*ReplyStore, error) {
	layout, ok := DeriveWALLayout(config)
	if !ok || config.ClientsMax == 0 || config.ClientsMax > uint64(^uint32(0)) || memberCount == 0 {
		return nil, ErrInvalidReplyStore
	}
	buffer, err := NewAlignedBuffer(layout.ReplyStride, SectorSize)
	if err != nil {
		return nil, err
	}
	return &ReplyStore{
		storage:     storage,
		layout:      layout,
		clientsMax:  uint32(config.ClientsMax),
		group:       group,
		memberCount: memberCount,
		buffer:      buffer,
	}, nil
}

func (store *ReplyStore) Write(slot uint32, frame []byte) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	operation := IOOperation{Kind: IOReplyWrite, Offset: uint64(slot), Buffer: frame}
	var sequence ioSequence
	if err := store.beginIO(&sequence, &operation); err != nil {
		return err
	}
	defer func() { store.ioActive = false }()
	for sequence.phase != IOPhaseCompletion {
		if err := store.advanceIO(&sequence, &operation); err != nil {
			return err
		}
	}
	return nil
}

func (store *ReplyStore) Read(slot uint32, expected protocol.Header, destination []byte) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	operation := IOOperation{Kind: IOReplyRead, Offset: uint64(slot), Buffer: destination, ExpectedHeader: expected}
	var sequence ioSequence
	if err := store.beginIO(&sequence, &operation); err != nil {
		return nil, err
	}
	defer func() { store.ioActive = false }()
	if err := store.advanceIO(&sequence, &operation); err != nil {
		return nil, err
	}
	return destination[:operation.Size:operation.Size], nil
}

func (store *ReplyStore) beginIO(sequence *ioSequence, operation *IOOperation) error {
	if store.ioActive {
		return ErrIOBackpressure
	}
	if operation.Offset >= uint64(store.clientsMax) {
		return ErrInvalidReplyStore
	}
	slot := uint32(operation.Offset)
	offset, ok := store.slotOffset(slot)
	if !ok {
		return ErrInvalidReplyStore
	}
	*sequence = ioSequence{storage: store.storage, offset: offset, size: store.layout.ReplyStride}
	if operation.Kind == IOReplyRead {
		operation.Size = 0
		if uint64(len(operation.Buffer)) < store.layout.ReplyStride {
			return ErrInvalidReplyStore
		}
		sequence.phase, sequence.buffer = IOPhaseRead, operation.Buffer[:store.layout.ReplyStride]
		store.ioActive = true
		return nil
	}
	frame := operation.Buffer
	if uint64(len(frame)) > store.layout.ReplyStride {
		return ErrInvalidReplyStore
	}
	header, body, reason := protocol.DecodeFrame(frame, store.group, uint32(store.layout.ReplyStride), store.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandReply || protocol.ChecksumBytes(body) != header.BodyChecksum {
		return ErrInvalidReplyStore
	}
	clear(store.buffer)
	copy(store.buffer, frame)
	sequence.phase, sequence.buffer = IOPhaseWrite, store.buffer
	store.ioActive = true
	return nil
}

func (store *ReplyStore) advanceIO(sequence *ioSequence, operation *IOOperation) error {
	if err := sequence.physical(); err != nil {
		return fmt.Errorf("%w: reply slot %d phase %d: %w", ErrStorage, operation.Offset, sequence.phase, err)
	}
	if operation.Kind == IOReplyWrite && sequence.phase == IOPhaseWrite {
		sequence.syncNext()
		return nil
	}
	sequence.phase = IOPhaseCompletion
	if operation.Kind == IOReplyWrite {
		return nil
	}
	physical := sequence.buffer
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || uint64(size) > store.layout.ReplyStride {
		return ErrInvalidReplyStore
	}
	frame := physical[:size:size]
	header, _, reason := protocol.DecodeFrame(frame, store.group, uint32(store.layout.ReplyStride), store.memberCount)
	expected := &operation.ExpectedHeader
	if reason != protocol.RejectNone || header.Command != protocol.CommandReply || header.HeaderChecksum != expected.HeaderChecksum || replyClient(&header) != replyClient(expected) || replyOp(&header) != replyOp(expected) {
		return ErrInvalidReplyStore
	}
	operation.Size = uint64(size)
	return nil
}

func (store *ReplyStore) slotOffset(slot uint32) (uint64, bool) {
	offset, ok := checkedMul(uint64(slot), store.layout.ReplyStride)
	if ok {
		offset, ok = checkedAdd(store.layout.ReplyBase, offset)
	}
	return offset, ok
}
