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
	if slot >= store.clientsMax || uint64(len(frame)) > store.layout.ReplyStride {
		return ErrInvalidReplyStore
	}
	header, body, reason := protocol.DecodeFrame(frame, store.group, uint32(store.layout.ReplyStride), store.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandReply || protocol.ChecksumBytes(body) != header.BodyChecksum {
		return ErrInvalidReplyStore
	}
	clear(store.buffer)
	copy(store.buffer, frame)
	offset, ok := store.slotOffset(slot)
	if !ok {
		return ErrInvalidReplyStore
	}
	if err := store.storage.WriteAt(store.buffer, offset); err != nil {
		return fmt.Errorf("%w: reply slot %d: %w", ErrStorage, slot, err)
	}
	if err := store.storage.Sync(); err != nil {
		return fmt.Errorf("%w: durable reply slot %d: %w", ErrStorage, slot, err)
	}
	return nil
}

func (store *ReplyStore) Read(slot uint32, expected protocol.Header, destination []byte) ([]byte, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if slot >= store.clientsMax || uint64(len(destination)) < store.layout.ReplyStride {
		return nil, ErrInvalidReplyStore
	}
	offset, ok := store.slotOffset(slot)
	if !ok {
		return nil, ErrInvalidReplyStore
	}
	physical := destination[:store.layout.ReplyStride]
	if err := store.storage.ReadAt(physical, offset); err != nil {
		return nil, fmt.Errorf("%w: reply slot %d: %w", ErrStorage, slot, err)
	}
	size := binary.LittleEndian.Uint32(physical[96:100])
	if size < protocol.HeaderSize || uint64(size) > store.layout.ReplyStride {
		return nil, ErrInvalidReplyStore
	}
	frame := physical[:size:size]
	header, _, reason := protocol.DecodeFrame(frame, store.group, uint32(store.layout.ReplyStride), store.memberCount)
	if reason != protocol.RejectNone || header.Command != protocol.CommandReply || header.HeaderChecksum != expected.HeaderChecksum || replyClient(&header) != replyClient(&expected) || replyOp(&header) != replyOp(&expected) {
		return nil, ErrInvalidReplyStore
	}
	return frame, nil
}

func (store *ReplyStore) slotOffset(slot uint32) (uint64, bool) {
	offset, ok := checkedMul(uint64(slot), store.layout.ReplyStride)
	if ok {
		offset, ok = checkedAdd(store.layout.ReplyBase, offset)
	}
	return offset, ok
}
