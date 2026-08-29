package replication

import (
	"bytes"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestReplyStoreSerializesOverlappingWorkerWrites(t *testing.T) {
	config := compactTestClusterConfig()
	layout, ok := DeriveWALLayout(config)
	if !ok {
		t.Fatal("invalid layout")
	}
	storage := &overlapStorage{bytes: make([]byte, layout.BlockBase)}
	store, err := NewReplyStore(storage, config, protocol.GroupID{9}, 3)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewIOEngine(storage, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer closeIOEngine(t, engine)

	firstFrame, firstHeader := makeReplyFrame(t, protocol.ClientID{1}, 5, []byte("first reply"))
	secondFrame, secondHeader := makeReplyFrame(t, protocol.ClientID{2}, 6, []byte("second reply"))
	for slot, frame := range [][]byte{firstFrame, secondFrame} {
		if _, err := engine.Submit(IOOperation{Kind: IOReplyWrite, Offset: uint64(slot), Buffer: frame, ReplyStore: store}); err != nil {
			t.Fatal(err)
		}
	}
	for _, completion := range collectIOCompletions(t, engine, 2) {
		if completion.Err != nil {
			t.Fatal(completion.Err)
		}
	}
	if storage.maximum.Load() != 1 {
		t.Fatalf("concurrent reply store writes = %d, want 1", storage.maximum.Load())
	}

	for slot, test := range []struct {
		header protocol.Header
		body   []byte
	}{{firstHeader, []byte("first reply")}, {secondHeader, []byte("second reply")}} {
		buffer, err := NewAlignedBuffer(layout.ReplyStride, SectorSize)
		if err != nil {
			t.Fatal(err)
		}
		frame, err := store.Read(uint32(slot), test.header, buffer)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(frame[protocol.HeaderSize:], test.body) {
			t.Fatalf("slot %d body = %q, want %q", slot, frame[protocol.HeaderSize:], test.body)
		}
	}
}

func makeReplyFrame(t testing.TB, client protocol.ClientID, op protocol.Op, body []byte) ([]byte, protocol.Header) {
	t.Helper()
	frame := make([]byte, protocol.HeaderSize+len(body))
	copy(frame[protocol.HeaderSize:], body)
	header := protocol.Header{
		Group:    protocol.GroupID{9},
		View:     0,
		Release:  1,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandReply,
		Author:   0,
	}
	header.Fields[0] = 1
	copy(header.Fields[64:80], client[:])
	putUint64(header.Fields[80:88], uint64(op))
	putUint64(header.Fields[88:96], uint64(op))
	putUint64(header.Fields[96:104], uint64(op))
	putUint32(header.Fields[104:108], 1)
	header.Fields[108] = byte(protocol.OperationNoop)
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	context := protocol.ChecksumBytes(frame[protocol.HeaderChecksumFrom:protocol.HeaderSize])
	copy(header.Fields[32:48], context[:])
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame, header
}

type overlapStorage struct {
	mu      sync.Mutex
	bytes   []byte
	active  atomic.Int32
	maximum atomic.Int32
}

func (storage *overlapStorage) ReadAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	copy(buffer, storage.bytes[offset:offset+uint64(len(buffer))])
	return nil
}

func (storage *overlapStorage) WriteAt(buffer []byte, offset uint64) error {
	active := storage.active.Add(1)
	for maximum := storage.maximum.Load(); active > maximum && !storage.maximum.CompareAndSwap(maximum, active); maximum = storage.maximum.Load() {
	}
	storage.mu.Lock()
	copy(storage.bytes[offset:offset+uint64(len(buffer))], buffer)
	storage.mu.Unlock()
	storage.active.Add(-1)
	return nil
}

func (storage *overlapStorage) Sync() error           { return nil }
func (storage *overlapStorage) Resize(_ uint64) error { return nil }
func (storage *overlapStorage) Size() (uint64, error) { return uint64(len(storage.bytes)), nil }
func (storage *overlapStorage) SyncParent() error     { return nil }
func (storage *overlapStorage) Close() error          { return nil }
