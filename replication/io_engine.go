package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrIOBackpressure = errors.New("replication: storage request pool exhausted")
	ErrIOCanceled     = errors.New("replication: storage request canceled before effect")
	ErrIOHandle       = errors.New("replication: invalid storage request handle")
)

type IOKind uint8

const (
	IORead IOKind = iota + 1
	IOWrite
	IOSync
	IOResize
	IOWALAppend
	IOReplyWrite
	IOReplyRead
	IOSuperblockPersist
)

type IOHandle struct {
	Index      uint32
	Generation uint64
}

type IOOperation struct {
	Kind            IOKind
	Offset          uint64
	Buffer          []byte
	Size            uint64
	Context         uint64
	WAL             *WAL
	ReplyStore      *ReplyStore
	ExpectedHeader  protocol.Header
	SuperblockStore *SuperblockStore
	Superblock      Superblock
	ReusableThrough protocol.Op
}

type IOCompletion struct {
	Handle  IOHandle
	Kind    IOKind
	Offset  uint64
	Buffer  []byte
	Size    uint64
	Context uint64
	Err     error
}

const (
	ioSlotFree uint32 = iota
	ioSlotQueued
	ioSlotRunning
	ioSlotCanceled
	ioSlotComplete
)

type ioSlot struct {
	state      atomic.Uint32
	generation atomic.Uint64
	operation  IOOperation
	err        error
}

type IOEngine struct {
	storage   Storage
	work      chan uint32
	done      chan struct{}
	notify    chan struct{}
	closed    atomic.Bool
	lifecycle sync.Mutex

	workers sync.WaitGroup
	slots   []ioSlot
	free    []uint32
	freeLen int
	ready   *MPSCRing[uint32]
}

func NewIOEngine(storage Storage, requestCount, workerCount uint32) (*IOEngine, error) {
	if storage == nil || requestCount == 0 || workerCount == 0 || workerCount > requestCount {
		return nil, ErrInvalidConfiguration
	}
	ringCapacity := uint64(2)
	for ringCapacity < uint64(requestCount) {
		ringCapacity <<= 1
	}
	ready, err := NewMPSCRing[uint32](ringCapacity)
	if err != nil {
		return nil, err
	}
	engine := &IOEngine{
		storage: storage,
		work:    make(chan uint32, requestCount),
		done:    make(chan struct{}),
		notify:  make(chan struct{}, 1),
		slots:   make([]ioSlot, int(requestCount)),
		free:    make([]uint32, int(requestCount)),
		freeLen: int(requestCount),
		ready:   ready,
	}
	for index := range engine.free {
		engine.free[index] = uint32(len(engine.free) - index - 1)
	}
	engine.workers.Add(int(workerCount))
	for range workerCount {
		go engine.runWorker()
	}
	go func() {
		engine.workers.Wait()
		close(engine.done)
	}()
	return engine, nil
}

// Submit is single-consumer owned by the replica event loop.
func (engine *IOEngine) Submit(operation IOOperation) (IOHandle, error) {
	engine.lifecycle.Lock()
	defer engine.lifecycle.Unlock()
	if engine.closed.Load() {
		return IOHandle{}, ErrStorageClosed
	}
	if engine.freeLen == 0 {
		return IOHandle{}, ErrIOBackpressure
	}
	if operation.Kind < IORead || operation.Kind > IOSuperblockPersist {
		return IOHandle{}, ErrIOHandle
	}
	if (operation.Kind == IORead || operation.Kind == IOWrite) && len(operation.Buffer) == 0 {
		return IOHandle{}, ErrIOHandle
	}
	if operation.Kind == IOWALAppend && (operation.WAL == nil || len(operation.Buffer) == 0) {
		return IOHandle{}, ErrIOHandle
	}
	replyOperation := operation.Kind == IOReplyWrite || operation.Kind == IOReplyRead
	if replyOperation {
		if operation.ReplyStore == nil || len(operation.Buffer) == 0 {
			return IOHandle{}, ErrIOHandle
		}
	}
	if operation.Kind == IOSuperblockPersist && operation.SuperblockStore == nil {
		return IOHandle{}, ErrIOHandle
	}
	engine.freeLen--
	index := engine.free[engine.freeLen]
	slot := &engine.slots[index]
	generation := slot.generation.Add(1)
	slot.operation = operation
	slot.err = nil
	slot.state.Store(ioSlotQueued)
	handle := IOHandle{Index: index, Generation: generation}
	engine.work <- index
	return handle, nil
}

func (engine *IOEngine) Cancel(handle IOHandle) bool {
	if int(handle.Index) >= len(engine.slots) {
		return false
	}
	slot := &engine.slots[handle.Index]
	if slot.generation.Load() != handle.Generation {
		return false
	}
	return slot.state.CompareAndSwap(ioSlotQueued, ioSlotCanceled)
}

// Poll is single-consumer owned by the replica event loop.
func (engine *IOEngine) Poll(completion *IOCompletion) bool {
	var index uint32
	if !engine.ready.TryPop(&index) {
		return false
	}
	slot := &engine.slots[index]
	if slot.state.Load() != ioSlotComplete {
		panic("replication: incomplete storage request reached event loop")
	}
	*completion = IOCompletion{
		Handle: IOHandle{
			Index:      index,
			Generation: slot.generation.Load(),
		},
		Kind:    slot.operation.Kind,
		Offset:  slot.operation.Offset,
		Buffer:  slot.operation.Buffer,
		Size:    slot.operation.Size,
		Context: slot.operation.Context,
		Err:     slot.err,
	}
	slot.operation = IOOperation{}
	slot.err = nil
	slot.state.Store(ioSlotFree)
	engine.free[engine.freeLen] = index
	engine.freeLen++
	return true
}

func (engine *IOEngine) Available() int {
	return engine.freeLen
}

func (engine *IOEngine) Ready() <-chan struct{} {
	return engine.notify
}

func (engine *IOEngine) Close(ctx context.Context) error {
	engine.lifecycle.Lock()
	if !engine.closed.Swap(true) {
		close(engine.work)
	}
	engine.lifecycle.Unlock()
	select {
	case <-engine.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (engine *IOEngine) runWorker() {
	defer engine.workers.Done()
	for index := range engine.work {
		slot := &engine.slots[index]
		if slot.state.CompareAndSwap(ioSlotCanceled, ioSlotRunning) {
			slot.err = ErrIOCanceled
		} else if slot.state.CompareAndSwap(ioSlotQueued, ioSlotRunning) {
			slot.err = engine.execute(&slot.operation)
		} else {
			panic("replication: storage request entered worker twice")
		}
		slot.state.Store(ioSlotComplete)
		if !engine.ready.TryPush(index) {
			panic("replication: storage completion ring exhausted")
		}
		select {
		case engine.notify <- struct{}{}:
		default:
		}
	}
}

func (engine *IOEngine) execute(operation *IOOperation) error {
	switch operation.Kind {
	case IORead:
		return engine.storage.ReadAt(operation.Buffer, operation.Offset)
	case IOWrite:
		return engine.storage.WriteAt(operation.Buffer, operation.Offset)
	case IOSync:
		return engine.storage.Sync()
	case IOResize:
		return engine.storage.Resize(operation.Size)
	case IOWALAppend:
		return operation.WAL.Append(operation.Buffer, operation.ReusableThrough)
	case IOReplyWrite:
		return operation.ReplyStore.Write(uint32(operation.Offset), operation.Buffer)
	case IOReplyRead:
		frame, err := operation.ReplyStore.Read(uint32(operation.Offset), operation.ExpectedHeader, operation.Buffer)
		operation.Size = uint64(len(frame))
		return err
	case IOSuperblockPersist:
		return operation.SuperblockStore.Persist(operation.Superblock)
	default:
		return ErrIOHandle
	}
}
