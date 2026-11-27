package replication

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
)

var (
	ErrIOBackpressure = errors.New("replication: storage request pool exhausted")
	errIOCanceled     = errors.New("replication: storage request canceled")
	errIOHandle       = simhook.ErrIOHandle
)

type (
	ioKind       = simhook.IOKind
	ioHandle     = simhook.IOHandle
	ioPhase      = simhook.IOPhase
	ioEvent      = simhook.IOEvent
	ioController = simhook.IOController
)

const (
	ioRead              = simhook.IORead
	ioWrite             = simhook.IOWrite
	ioSync              = simhook.IOSync
	ioResize            = simhook.IOResize
	ioWALAppend         = simhook.IOWALAppend
	ioReplyWrite        = simhook.IOReplyWrite
	ioReplyRead         = simhook.IOReplyRead
	ioSuperblockPersist = simhook.IOSuperblockPersist
)

type ioOperation struct {
	Kind              ioKind
	Offset            uint64
	Buffer            []byte
	Size              uint64
	Context           uint64
	walJournal        *walJournal
	durableReplyStore *durableReplyStore
	ExpectedHeader    protocol.Header
	superblockStore   *superblockStore
	superblockRecord  superblockRecord
	ReusableThrough   protocol.Op
}

type ioCompletion struct {
	Handle  ioHandle
	Kind    ioKind
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
	operation  ioOperation
	err        error
	sequence   ioSequence
}

type storageIOEngine struct {
	storage   Storage
	work      chan uint32
	done      chan struct{}
	notify    chan struct{}
	closed    atomic.Bool
	faulted   atomic.Bool
	lifecycle sync.Mutex

	workers     sync.WaitGroup
	slots       []ioSlot
	free        []uint32
	freeLen     int
	ready       *mpscRing[uint32]
	synchronous bool
	controller  *ioController
}

func newDefaultIOEngine(storage Storage, requestCount, workerCount uint32) (*storageIOEngine, error) {
	return newIOEngine(storage, requestCount, workerCount, false, nil)
}

func newIOEngine(storage Storage, requestCount, workerCount uint32, synchronous bool, controller *ioController) (*storageIOEngine, error) {
	if storage == nil || requestCount == 0 {
		return nil, ErrInvalidConfiguration
	}
	if controller != nil {
		if synchronous {
			return nil, ErrInvalidConfiguration
		}
		if controller.Bound() {
			return nil, errIOControllerBound
		}
	}
	workersInvalid := workerCount == 0 || workerCount > requestCount
	if !synchronous && controller == nil && workersInvalid {
		return nil, ErrInvalidConfiguration
	}
	ringCapacity := uint64(2)
	for ringCapacity < uint64(requestCount) {
		ringCapacity <<= 1
	}
	ready, err := newMPSCRing[uint32](ringCapacity)
	if err != nil {
		return nil, err
	}
	engine := &storageIOEngine{
		storage: storage, synchronous: synchronous,
		work: make(chan uint32, requestCount), done: make(chan struct{}), notify: make(chan struct{}, 1),
		slots: make([]ioSlot, int(requestCount)), free: make([]uint32, int(requestCount)),
		freeLen: int(requestCount), ready: ready,
		controller: controller,
	}
	for index := range engine.free {
		engine.free[index] = uint32(len(engine.free) - index - 1)
	}
	if controller != nil {
		if err := controller.Bind(&ioControllerBackend{engine: engine}); err != nil {
			return nil, err
		}
	}
	if synchronous || controller != nil {
		close(engine.done)
		return engine, nil
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
func (engine *storageIOEngine) Submit(operation ioOperation) (ioHandle, error) {
	engine.lifecycle.Lock()
	defer engine.lifecycle.Unlock()
	if engine.faulted.Load() {
		return ioHandle{}, ErrReplicaInvariant
	}
	if engine.closed.Load() {
		return ioHandle{}, ErrStorageClosed
	}
	if engine.freeLen == 0 {
		return ioHandle{}, ErrIOBackpressure
	}
	if operation.Kind < ioRead || operation.Kind > ioSuperblockPersist {
		return ioHandle{}, errIOHandle
	}
	if (operation.Kind == ioRead || operation.Kind == ioWrite) && len(operation.Buffer) == 0 {
		return ioHandle{}, errIOHandle
	}
	if operation.Kind == ioWALAppend && (operation.walJournal == nil || len(operation.Buffer) == 0) {
		return ioHandle{}, errIOHandle
	}
	replyOperation := operation.Kind == ioReplyWrite || operation.Kind == ioReplyRead
	if replyOperation {
		if operation.durableReplyStore == nil || len(operation.Buffer) == 0 {
			return ioHandle{}, errIOHandle
		}
	}
	if operation.Kind == ioSuperblockPersist && operation.superblockStore == nil {
		return ioHandle{}, errIOHandle
	}
	engine.freeLen--
	index := engine.free[engine.freeLen]
	slot := &engine.slots[index]
	generation := slot.generation.Add(1)
	slot.operation = operation
	slot.err = nil
	slot.sequence = ioSequence{}
	slot.state.Store(ioSlotQueued)
	handle := ioHandle{Index: index, Generation: generation}
	if engine.synchronous {
		slot.state.Store(ioSlotRunning)
		slot.err = engine.execute(&slot.operation)
		engine.complete(index, slot)
	} else if engine.controller == nil {
		engine.work <- index
	}
	return handle, nil
}

func (engine *storageIOEngine) Cancel(handle ioHandle) bool {
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
func (engine *storageIOEngine) Poll(completion *ioCompletion) (bool, error) {
	if engine.faulted.Load() {
		return false, ErrReplicaInvariant
	}
	var index uint32
	if !engine.ready.TryPop(&index) {
		return false, nil
	}
	slot := &engine.slots[index]
	if slot.state.Load() != ioSlotComplete {
		engine.faulted.Store(true)
		return false, ErrReplicaInvariant
	}
	*completion = ioCompletion{
		Handle: ioHandle{
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
	slot.operation = ioOperation{}
	slot.err = nil
	slot.sequence = ioSequence{}
	slot.state.Store(ioSlotFree)
	engine.free[engine.freeLen] = index
	engine.freeLen++
	return true, nil
}

func (engine *storageIOEngine) Available() int {
	return engine.freeLen
}

func (engine *storageIOEngine) Drained() bool {
	return engine.freeLen == len(engine.free)
}

func (engine *storageIOEngine) Ready() <-chan struct{} {
	return engine.notify
}

func (engine *storageIOEngine) Close(ctx context.Context) error {
	engine.lifecycle.Lock()
	if !engine.closed.Swap(true) {
		close(engine.work)
	}
	engine.lifecycle.Unlock()
	if engine.controller != nil {
		if err := engine.controller.Drain(); err != nil {
			return err
		}
	}
	select {
	case <-engine.done:
		if engine.faulted.Load() {
			return ErrReplicaInvariant
		}
		return nil
	default:
	}
	select {
	case <-engine.done:
		if engine.faulted.Load() {
			return ErrReplicaInvariant
		}
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

func (engine *storageIOEngine) runWorker() {
	defer engine.workers.Done()
	for index := range engine.work {
		slot := &engine.slots[index]
		if slot.state.CompareAndSwap(ioSlotCanceled, ioSlotRunning) {
			slot.err = errIOCanceled
		} else if slot.state.CompareAndSwap(ioSlotQueued, ioSlotRunning) {
			slot.err = engine.execute(&slot.operation)
		} else {
			engine.recordInvariantFault()
			continue
		}
		engine.complete(index, slot)
	}
}

func (engine *storageIOEngine) complete(index uint32, slot *ioSlot) {
	slot.state.Store(ioSlotComplete)
	if !engine.ready.TryPush(index) {
		engine.recordInvariantFault()
		return
	}
	select {
	case engine.notify <- struct{}{}:
	default:
	}
}

func (engine *storageIOEngine) recordInvariantFault() {
	engine.faulted.Store(true)
	select {
	case engine.notify <- struct{}{}:
	default:
	}
}

func (engine *storageIOEngine) execute(operation *ioOperation) error {
	switch operation.Kind {
	case ioRead:
		return engine.storage.ReadAt(operation.Buffer, operation.Offset)
	case ioWrite:
		return engine.storage.WriteAt(operation.Buffer, operation.Offset)
	case ioSync:
		return engine.storage.Sync()
	case ioResize:
		return engine.storage.Resize(operation.Size)
	case ioWALAppend:
		return operation.walJournal.Append(operation.Buffer, operation.ReusableThrough)
	case ioReplyWrite:
		return operation.durableReplyStore.Write(uint32(operation.Offset), operation.Buffer)
	case ioReplyRead:
		frame, err := operation.durableReplyStore.Read(uint32(operation.Offset), operation.ExpectedHeader, operation.Buffer)
		operation.Size = uint64(len(frame))
		return err
	case ioSuperblockPersist:
		return operation.superblockStore.Persist(operation.superblockRecord)
	default:
		return errIOHandle
	}
}
