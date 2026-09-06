package replication

import (
	"errors"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

type IOPhase uint8

const (
	IOPhaseQueued IOPhase = iota + 1
	IOPhaseRead
	IOPhaseWrite
	IOPhaseSync
	IOPhaseResize
	IOPhaseCompletion
)

type IOEvent struct {
	Handle IOHandle
	Kind   IOKind
	Phase  IOPhase
	Offset uint64
	Size   uint64
}

// IOController is owned by the replica event-loop thread. A controller belongs
// to one engine incarnation, including after that engine closes.
// Buffers remain borrowed until the engine's Poll returns their completion.
type IOController struct {
	engine *IOEngine
}

func NewIOController() *IOController { return &IOController{} }

func (controller *IOController) Capacity() int {
	if controller.engine == nil {
		return 0
	}
	return len(controller.engine.slots)
}

func (controller *IOController) Used() int {
	if controller.engine == nil {
		return 0
	}
	return len(controller.engine.slots) - controller.engine.Available()
}

// Pending reports advanceable events in handle-index order. Requests waiting
// for another operation's store scratch buffer are not yet advanceable.
func (controller *IOController) Pending(dst []IOEvent) int {
	if controller.engine == nil {
		return 0
	}
	n := 0
	for index := range controller.engine.slots {
		slot := &controller.engine.slots[index]
		state := slot.state.Load()
		if state == ioSlotFree || state == ioSlotComplete {
			continue
		}
		if state == ioSlotQueued && controller.storeBusy(slot) {
			continue
		}
		if n == len(dst) {
			break
		}
		phase := slot.sequence.phase
		if state == ioSlotQueued {
			phase = IOPhaseQueued
		}
		if state == ioSlotCanceled {
			phase = IOPhaseCompletion
		}
		dst[n] = IOEvent{Handle: IOHandle{Index: uint32(index), Generation: slot.generation.Load()}, Kind: slot.operation.Kind, Phase: phase, Offset: slot.sequence.offset, Size: slot.sequence.size}
		n++
	}
	return n
}

func (controller *IOController) PendingCount() int {
	if controller.engine == nil {
		return 0
	}
	n := 0
	for index := range controller.engine.slots {
		state := controller.engine.slots[index].state.Load()
		if state != ioSlotFree && state != ioSlotComplete {
			n++
		}
	}
	return n
}

func (controller *IOController) storeBusy(candidate *ioSlot) bool {
	for index := range controller.engine.slots {
		other := &controller.engine.slots[index]
		if other == candidate || other.state.Load() != ioSlotRunning || other.sequence.phase == IOPhaseCompletion {
			continue
		}
		a, b := &candidate.operation, &other.operation
		sameWAL := a.WAL != nil && a.WAL == b.WAL
		sameReplies := a.ReplyStore != nil && a.ReplyStore == b.ReplyStore
		sameSuperblocks := a.SuperblockStore != nil && a.SuperblockStore == b.SuperblockStore
		if sameWAL || sameReplies || sameSuperblocks {
			return true
		}
	}
	return false
}

// Advance performs one start, physical effect, or completion publication.
// Storage errors are delivered through Poll, not returned as scheduler errors.
func (controller *IOController) Advance(handle IOHandle) error {
	engine := controller.engine
	if engine == nil || int(handle.Index) >= len(engine.slots) {
		return ErrIOHandle
	}
	slot := &engine.slots[handle.Index]
	if slot.generation.Load() != handle.Generation {
		return ErrIOHandle
	}
	switch slot.state.Load() {
	case ioSlotQueued:
		if controller.storeBusy(slot) {
			return ErrIOBackpressure
		}
		slot.state.Store(ioSlotRunning)
		slot.err = slot.sequence.begin(engine.storage, &slot.operation)
		if slot.err != nil {
			slot.sequence.phase = IOPhaseCompletion
		}
	case ioSlotCanceled:
		slot.err = ErrIOCanceled
		engine.complete(handle.Index, slot)
	case ioSlotRunning:
		if slot.sequence.phase == IOPhaseCompletion {
			engine.complete(handle.Index, slot)
			return nil
		}
		slot.err = slot.sequence.advance(&slot.operation)
		if slot.err != nil {
			slot.sequence.phase = IOPhaseCompletion
		}
	default:
		return ErrIOHandle
	}
	return nil
}

// Drain publishes all remaining completions; Poll retains ownership of recycling
// slots and returning borrowed buffers to the replica.
func (controller *IOController) Drain() error {
	var events [1]IOEvent
	for controller.PendingCount() != 0 {
		if controller.Pending(events[:]) == 0 {
			return ErrIOBackpressure
		}
		if err := controller.Advance(events[0].Handle); err != nil {
			return err
		}
	}
	return nil
}

// Crash never performs storage I/O. Already published completions remain
// published; all other requests become canceled completion events.
func (controller *IOController) Crash() error {
	if controller.engine == nil {
		return ErrIOHandle
	}
	controller.engine.closed.Store(true)
	for index := range controller.engine.slots {
		slot := &controller.engine.slots[index]
		state := slot.state.Load()
		if state == ioSlotFree || state == ioSlotComplete {
			continue
		}
		if state == ioSlotRunning && slot.sequence.phase != IOPhaseCompletion {
			slot.sequence.abandon(&slot.operation)
		}
		slot.err = ErrIOCanceled
		slot.sequence.phase = IOPhaseCompletion
		slot.state.Store(ioSlotCanceled)
	}
	return nil
}

// ioSequence is embedded in bounded engine slots. Direct store operations run
// the same transitions under their existing whole-operation mutex.
type ioSequence struct {
	phase      IOPhase
	storage    Storage
	buffer     []byte
	offset     uint64
	size       uint64
	step       uint8
	index      uint64
	generation uint64
	checksum   protocol.Checksum
}

func (sequence *ioSequence) begin(storage Storage, operation *IOOperation) error {
	*sequence = ioSequence{storage: storage, buffer: operation.Buffer, offset: operation.Offset, size: uint64(len(operation.Buffer))}
	switch operation.Kind {
	case IORead:
		sequence.phase = IOPhaseRead
	case IOWrite:
		sequence.phase = IOPhaseWrite
	case IOSync:
		sequence.phase = IOPhaseSync
	case IOResize:
		sequence.phase, sequence.size = IOPhaseResize, operation.Size
	case IOWALAppend:
		operation.WAL.mu.Lock()
		defer operation.WAL.mu.Unlock()
		return operation.WAL.beginAppend(sequence, operation.Buffer, operation.ReusableThrough)
	case IOReplyWrite, IOReplyRead:
		operation.ReplyStore.mu.Lock()
		defer operation.ReplyStore.mu.Unlock()
		return operation.ReplyStore.beginIO(sequence, operation)
	case IOSuperblockPersist:
		operation.SuperblockStore.mu.Lock()
		defer operation.SuperblockStore.mu.Unlock()
		return operation.SuperblockStore.beginPersist(sequence, &operation.Superblock)
	default:
		return ErrIOHandle
	}
	return nil
}

func (sequence *ioSequence) physical() error {
	switch sequence.phase {
	case IOPhaseRead:
		return sequence.storage.ReadAt(sequence.buffer, sequence.offset)
	case IOPhaseWrite:
		return sequence.storage.WriteAt(sequence.buffer, sequence.offset)
	case IOPhaseSync:
		return sequence.storage.Sync()
	case IOPhaseResize:
		return sequence.storage.Resize(sequence.size)
	default:
		return ErrIOHandle
	}
}

func (sequence *ioSequence) advance(operation *IOOperation) error {
	switch operation.Kind {
	case IOWALAppend:
		operation.WAL.mu.Lock()
		defer operation.WAL.mu.Unlock()
		err := operation.WAL.advanceAppend(sequence, operation.Buffer)
		if err != nil || sequence.phase == IOPhaseCompletion {
			operation.WAL.ioActive = false
		}
		return err
	case IOReplyRead, IOReplyWrite:
		operation.ReplyStore.mu.Lock()
		defer operation.ReplyStore.mu.Unlock()
		err := operation.ReplyStore.advanceIO(sequence, operation)
		if err != nil || sequence.phase == IOPhaseCompletion {
			operation.ReplyStore.ioActive = false
		}
		return err
	case IOSuperblockPersist:
		operation.SuperblockStore.mu.Lock()
		defer operation.SuperblockStore.mu.Unlock()
		err := operation.SuperblockStore.advancePersist(sequence, &operation.Superblock)
		if err != nil || sequence.phase == IOPhaseCompletion {
			operation.SuperblockStore.ioActive = false
		}
		return err
	default:
		err := sequence.physical()
		sequence.phase = IOPhaseCompletion
		return err
	}
}

func (sequence *ioSequence) abandon(operation *IOOperation) {
	switch operation.Kind {
	case IOWALAppend:
		operation.WAL.mu.Lock()
		operation.WAL.ioActive = false
		operation.WAL.mu.Unlock()
	case IOReplyRead, IOReplyWrite:
		operation.ReplyStore.mu.Lock()
		operation.ReplyStore.ioActive = false
		operation.ReplyStore.mu.Unlock()
	case IOSuperblockPersist:
		operation.SuperblockStore.mu.Lock()
		operation.SuperblockStore.ioActive = false
		operation.SuperblockStore.mu.Unlock()
	}
}

func (sequence *ioSequence) syncNext() {
	sequence.phase = IOPhaseSync
	sequence.buffer = nil
	sequence.size = 0
}

var errIOControllerBound = errors.New("replication: I/O controller already bound")
