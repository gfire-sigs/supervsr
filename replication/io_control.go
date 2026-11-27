package replication

import (
	"z.gfire.dev/supervsr/replication/internal/simhook"
	"z.gfire.dev/supervsr/replication/protocol"
)

const (
	ioPhaseQueued     = simhook.IOPhaseQueued
	ioPhaseRead       = simhook.IOPhaseRead
	ioPhaseWrite      = simhook.IOPhaseWrite
	ioPhaseSync       = simhook.IOPhaseSync
	ioPhaseResize     = simhook.IOPhaseResize
	ioPhaseCompletion = simhook.IOPhaseCompletion
)

var errIOControllerBound = simhook.ErrControllerBound

type ioControllerBackend struct {
	engine *storageIOEngine
}

func (controller *ioControllerBackend) Capacity() int {
	if controller.engine == nil {
		return 0
	}
	return len(controller.engine.slots)
}

func (controller *ioControllerBackend) Used() int {
	if controller.engine == nil {
		return 0
	}
	return len(controller.engine.slots) - controller.engine.Available()
}

// Pending reports advanceable events in handle-index order. Requests waiting
// for another operation's store scratch buffer are not yet advanceable.
func (controller *ioControllerBackend) Pending(dst []ioEvent) int {
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
			phase = ioPhaseQueued
		}
		if state == ioSlotCanceled {
			phase = ioPhaseCompletion
		}
		dst[n] = ioEvent{Handle: ioHandle{Index: uint32(index), Generation: slot.generation.Load()}, Kind: slot.operation.Kind, Phase: phase, Offset: slot.sequence.offset, Size: slot.sequence.size}
		n++
	}
	return n
}

func (controller *ioControllerBackend) PendingCount() int {
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

func (controller *ioControllerBackend) storeBusy(candidate *ioSlot) bool {
	for index := range controller.engine.slots {
		other := &controller.engine.slots[index]
		if other == candidate || other.state.Load() != ioSlotRunning || other.sequence.phase == ioPhaseCompletion {
			continue
		}
		a, b := &candidate.operation, &other.operation
		sameWAL := a.walJournal != nil && a.walJournal == b.walJournal
		sameReplies := a.durableReplyStore != nil && a.durableReplyStore == b.durableReplyStore
		sameSuperblocks := a.superblockStore != nil && a.superblockStore == b.superblockStore
		if sameWAL || sameReplies || sameSuperblocks {
			return true
		}
	}
	return false
}

// Advance performs one start, physical effect, or completion publication.
// Storage errors are delivered through Poll, not returned as scheduler errors.
func (controller *ioControllerBackend) Advance(handle ioHandle) error {
	engine := controller.engine
	if engine == nil || int(handle.Index) >= len(engine.slots) {
		return errIOHandle
	}
	slot := &engine.slots[handle.Index]
	if slot.generation.Load() != handle.Generation {
		return errIOHandle
	}
	switch slot.state.Load() {
	case ioSlotQueued:
		if controller.storeBusy(slot) {
			return ErrIOBackpressure
		}
		slot.state.Store(ioSlotRunning)
		slot.err = slot.sequence.begin(engine.storage, &slot.operation)
		if slot.err != nil {
			slot.sequence.phase = ioPhaseCompletion
		}
	case ioSlotCanceled:
		slot.err = errIOCanceled
		engine.complete(handle.Index, slot)
	case ioSlotRunning:
		if slot.sequence.phase == ioPhaseCompletion {
			engine.complete(handle.Index, slot)
			return nil
		}
		slot.err = slot.sequence.advance(&slot.operation)
		if slot.err != nil {
			slot.sequence.phase = ioPhaseCompletion
		}
	default:
		return errIOHandle
	}
	return nil
}

// Drain publishes all remaining completions; Poll retains ownership of recycling
// slots and returning borrowed buffers to the replica.
func (controller *ioControllerBackend) Drain() error {
	var events [1]ioEvent
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
func (controller *ioControllerBackend) Crash() error {
	if controller.engine == nil {
		return errIOHandle
	}
	controller.engine.closed.Store(true)
	for index := range controller.engine.slots {
		slot := &controller.engine.slots[index]
		state := slot.state.Load()
		if state == ioSlotFree || state == ioSlotComplete {
			continue
		}
		if state == ioSlotRunning && slot.sequence.phase != ioPhaseCompletion {
			slot.sequence.abandon(&slot.operation)
		}
		slot.err = errIOCanceled
		slot.sequence.phase = ioPhaseCompletion
		slot.state.Store(ioSlotCanceled)
	}
	return nil
}

// ioSequence is embedded in bounded engine slots. Direct store operations run
// the same transitions under their existing whole-operation mutex.
type ioSequence struct {
	phase      ioPhase
	storage    Storage
	buffer     []byte
	offset     uint64
	size       uint64
	step       uint8
	index      uint64
	generation uint64
	checksum   protocol.Checksum
}

func (sequence *ioSequence) begin(storage Storage, operation *ioOperation) error {
	*sequence = ioSequence{storage: storage, buffer: operation.Buffer, offset: operation.Offset, size: uint64(len(operation.Buffer))}
	switch operation.Kind {
	case ioRead:
		sequence.phase = ioPhaseRead
	case ioWrite:
		sequence.phase = ioPhaseWrite
	case ioSync:
		sequence.phase = ioPhaseSync
	case ioResize:
		sequence.phase, sequence.size = ioPhaseResize, operation.Size
	case ioWALAppend:
		operation.walJournal.mu.Lock()
		defer operation.walJournal.mu.Unlock()
		return operation.walJournal.beginAppend(sequence, operation.Buffer, operation.ReusableThrough)
	case ioReplyWrite, ioReplyRead:
		operation.durableReplyStore.mu.Lock()
		defer operation.durableReplyStore.mu.Unlock()
		return operation.durableReplyStore.beginIO(sequence, operation)
	case ioSuperblockPersist:
		operation.superblockStore.mu.Lock()
		defer operation.superblockStore.mu.Unlock()
		return operation.superblockStore.beginPersist(sequence, &operation.superblockRecord)
	default:
		return errIOHandle
	}
	return nil
}

func (sequence *ioSequence) physical() error {
	switch sequence.phase {
	case ioPhaseRead:
		return sequence.storage.ReadAt(sequence.buffer, sequence.offset)
	case ioPhaseWrite:
		return sequence.storage.WriteAt(sequence.buffer, sequence.offset)
	case ioPhaseSync:
		return sequence.storage.Sync()
	case ioPhaseResize:
		return sequence.storage.Resize(sequence.size)
	default:
		return errIOHandle
	}
}

func (sequence *ioSequence) advance(operation *ioOperation) error {
	switch operation.Kind {
	case ioWALAppend:
		operation.walJournal.mu.Lock()
		defer operation.walJournal.mu.Unlock()
		err := operation.walJournal.advanceAppend(sequence, operation.Buffer)
		if err != nil || sequence.phase == ioPhaseCompletion {
			operation.walJournal.ioActive = false
		}
		return err
	case ioReplyRead, ioReplyWrite:
		operation.durableReplyStore.mu.Lock()
		defer operation.durableReplyStore.mu.Unlock()
		err := operation.durableReplyStore.advanceIO(sequence, operation)
		if err != nil || sequence.phase == ioPhaseCompletion {
			operation.durableReplyStore.ioActive = false
		}
		return err
	case ioSuperblockPersist:
		operation.superblockStore.mu.Lock()
		defer operation.superblockStore.mu.Unlock()
		err := operation.superblockStore.advancePersist(sequence, &operation.superblockRecord)
		if err != nil || sequence.phase == ioPhaseCompletion {
			operation.superblockStore.ioActive = false
		}
		return err
	default:
		err := sequence.physical()
		sequence.phase = ioPhaseCompletion
		return err
	}
}

func (sequence *ioSequence) abandon(operation *ioOperation) {
	switch operation.Kind {
	case ioWALAppend:
		operation.walJournal.mu.Lock()
		operation.walJournal.ioActive = false
		operation.walJournal.mu.Unlock()
	case ioReplyRead, ioReplyWrite:
		operation.durableReplyStore.mu.Lock()
		operation.durableReplyStore.ioActive = false
		operation.durableReplyStore.mu.Unlock()
	case ioSuperblockPersist:
		operation.superblockStore.mu.Lock()
		operation.superblockStore.ioActive = false
		operation.superblockStore.mu.Unlock()
	}
}

func (sequence *ioSequence) syncNext() {
	sequence.phase = ioPhaseSync
	sequence.buffer = nil
	sequence.size = 0
}
