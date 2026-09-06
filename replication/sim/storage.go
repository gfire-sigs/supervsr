package sim

import (
	"errors"
	"sync"
	"syscall"

	"github.com/gfire-sigs/supervsr/replication"
)

var ErrInjectedFault = errors.New("simulation: injected fault")

const DelayedWritesMax = 64

type FaultEffect uint8

const (
	FaultFail FaultEffect = iota + 1
	FaultTornWrite
	FaultTornSync
	FaultLostWrite
	FaultLostSync
	FaultTornRead
	FaultStaleRead
	FaultCorruptRead
	FaultCorruptWrite
	FaultMisdirectedRead
	FaultMisdirectedWrite
	FaultDelayedWrite
)

type StorageFault struct {
	At uint64
	// Kind zero targets exactly At; otherwise target the first matching operation at or after At.
	Kind   replication.IOKind
	Effect FaultEffect
	Prefix uint64
	Target uint64
	Mask   byte
}

type delayedWrite struct {
	data   []byte
	offset uint64
}

type Storage struct {
	mu            sync.Mutex
	working       []byte
	durable       []byte
	operation     uint64
	fault         StorageFault
	delayed       [DelayedWritesMax]delayedWrite
	delayedCount  int
	capacityLimit uint64
}

func NewStorage() *Storage {
	return &Storage{}
}

func (storage *Storage) Arm(fault StorageFault) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.fault.Effect != 0 || fault.At <= storage.operation || fault.Effect < FaultFail || fault.Effect > FaultDelayedWrite || fault.Kind > replication.IOResize {
		return replication.ErrInvalidConfiguration
	}
	storage.fault = fault
	return nil
}

func (storage *Storage) ClearFault() {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.fault = StorageFault{}
}

func (storage *Storage) NextOperation() uint64 {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return storage.operation + 1
}

func (storage *Storage) FaultPending() bool {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return storage.fault.Effect != 0
}

// SetCapacityLimit bounds file growth, not overwrites in already allocated zones. Zero removes the limit.
func (storage *Storage) SetCapacityLimit(bytes uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if bytes != 0 && bytes < uint64(len(storage.working)) {
		return replication.ErrInvalidConfiguration
	}
	storage.capacityLimit = bytes
	return nil
}

func (storage *Storage) ReadAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault(replication.IORead)
	if fault.Effect == FaultFail {
		return ErrInjectedFault
	}
	source := storage.working
	if fault.Effect == FaultStaleRead {
		source = storage.durable
	}
	if fault.Effect == FaultMisdirectedRead {
		offset = fault.Target
	}
	end := offset + uint64(len(buffer))
	if end < offset || end > uint64(len(source)) {
		return replication.ErrShortIO
	}
	switch fault.Effect {
	case 0, FaultStaleRead, FaultMisdirectedRead:
		copy(buffer, source[offset:end])
		return nil
	case FaultTornRead:
		copied := min(uint64(len(buffer)), fault.Prefix)
		copy(buffer[:copied], source[offset:offset+copied])
		return ErrInjectedFault
	case FaultCorruptRead:
		copy(buffer, source[offset:end])
		if fault.Prefix >= uint64(len(buffer)) {
			return ErrInjectedFault
		}
		buffer[fault.Prefix] ^= faultMask(fault.Mask)
		return nil
	default:
		return ErrInjectedFault
	}
}

func (storage *Storage) WriteAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault(replication.IOWrite)
	if fault.Effect == FaultFail {
		return ErrInjectedFault
	}
	if fault.Effect == FaultMisdirectedWrite {
		offset = fault.Target
	}
	end := offset + uint64(len(buffer))
	if end < offset || end > uint64(len(storage.working)) {
		return replication.ErrShortIO
	}
	switch fault.Effect {
	case 0, FaultMisdirectedWrite:
		copy(storage.working[offset:end], buffer)
		return nil
	case FaultTornWrite:
		written := min(uint64(len(buffer)), fault.Prefix)
		copy(storage.working[offset:offset+written], buffer[:written])
		return ErrInjectedFault
	case FaultLostWrite:
		return nil
	case FaultCorruptWrite:
		copy(storage.working[offset:end], buffer)
		if fault.Prefix >= uint64(len(buffer)) {
			return ErrInjectedFault
		}
		storage.working[offset+fault.Prefix] ^= faultMask(fault.Mask)
		return nil
	case FaultDelayedWrite:
		if storage.delayedCount == len(storage.delayed) {
			return ErrInjectedFault
		}
		storage.delayed[storage.delayedCount] = delayedWrite{data: append([]byte(nil), buffer...), offset: offset}
		storage.delayedCount++
		return nil
	default:
		return ErrInjectedFault
	}
}

func (storage *Storage) Sync() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault(replication.IOSync)
	if fault.Effect == FaultFail {
		return ErrInjectedFault
	}
	switch fault.Effect {
	case 0:
		if len(storage.durable) != len(storage.working) {
			storage.durable = make([]byte, len(storage.working))
		}
		copy(storage.durable, storage.working)
		return nil
	case FaultTornSync:
		if len(storage.durable) != len(storage.working) {
			storage.durable = make([]byte, len(storage.working))
		}
		copied := min(uint64(len(storage.working)), fault.Prefix)
		copy(storage.durable[:copied], storage.working[:copied])
		return ErrInjectedFault
	case FaultLostSync:
		return nil
	default:
		return ErrInjectedFault
	}
}

func (storage *Storage) Resize(size uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.nextFault(replication.IOResize).Effect != 0 {
		return ErrInjectedFault
	}
	if storage.capacityLimit != 0 && size > storage.capacityLimit {
		return errors.Join(ErrInjectedFault, syscall.ENOSPC)
	}
	if size > uint64(int(^uint(0)>>1)) {
		return replication.ErrStorage
	}
	resized := make([]byte, int(size))
	copy(resized, storage.working)
	storage.working = resized
	return nil
}

func (storage *Storage) Size() (uint64, error) {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.nextFault(0).Effect != 0 {
		return 0, ErrInjectedFault
	}
	return uint64(len(storage.working)), nil
}

func (storage *Storage) SyncParent() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.nextFault(0).Effect != 0 {
		return ErrInjectedFault
	}
	return nil
}

func (*Storage) Close() error {
	return nil
}

func (storage *Storage) PendingWrites() int {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return storage.delayedCount
}

func (storage *Storage) ReleaseDelayedWrite(index int) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if index < 0 || index >= storage.delayedCount {
		return replication.ErrInvalidConfiguration
	}
	write := storage.delayed[index]
	end := write.offset + uint64(len(write.data))
	if end < write.offset || end > uint64(len(storage.working)) {
		return replication.ErrShortIO
	}
	copy(storage.working[write.offset:end], write.data)
	copy(storage.delayed[index:], storage.delayed[index+1:storage.delayedCount])
	storage.delayedCount--
	storage.delayed[storage.delayedCount] = delayedWrite{}
	return nil
}

func (storage *Storage) DiscardDelayedWrites() {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	for index := range storage.delayedCount {
		storage.delayed[index] = delayedWrite{}
	}
	storage.delayedCount = 0
}

func (storage *Storage) CorruptWorking(offset uint64, mask byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if offset >= uint64(len(storage.working)) {
		return replication.ErrShortIO
	}
	storage.working[offset] ^= faultMask(mask)
	return nil
}

func (storage *Storage) CorruptDurable(offset uint64, mask byte) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if offset >= uint64(len(storage.durable)) {
		return replication.ErrShortIO
	}
	storage.durable[offset] ^= faultMask(mask)
	return nil
}

func (storage *Storage) Crash() {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.working = append(storage.working[:0], storage.durable...)
	storage.fault = StorageFault{}
	for index := range storage.delayedCount {
		storage.delayed[index] = delayedWrite{}
	}
	storage.delayedCount = 0
}

func (storage *Storage) DurableBytes() []byte {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return append([]byte(nil), storage.durable...)
}

func (storage *Storage) nextFault(kind replication.IOKind) StorageFault {
	storage.operation++
	if storage.fault.At > storage.operation {
		return StorageFault{}
	}
	missedExactOperation := storage.fault.Kind == 0 && storage.fault.At != storage.operation
	mismatchedKind := storage.fault.Kind != 0 && storage.fault.Kind != kind
	if missedExactOperation || mismatchedKind {
		return StorageFault{}
	}
	fault := storage.fault
	storage.fault = StorageFault{}
	return fault
}

func faultMask(mask byte) byte {
	if mask == 0 {
		return 1
	}
	return mask
}
