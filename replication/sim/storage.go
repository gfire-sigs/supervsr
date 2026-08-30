package sim

import (
	"errors"
	"sync"

	"github.com/gfire-sigs/supervsr/replication"
)

var ErrInjectedFault = errors.New("simulation: injected fault")

type FaultEffect uint8

const (
	FaultFail FaultEffect = iota + 1
	FaultTornWrite
	FaultTornSync
)

type StorageFault struct {
	At     uint64
	Effect FaultEffect
	Prefix uint64
}

type Storage struct {
	mu        sync.Mutex
	working   []byte
	durable   []byte
	operation uint64
	fault     StorageFault
}

func NewStorage() *Storage {
	return &Storage{}
}

func (storage *Storage) Arm(fault StorageFault) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if fault.At <= storage.operation || fault.Effect < FaultFail || fault.Effect > FaultTornSync {
		return replication.ErrInvalidConfiguration
	}
	storage.fault = fault
	return nil
}

func (storage *Storage) NextOperation() uint64 {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return storage.operation + 1
}

func (storage *Storage) ReadAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault()
	if fault.Effect != 0 {
		return ErrInjectedFault
	}
	end := offset + uint64(len(buffer))
	if end < offset || end > uint64(len(storage.working)) {
		return replication.ErrShortIO
	}
	copy(buffer, storage.working[offset:end])
	return nil
}

func (storage *Storage) WriteAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault()
	end := offset + uint64(len(buffer))
	if end < offset || end > uint64(len(storage.working)) {
		return replication.ErrShortIO
	}
	if fault.Effect == FaultFail || fault.Effect == FaultTornSync {
		return ErrInjectedFault
	}
	written := uint64(len(buffer))
	if fault.Effect == FaultTornWrite {
		written = min(written, fault.Prefix)
	}
	copy(storage.working[offset:offset+written], buffer[:written])
	if fault.Effect == FaultTornWrite {
		return ErrInjectedFault
	}
	return nil
}

func (storage *Storage) Sync() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	fault := storage.nextFault()
	if fault.Effect == FaultFail || fault.Effect == FaultTornWrite {
		return ErrInjectedFault
	}
	if len(storage.durable) != len(storage.working) {
		storage.durable = make([]byte, len(storage.working))
	}
	copied := uint64(len(storage.working))
	if fault.Effect == FaultTornSync {
		copied = min(copied, fault.Prefix)
	}
	copy(storage.durable[:copied], storage.working[:copied])
	if fault.Effect == FaultTornSync {
		return ErrInjectedFault
	}
	return nil
}

func (storage *Storage) Resize(size uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.nextFault().Effect != 0 {
		return ErrInjectedFault
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
	if storage.nextFault().Effect != 0 {
		return 0, ErrInjectedFault
	}
	return uint64(len(storage.working)), nil
}

func (storage *Storage) SyncParent() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.nextFault().Effect != 0 {
		return ErrInjectedFault
	}
	return nil
}

func (*Storage) Close() error {
	return nil
}

func (storage *Storage) Crash() {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.working = append(storage.working[:0], storage.durable...)
	storage.fault = StorageFault{}
}

func (storage *Storage) DurableBytes() []byte {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return append([]byte(nil), storage.durable...)
}

func (storage *Storage) nextFault() StorageFault {
	storage.operation++
	if storage.fault.At != storage.operation {
		return StorageFault{}
	}
	fault := storage.fault
	storage.fault = StorageFault{}
	return fault
}
