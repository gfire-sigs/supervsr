package replication

import (
	"errors"
	"fmt"
	"sync"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrUnformatted = errors.New("replication: storage is unformatted")
	ErrCorrupt     = errors.New("replication: durable state is corrupt")
)

type SuperblockStore struct {
	mu         sync.Mutex
	storage    Storage
	validation SuperblockValidation
	current    SuperblockCandidate
	buffer     []byte
	ioActive   bool
}

func OpenSuperblockStore(storage Storage, validation SuperblockValidation, beforeRepair ...func(Superblock) error) (*SuperblockStore, error) {
	if storage == nil || !protocol.HardwareChecksumAvailable() {
		return nil, ErrHardwareChecksumUnavailable
	}
	if len(beforeRepair) > 1 || (len(beforeRepair) == 1 && beforeRepair[0] == nil) {
		return nil, ErrInvalidConfiguration
	}
	minimumSize, ok := validation.Cluster.BlockBase()
	if !ok {
		return nil, ErrInvalidConfiguration
	}
	size, err := storage.Size()
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return nil, ErrUnformatted
	}
	if size < minimumSize || (size-minimumSize)%validation.Cluster.BlockSize != 0 {
		return nil, ErrCorrupt
	}
	buffer, err := NewAlignedBuffer(SuperblockBytes, SectorSize)
	if err != nil {
		return nil, err
	}
	copies := int(validation.Cluster.SuperblockCopies)
	candidates := make([]SuperblockCandidate, 0, copies)
	var byPhysical [8]SuperblockCandidate
	var validPhysical [8]bool
	allZero := true
	successfulReads := 0
	var firstReadError error
	incompatibleCopies := 0
	for index := range copies {
		clear(buffer)
		if err := storage.ReadAt(buffer, uint64(index)*SuperblockBytes); err != nil {
			if firstReadError == nil {
				firstReadError = err
			}
			continue
		}
		successfulReads++
		allZero = allZero && allZeroBytes(buffer)
		candidate, err := DecodeSuperblock(buffer, uint16(index), validation)
		if err != nil {
			if errors.Is(err, ErrIncompatibleConfiguration) {
				incompatibleCopies++
			}
			continue
		}
		candidates = append(candidates, candidate)
		byPhysical[index] = candidate
		validPhysical[index] = true
	}
	if len(candidates) == 0 {
		if successfulReads == 0 && firstReadError != nil {
			return nil, errors.Join(ErrStorage, firstReadError)
		}
		if allZero {
			return nil, ErrUnformatted
		}
		openQuorum, _ := superblockOpenQuorum(validation.Cluster.SuperblockCopies)
		if incompatibleCopies >= int(openQuorum) {
			return nil, ErrIncompatibleConfiguration
		}
		return nil, ErrCorrupt
	}
	selected, err := SelectSuperblock(candidates, validation.Cluster.SuperblockCopies)
	if err != nil {
		if errors.Is(err, ErrSuperblockInitializationIncomplete) {
			return nil, errors.Join(ErrUnformatted, err)
		}
		return nil, errors.Join(ErrCorrupt, err)
	}
	if len(beforeRepair) == 1 {
		if err := beforeRepair[0](selected.Superblock); err != nil {
			return nil, err
		}
	}
	store := &SuperblockStore{
		storage:    storage,
		validation: validation,
		current:    selected,
		buffer:     buffer,
	}
	if err := store.repairCopies(byPhysical, validPhysical); err != nil {
		return nil, err
	}
	return store, nil
}

func (store *SuperblockStore) Current() Superblock {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.current.Superblock
}

func (store *SuperblockStore) Persist(next Superblock) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	var sequence ioSequence
	if err := store.beginPersist(&sequence, &next); err != nil {
		return err
	}
	defer func() { store.ioActive = false }()
	for sequence.phase != IOPhaseCompletion {
		if err := store.advancePersist(&sequence, &next); err != nil {
			return err
		}
	}
	return nil
}

func (store *SuperblockStore) beginPersist(sequence *ioSequence, next *Superblock) error {
	if store.ioActive {
		return ErrIOBackpressure
	}
	current := &store.current.Superblock
	if next.Sequence != current.Sequence+1 || next.ParentChecksum != current.Checksum || durableStateRegressed(&current.State, &next.State) {
		return ErrInvalidSuperblock
	}
	if err := next.Validate(store.validation); err != nil {
		return err
	}
	if _, ok := superblockWriteCopies(store.validation.Cluster.SuperblockCopies); !ok {
		return ErrInvalidSuperblock
	}
	*sequence = ioSequence{storage: store.storage, index: next.Sequence % store.validation.Cluster.SuperblockCopies}
	if err := store.prepareCopy(sequence, next); err != nil {
		return err
	}
	store.ioActive = true
	return nil
}

func (store *SuperblockStore) prepareCopy(sequence *ioSequence, next *Superblock) error {
	physicalIndex := uint16((sequence.index + uint64(sequence.step)) % store.validation.Cluster.SuperblockCopies)
	if err := next.Encode(store.buffer, physicalIndex, store.validation); err != nil {
		return err
	}
	sequence.phase, sequence.buffer = IOPhaseWrite, store.buffer
	sequence.offset, sequence.size = uint64(physicalIndex)*SuperblockBytes, SuperblockBytes
	return nil
}

func (store *SuperblockStore) advancePersist(sequence *ioSequence, next *Superblock) error {
	if err := sequence.physical(); err != nil {
		if sequence.phase == IOPhaseWrite {
			return fmt.Errorf("%w: superblock copy %d: %w", ErrStorage, sequence.offset/SuperblockBytes, err)
		}
		return err
	}
	if sequence.phase == IOPhaseWrite {
		sequence.syncNext()
		return nil
	}
	sequence.step++
	writeCopies, _ := superblockWriteCopies(store.validation.Cluster.SuperblockCopies)
	if int(sequence.step) < int(writeCopies) {
		return store.prepareCopy(sequence, next)
	}
	store.current = SuperblockCandidate{Superblock: *next, PhysicalIndex: uint16(sequence.index)}
	sequence.phase = IOPhaseCompletion
	return nil
}

func (store *SuperblockStore) repairCopies(byPhysical [8]SuperblockCandidate, validPhysical [8]bool) error {
	selected := store.current.Superblock
	copies := int(store.validation.Cluster.SuperblockCopies)
	for index := range copies {
		valid := validPhysical[index]
		candidate := byPhysical[index]
		if valid && !candidate.MisdirectedIndex && candidate.Superblock.Checksum == selected.Checksum {
			continue
		}
		if err := selected.Encode(store.buffer, uint16(index), store.validation); err != nil {
			return err
		}
		if err := store.storage.WriteAt(store.buffer, uint64(index)*SuperblockBytes); err != nil {
			return fmt.Errorf("%w: repair superblock copy %d: %w", ErrStorage, index, err)
		}
		if err := store.storage.Sync(); err != nil {
			return err
		}
	}
	return nil
}
