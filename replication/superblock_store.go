package replication

import (
	"errors"
	"fmt"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var (
	ErrUnformatted = errors.New("replication: storage is unformatted")
	ErrCorrupt     = errors.New("replication: durable state is corrupt")
)

type SuperblockStore struct {
	storage    Storage
	validation SuperblockValidation
	current    SuperblockCandidate
	buffer     []byte
}

func OpenSuperblockStore(storage Storage, validation SuperblockValidation) (*SuperblockStore, error) {
	if storage == nil || !protocol.HardwareChecksumAvailable() {
		return nil, ErrHardwareChecksumUnavailable
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
		return nil, ErrCorrupt
	}
	selected, err := SelectSuperblock(candidates, validation.Cluster.SuperblockCopies)
	if err != nil {
		if errors.Is(err, ErrSuperblockInitializationIncomplete) {
			return nil, errors.Join(ErrUnformatted, err)
		}
		return nil, errors.Join(ErrCorrupt, err)
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
	return store.current.Superblock
}

func (store *SuperblockStore) Persist(next Superblock) error {
	current := &store.current.Superblock
	if next.Sequence != current.Sequence+1 || next.ParentChecksum != current.Checksum || durableStateRegressed(&current.State, &next.State) {
		return ErrInvalidSuperblock
	}
	if err := next.Validate(store.validation); err != nil {
		return err
	}
	writeCopies, ok := superblockWriteCopies(store.validation.Cluster.SuperblockCopies)
	if !ok {
		return ErrInvalidSuperblock
	}
	start := next.Sequence % store.validation.Cluster.SuperblockCopies
	for step := range writeCopies {
		physicalIndex := uint16((start + uint64(step)) % store.validation.Cluster.SuperblockCopies)
		if err := next.Encode(store.buffer, physicalIndex, store.validation); err != nil {
			return err
		}
		if err := store.storage.WriteAt(store.buffer, uint64(physicalIndex)*SuperblockBytes); err != nil {
			return fmt.Errorf("%w: superblock copy %d: %w", ErrStorage, physicalIndex, err)
		}
		if err := store.storage.Sync(); err != nil {
			return err
		}
	}
	store.current = SuperblockCandidate{Superblock: next, PhysicalIndex: uint16(start)}
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
