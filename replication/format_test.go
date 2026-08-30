package replication

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestFormatCreatesRepairableInitialQuorum(t *testing.T) {
	cfg, validation := formatFixture(t)
	storage := &crashStorage{}
	if err := Format(t.Context(), cfg, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	store, err := OpenSuperblockStore(storage, validation)
	if err != nil {
		t.Fatal(err)
	}
	if store.Current().Sequence != 1 || store.Current().State.View != 0 || store.Current().State.LogView != 0 {
		t.Fatalf("initial superblock = %+v", store.Current())
	}
	if copies := validInitialCopyCount(storage, validation); copies != int(cfg.Cluster.SuperblockCopies) {
		t.Fatalf("repaired copies = %d, want %d", copies, cfg.Cluster.SuperblockCopies)
	}
}

func TestOpenSuperblockStoreDistinguishesIncompatibleConfiguration(t *testing.T) {
	cfg, validation := formatFixture(t)
	storage := &crashStorage{}
	if err := Format(t.Context(), cfg, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	validation.ConfigurationChecksum[0] ^= 0xff
	if _, err := OpenSuperblockStore(storage, validation); !errors.Is(err, ErrIncompatibleConfiguration) {
		t.Fatalf("open error = %v, want %v", err, ErrIncompatibleConfiguration)
	}
}

func TestFormatCrashBeforeWriteQuorumRemainsUnformatted(t *testing.T) {
	cfg, validation := formatFixture(t)
	probe := &crashStorage{}
	if err := Format(t.Context(), cfg, FormatDependencies{Storage: probe}); err != nil {
		t.Fatal(err)
	}
	operationCount := probe.operation
	writeQuorum, _ := superblockWriteCopies(cfg.Cluster.SuperblockCopies)

	for failAt := 1; failAt <= operationCount; failAt++ {
		t.Run(operationName(failAt), func(t *testing.T) {
			storage := &crashStorage{failAt: failAt}
			if err := Format(context.Background(), cfg, FormatDependencies{Storage: storage}); err == nil {
				t.Fatal("format succeeded across injected crash")
			}
			storage.Crash()
			durableCopies := validInitialCopyCount(storage, validation)
			store, err := OpenSuperblockStore(storage, validation)
			if durableCopies < int(writeQuorum) {
				if !errors.Is(err, ErrUnformatted) {
					t.Fatalf("durable copies %d: open error = %v, want %v", durableCopies, err, ErrUnformatted)
				}
				return
			}
			if err != nil {
				t.Fatalf("durable copies %d: open: %v", durableCopies, err)
			}
			if store.Current().Sequence != 1 {
				t.Fatalf("sequence = %d, want 1", store.Current().Sequence)
			}
		})
	}
}

func TestFormatRejectsNonemptyTarget(t *testing.T) {
	cfg, _ := formatFixture(t)
	storage := &crashStorage{working: make([]byte, 1), durable: make([]byte, 1)}
	if err := Format(t.Context(), cfg, FormatDependencies{Storage: storage}); !errors.Is(err, ErrStorageNotEmpty) {
		t.Fatalf("error = %v, want %v", err, ErrStorageNotEmpty)
	}
}

func TestSuperblockUpdateCrashSelectsOnlyDurableQuorum(t *testing.T) {
	cfg, validation := formatFixture(t)
	openQuorum, _ := superblockOpenQuorum(cfg.Cluster.SuperblockCopies)
	for failAt := 1; failAt <= 6; failAt++ {
		t.Run(operationName(failAt), func(t *testing.T) {
			storage := &crashStorage{}
			if err := Format(t.Context(), cfg, FormatDependencies{Storage: storage}); err != nil {
				t.Fatal(err)
			}
			storage.Crash()
			store, err := OpenSuperblockStore(storage, validation)
			if err != nil {
				t.Fatal(err)
			}
			storage.operation = 0
			storage.failAt = failAt
			next := store.Current()
			next.Sequence++
			next.ParentChecksum = store.Current().Checksum
			next.State.View = 1
			if err := store.Persist(next); err == nil {
				t.Fatal("superblock update succeeded across injected crash")
			}
			storage.Crash()
			durableNewCopies := validSequenceCopyCount(storage, validation, 2)
			reopened, err := OpenSuperblockStore(storage, validation)
			if err != nil {
				t.Fatal(err)
			}
			wantSequence := uint64(1)
			if durableNewCopies >= int(openQuorum) {
				wantSequence = 2
			}
			if reopened.Current().Sequence != wantSequence {
				t.Fatalf("new copies %d: sequence = %d, want %d", durableNewCopies, reopened.Current().Sequence, wantSequence)
			}
		})
	}
}

func formatFixture(t testing.TB) (FormatConfig, SuperblockValidation) {
	t.Helper()
	cluster := compactTestClusterConfig()
	if err := cluster.Validate(3, 0); err != nil {
		t.Fatal(err)
	}
	membership := Membership{
		Members:     [MembersMax]protocol.MemberID{{1}, {2}, {3}},
		ActiveCount: 3,
		LocalMember: protocol.MemberID{1},
	}
	cfg := FormatConfig{
		Group:          protocol.GroupID{1},
		Membership:     membership,
		Cluster:        cluster,
		CurrentRelease: 1,
	}
	return cfg, SuperblockValidation{
		Group:                 cfg.Group,
		Membership:            membership,
		ConfigurationChecksum: cluster.Fingerprint(),
		Cluster:               cluster,
	}
}

func validInitialCopyCount(storage *crashStorage, validation SuperblockValidation) int {
	return validSequenceCopyCount(storage, validation, 1)
}

func validSequenceCopyCount(storage *crashStorage, validation SuperblockValidation, sequence uint64) int {
	if len(storage.working) < int(validation.Cluster.SuperblockCopies)*SuperblockBytes {
		return 0
	}
	count := 0
	for index := range int(validation.Cluster.SuperblockCopies) {
		start := index * SuperblockBytes
		candidate, err := DecodeSuperblock(storage.working[start:start+SuperblockBytes], uint16(index), validation)
		if err == nil && candidate.Superblock.Sequence == sequence {
			count++
		}
	}
	return count
}

func operationName(index int) string {
	const prefix = "operation_"
	return prefix + string(rune('A'+index-1))
}

type crashStorage struct {
	mu        sync.Mutex
	working   []byte
	durable   []byte
	operation int
	failAt    int
}

func (storage *crashStorage) ReadAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if offset+uint64(len(buffer)) > uint64(len(storage.working)) {
		return ErrShortIO
	}
	copy(buffer, storage.working[offset:offset+uint64(len(buffer))])
	return nil
}

func (storage *crashStorage) WriteAt(buffer []byte, offset uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.fail() {
		return ErrStorage
	}
	if offset+uint64(len(buffer)) > uint64(len(storage.working)) {
		return ErrShortIO
	}
	copy(storage.working[offset:offset+uint64(len(buffer))], buffer)
	return nil
}

func (storage *crashStorage) Sync() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.fail() {
		return ErrStorage
	}
	storage.durable = append(storage.durable[:0], storage.working...)
	return nil
}

func (storage *crashStorage) Resize(size uint64) error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.fail() {
		return ErrStorage
	}
	if size <= uint64(len(storage.working)) {
		storage.working = storage.working[:size]
		return nil
	}
	storage.working = append(storage.working, make([]byte, int(size)-len(storage.working))...)
	return nil
}

func (storage *crashStorage) Size() (uint64, error) {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	return uint64(len(storage.working)), nil
}

func (storage *crashStorage) SyncParent() error {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	if storage.fail() {
		return ErrStorage
	}
	return nil
}

func (storage *crashStorage) Close() error { return nil }

func (storage *crashStorage) Crash() {
	storage.mu.Lock()
	defer storage.mu.Unlock()
	storage.working = append(storage.working[:0], storage.durable...)
	storage.failAt = 0
	storage.operation = 0
}

func (storage *crashStorage) fail() bool {
	storage.operation++
	return storage.failAt == storage.operation
}
