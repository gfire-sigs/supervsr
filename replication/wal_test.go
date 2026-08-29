package replication

import (
	"errors"
	"fmt"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestWALLayoutMatchesBlockBase(t *testing.T) {
	cfg := compactTestClusterConfig()
	layout, ok := DeriveWALLayout(cfg)
	if !ok {
		t.Fatal("layout derivation failed")
	}
	blockBase, ok := cfg.BlockBase()
	if !ok {
		t.Fatal("BlockBase derivation failed")
	}
	if layout.BlockBase != blockBase {
		t.Fatalf("layout block base = %d, want %d", layout.BlockBase, blockBase)
	}
	if layout.HeaderBase%SectorSize != 0 || layout.PrepareBase%SectorSize != 0 || layout.PrepareStride%SectorSize != 0 || layout.ReplyBase%SectorSize != 0 || layout.BlockBase%cfg.BlockSize != 0 {
		t.Fatalf("unaligned layout: %+v", layout)
	}
}

func TestWALAppendPersistsBodyBeforeRedundantHeader(t *testing.T) {
	cfg := compactTestClusterConfig()
	layout, _ := DeriveWALLayout(cfg)
	storage := newScriptedStorage(t, layout.BlockBase)
	wal, err := NewWAL(storage, cfg, protocol.GroupID{1}, 3)
	if err != nil {
		t.Fatal(err)
	}
	frame := validPrepareFrame(t, cfg, 1)
	if err := wal.Append(frame, 0); err != nil {
		t.Fatal(err)
	}

	prepareOffset := layout.PrepareBase + layout.PrepareStride
	headerSectorOffset := layout.HeaderBase
	want := []string{
		fmt.Sprintf("write:%d:%d", prepareOffset, layout.PrepareStride),
		"sync",
		fmt.Sprintf("write:%d:%d", headerSectorOffset, SectorSize),
		"sync",
	}
	if fmt.Sprint(storage.operations) != fmt.Sprint(want) {
		t.Fatalf("operations = %v, want %v", storage.operations, want)
	}
	slot := &wal.slots[1]
	if slot.Dirty || slot.Faulty || !slot.Inhabited || slot.Op != 1 {
		t.Fatalf("durable slot = %+v", slot)
	}
}

func TestWALAppendFailureNeverClearsDirty(t *testing.T) {
	cfg := compactTestClusterConfig()
	layout, _ := DeriveWALLayout(cfg)
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run(fmt.Sprintf("operation_%d", failAt), func(t *testing.T) {
			storage := newScriptedStorage(t, layout.BlockBase)
			storage.failAt = failAt
			wal, err := NewWAL(storage, cfg, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			if err := wal.Append(validPrepareFrame(t, cfg, 1), 0); err == nil {
				t.Fatal("append succeeded across injected failure")
			}
			if !wal.slots[1].Dirty {
				t.Fatal("failed append cleared dirty slot")
			}
		})
	}
}

func TestWALRefusesUnsafeSlotReuse(t *testing.T) {
	cfg := compactTestClusterConfig()
	layout, _ := DeriveWALLayout(cfg)
	storage := newScriptedStorage(t, layout.BlockBase)
	wal, err := NewWAL(storage, cfg, protocol.GroupID{1}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(validPrepareFrame(t, cfg, 1), 0); err != nil {
		t.Fatal(err)
	}
	nextWrap := protocol.Op(1 + cfg.JournalSlots)
	if err := wal.Append(validPrepareFrame(t, cfg, nextWrap), 0); !errors.Is(err, ErrWALSlotUnsafe) {
		t.Fatalf("reuse error = %v, want %v", err, ErrWALSlotUnsafe)
	}
	if err := wal.Append(validPrepareFrame(t, cfg, nextWrap), 1); err != nil {
		t.Fatalf("checkpoint-safe reuse: %v", err)
	}
}

func TestWALRecoveryDecisionMatrix(t *testing.T) {
	context := WALRecoveryContext{
		PhysicalSlot: 1,
		JournalSlots: 8,
		RetainedMin:  1,
		PrepareMax:   20,
		UntrustedMax: 17,
		TornMin:      17,
	}
	ordinary := func(op protocol.Op, checksum byte) WALCandidate {
		return WALCandidate{Kind: WALCandidateOrdinary, Op: op, View: 1, HeaderChecksum: protocol.Checksum{checksum}, BodyChecksum: protocol.Checksum{checksum + 1}}
	}
	invalid := WALCandidate{Kind: WALCandidateInvalid}
	reserved := WALCandidate{Kind: WALCandidateReserved}
	tests := []struct {
		name    string
		header  WALCandidate
		prepare WALCandidate
		want    WALRecoveryDecision
	}{
		{name: "clean equal", header: ordinary(9, 1), prepare: ordinary(9, 1), want: WALRecoveryClean},
		{name: "clean empty", header: reserved, prepare: reserved, want: WALRecoveryCleanEmpty},
		{name: "reserved header full prepare", header: reserved, prepare: ordinary(9, 1), want: WALRecoveryLocalRepair},
		{name: "invalid header unique maximum", header: invalid, prepare: ordinary(17, 1), want: WALRecoveryLocalRepair},
		{name: "newer prepare same slot", header: ordinary(9, 1), prepare: ordinary(17, 2), want: WALRecoveryLocalRepair},
		{name: "old header missing body", header: ordinary(9, 1), prepare: invalid, want: WALRecoveryRemoteRepair},
		{name: "bounded torn header", header: ordinary(17, 1), prepare: invalid, want: WALRecoveryTruncate},
		{name: "invalid uncertainty", header: invalid, prepare: invalid, want: WALRecoveryRemoteRepair},
		{name: "nonmaximum prepare uncertainty", header: invalid, prepare: ordinary(9, 1), want: WALRecoveryRemoteRepair},
		{name: "conflicting checksum", header: ordinary(9, 1), prepare: ordinary(9, 2), want: WALRecoveryRemoteRepair},
		{name: "future prepare", header: reserved, prepare: ordinary(25, 1), want: WALRecoveryTruncate},
		{name: "impossible slot", header: ordinary(10, 1), prepare: invalid, want: WALRecoveryFail},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if actual := ClassifyWALSlot(test.header, test.prepare, context); actual != test.want {
				t.Fatalf("decision = %d, want %d", actual, test.want)
			}
		})
	}
}

func compactTestClusterConfig() ClusterConfig {
	cfg := DefaultClusterConfig()
	cfg.ClientsMax = 4
	cfg.PipelineMax = 4
	cfg.ViewChangeHeadersSuffixMax = 5
	cfg.JournalSlots = 128
	cfg.MessageSizeMax = 4 << 10
	cfg.ApplicationBatchSizeMax = cfg.MessageSizeMax - protocol.HeaderSize
	cfg.ApplicationReplySizeMax = cfg.MessageSizeMax - protocol.HeaderSize
	cfg.BlockSize = 4 << 10
	cfg.CompactionOps = 16
	return cfg
}

func validPrepareFrame(t testing.TB, cfg ClusterConfig, op protocol.Op) []byte {
	t.Helper()
	body := []byte{1, 2, 3}
	frame := make([]byte, protocol.HeaderSize+len(body))
	copy(frame[protocol.HeaderSize:], body)
	header := protocol.Header{
		Group:    protocol.GroupID{1},
		View:     0,
		Release:  1,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandPrepare,
		Author:   0,
	}
	header.Fields[0] = 1
	header.Fields[80] = 1
	putUint64(header.Fields[96:104], uint64(op))
	putUint64(header.Fields[112:120], uint64(op))
	putUint32(header.Fields[120:124], uint32(op))
	header.Fields[124] = byte(protocol.OperationApplicationMin)
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame
}

func putUint64(destination []byte, value uint64) {
	for index := range 8 {
		destination[index] = byte(value >> (index * 8))
	}
}

func putUint32(destination []byte, value uint32) {
	for index := range 4 {
		destination[index] = byte(value >> (index * 8))
	}
}

type scriptedStorage struct {
	bytes      []byte
	operations []string
	failAt     int
}

func newScriptedStorage(t testing.TB, size uint64) *scriptedStorage {
	t.Helper()
	if size > uint64(int(^uint(0)>>1)) {
		t.Fatal("scripted storage size overflow")
	}
	return &scriptedStorage{bytes: make([]byte, int(size))}
}

func (storage *scriptedStorage) ReadAt(buffer []byte, offset uint64) error {
	if offset+uint64(len(buffer)) > uint64(len(storage.bytes)) {
		return ErrShortIO
	}
	copy(buffer, storage.bytes[offset:offset+uint64(len(buffer))])
	return nil
}

func (storage *scriptedStorage) WriteAt(buffer []byte, offset uint64) error {
	storage.operations = append(storage.operations, fmt.Sprintf("write:%d:%d", offset, len(buffer)))
	if storage.shouldFail() {
		return ErrStorage
	}
	if offset+uint64(len(buffer)) > uint64(len(storage.bytes)) {
		return ErrShortIO
	}
	copy(storage.bytes[offset:offset+uint64(len(buffer))], buffer)
	return nil
}

func (storage *scriptedStorage) Sync() error {
	storage.operations = append(storage.operations, "sync")
	if storage.shouldFail() {
		return ErrStorage
	}
	return nil
}

func (storage *scriptedStorage) Resize(size uint64) error {
	storage.bytes = make([]byte, int(size))
	return nil
}

func (storage *scriptedStorage) Size() (uint64, error) {
	return uint64(len(storage.bytes)), nil
}

func (storage *scriptedStorage) SyncParent() error { return nil }
func (storage *scriptedStorage) Close() error      { return nil }

func (storage *scriptedStorage) shouldFail() bool {
	return storage.failAt > 0 && len(storage.operations) == storage.failAt
}
