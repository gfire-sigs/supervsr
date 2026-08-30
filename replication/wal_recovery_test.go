package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestWALRecoverRepairsPrepareBeforeRedundantHeader(t *testing.T) {
	config, storage, checkpoint := formattedWALFixture(t)
	wal, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wal.Recover(checkpoint, 0, DefaultProcessConfig())
	if err != nil {
		t.Fatal(err)
	}
	prepare := connectedPrepareFrame(t, config, report.HeadHeader, 1)
	if err := wal.Append(prepare, 0); err != nil {
		t.Fatal(err)
	}

	reserved, err := ReservedPrepareHeader(protocol.GroupID{1}, 1, 1, uint32(config.MessageSizeMax))
	if err != nil {
		t.Fatal(err)
	}
	layout := wal.Layout()
	copy(storage.working[layout.HeaderBase+protocol.HeaderSize:layout.HeaderBase+2*protocol.HeaderSize], reserved[:])
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()

	recovered, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err = recovered.Recover(checkpoint, 0, DefaultProcessConfig())
	if err != nil {
		t.Fatal(err)
	}
	if report.HeadOp != 1 || report.FaultySlots != 0 {
		t.Fatalf("report = %+v", report)
	}
	header, found := recovered.RecoveredHeader(1)
	var expectedChecksum protocol.Checksum
	copy(expectedChecksum[:], prepare[:16])
	if !found || header.HeaderChecksum != expectedChecksum {
		t.Fatal("full prepare did not repair its redundant header")
	}
}

func TestWALRecoverSoloRejectsUncertainBody(t *testing.T) {
	config, storage, checkpoint := formattedWALFixture(t)
	wal, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wal.Recover(checkpoint, 0, DefaultProcessConfig())
	if err != nil {
		t.Fatal(err)
	}
	prepare := connectedPrepareFrame(t, config, report.HeadHeader, 1)
	if err := wal.Append(prepare, 0); err != nil {
		t.Fatal(err)
	}
	firstHeader, _, reason := protocol.DecodeFrame(prepare, protocol.GroupID{1}, uint32(config.MessageSizeMax), 1)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	second := connectedPrepareFrame(t, config, firstHeader, 2)
	if err := wal.Append(second, 0); err != nil {
		t.Fatal(err)
	}
	layout := wal.Layout()
	storage.working[layout.PrepareBase+layout.PrepareStride+protocol.HeaderSize] ^= 0x80
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()

	recovered, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := recovered.Recover(checkpoint, 0, DefaultProcessConfig()); !errors.Is(err, ErrWALUncertainSolo) {
		t.Fatalf("error = %v, want %v", err, ErrWALUncertainSolo)
	}
}

func TestRecoveredCommitTargetRetainsBoundedSuffix(t *testing.T) {
	config := compactTestClusterConfig()
	head := protocol.Header{}
	putUint64(head.Fields[96:104], 10)
	putUint64(head.Fields[104:112], 3)
	recovery := WALRecoveryReport{HeadOp: 10, HeadHeader: head}
	durable := Superblock{}

	target, err := recoveredCommitTarget(config, durable, recovery)
	if err != nil {
		t.Fatal(err)
	}
	want := protocol.Op(10 - config.PipelineMax)
	if target != want {
		t.Fatalf("recovered target = %d, want retained floor %d", target, want)
	}

	putUint64(recovery.HeadHeader.Fields[104:112], 7)
	target, err = recoveredCommitTarget(config, durable, recovery)
	if err != nil || target != 7 {
		t.Fatalf("recovered target = %d, error %v, want head commit 7", target, err)
	}

	durable.State.CommitMax = 8
	target, err = recoveredCommitTarget(config, durable, recovery)
	if err != nil || target != 8 {
		t.Fatalf("recovered target = %d, error %v, want durable commit 8", target, err)
	}

	durable.State.CommitMax = 11
	if _, err := recoveredCommitTarget(config, durable, recovery); !errors.Is(err, ErrWALRecovery) {
		t.Fatalf("invalid durable commit error = %v", err)
	}
}

func formattedWALFixture(t testing.TB) (ClusterConfig, *crashStorage, CheckpointState) {
	t.Helper()
	config := compactTestClusterConfig()
	membership := Membership{Members: [MembersMax]protocol.MemberID{{1}}, ActiveCount: 1, LocalMember: protocol.MemberID{1}}
	storage := &crashStorage{}
	format := FormatConfig{Group: protocol.GroupID{1}, Membership: membership, Cluster: config, CurrentRelease: 1}
	if err := Format(context.Background(), format, FormatDependencies{Storage: storage}); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	store, err := OpenSuperblockStore(storage, SuperblockValidation{Group: format.Group, Membership: membership, ConfigurationChecksum: config.Fingerprint(), Cluster: config})
	if err != nil {
		t.Fatal(err)
	}
	return config, storage, store.Current().State.Checkpoint
}

func connectedPrepareFrame(t testing.TB, config ClusterConfig, parent protocol.Header, op protocol.Op) []byte {
	t.Helper()
	body := []byte{1}
	frame := make([]byte, protocol.HeaderSize+len(body))
	copy(frame[protocol.HeaderSize:], body)
	header := protocol.Header{Group: protocol.GroupID{1}, View: 0, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandPrepare, Author: 0}
	copy(header.Fields[0:16], parent.HeaderChecksum[:])
	header.Fields[32] = 2
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
