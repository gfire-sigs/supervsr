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
	report, err := wal.Recover(checkpoint, 0, WALRecoveryView{})
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
	report, err = recovered.Recover(checkpoint, 0, WALRecoveryView{})
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
	for _, test := range []struct {
		name    string
		head    protocol.Op
		missing bool
	}{
		{name: "older corrupt body", head: 2},
		{name: "latest corrupt body", head: 1},
		{name: "latest missing body", head: 1, missing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			config, storage, checkpoint := formattedWALFixture(t)
			wal, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
			if err != nil {
				t.Fatal(err)
			}
			report, err := wal.Recover(checkpoint, 0, WALRecoveryView{})
			if err != nil {
				t.Fatal(err)
			}
			parent := report.HeadHeader
			for op := protocol.Op(1); op <= test.head; op++ {
				prepare := connectedPrepareFrame(t, config, parent, op)
				if err := wal.Append(prepare, 0); err != nil {
					t.Fatal(err)
				}
				var reason protocol.RejectReason
				parent, _, reason = protocol.DecodeFrame(prepare, protocol.GroupID{1}, uint32(config.MessageSizeMax), 1)
				if reason != protocol.RejectNone {
					t.Fatal(reason)
				}
			}
			layout := wal.Layout()
			offset := layout.PrepareBase + layout.PrepareStride
			if test.missing {
				clear(storage.working[offset : offset+layout.PrepareStride])
			} else {
				storage.working[offset+protocol.HeaderSize] ^= 0x80
			}
			if err := storage.Sync(); err != nil {
				t.Fatal(err)
			}
			storage.Crash()

			recovered, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := recovered.Recover(checkpoint, 0, WALRecoveryView{}); !errors.Is(err, ErrWALUncertainSolo) {
				t.Fatalf("error = %v, want %v", err, ErrWALUncertainSolo)
			}
		})
	}
}

func TestWALRecoverLatestBodyNeedsRemoteRepair(t *testing.T) {
	for _, missing := range []bool{false, true} {
		name := "corrupt body"
		if missing {
			name = "missing body"
		}
		t.Run(name, func(t *testing.T) {
			config, storage, checkpoint := formattedWALFixture(t)
			wal, err := NewWAL(storage, config, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			report, err := wal.Recover(checkpoint, 0, WALRecoveryView{})
			if err != nil {
				t.Fatal(err)
			}
			prepare := connectedPrepareFrame(t, config, report.HeadHeader, 1)
			if err := wal.Append(prepare, 0); err != nil {
				t.Fatal(err)
			}
			layout := wal.Layout()
			offset := layout.PrepareBase + layout.PrepareStride
			if missing {
				clear(storage.working[offset : offset+layout.PrepareStride])
			} else {
				storage.working[offset+protocol.HeaderSize] ^= 0x80
			}
			if err := storage.Sync(); err != nil {
				t.Fatal(err)
			}
			storage.Crash()

			for reopen := range 2 {
				wal, err = NewWAL(storage, config, protocol.GroupID{1}, 3)
				if err != nil {
					t.Fatal(err)
				}
				report, err = wal.Recover(checkpoint, 1, WALRecoveryView{})
				if err != nil {
					t.Fatal(err)
				}
				if report.HeadOp != 0 || report.FaultySlots != 1 || report.UntrustedMax != 1 {
					t.Fatalf("reopen %d: report = %+v, want unresolved op 1", reopen, report)
				}
				if _, found := wal.RecoveredHeader(1); found {
					t.Fatal("uncertain prepare exposed as recovered")
				}
				if _, present, nack := wal.JoinEvidence(1); present || nack {
					t.Fatalf("uncertain prepare evidence: present=%t nack=%t", present, nack)
				}
				// Rewriting a neighboring header must not persist the reserved placeholder.
				if err := wal.Append(checkpoint.Header[:], 0); err != nil {
					t.Fatal(err)
				}
				storage.Crash()
			}

			if err := wal.Append(prepare, 0); err != nil {
				t.Fatal(err)
			}
			storage.Crash()
			wal, err = NewWAL(storage, config, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			report, err = wal.Recover(checkpoint, 1, WALRecoveryView{})
			if err != nil {
				t.Fatal(err)
			}
			if report.HeadOp != 1 || report.FaultySlots != 0 {
				t.Fatalf("repaired report = %+v", report)
			}
			if _, present, nack := wal.JoinEvidence(1); !present || nack {
				t.Fatalf("repaired prepare evidence: present=%t nack=%t", present, nack)
			}
		})
	}
}

func TestWALRecoverTruncatesProvenFutureHeader(t *testing.T) {
	config, storage, checkpoint := formattedWALFixture(t)
	wal, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wal.Recover(checkpoint, 0, WALRecoveryView{})
	if err != nil {
		t.Fatal(err)
	}
	maximum, ok := wal.prepareMaximum(checkpoint.PrepareOp())
	if !ok {
		t.Fatal("invalid prepare maximum")
	}
	op := maximum + 1
	prepare := connectedPrepareFrame(t, config, report.HeadHeader, op)
	if err := wal.Append(prepare, 0); err != nil {
		t.Fatal(err)
	}
	layout := wal.Layout()
	offset := layout.PrepareBase + uint64(op)%config.JournalSlots*layout.PrepareStride
	clear(storage.working[offset : offset+layout.PrepareStride])
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()

	wal, err = NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err = wal.Recover(checkpoint, 0, WALRecoveryView{})
	if err != nil {
		t.Fatal(err)
	}
	if report.HeadOp != 0 || report.FaultySlots != 0 {
		t.Fatalf("future header recovery = %+v", report)
	}
	if _, present, nack := wal.JoinEvidence(op); present || !nack {
		t.Fatalf("proven future evidence: present=%t nack=%t", present, nack)
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
