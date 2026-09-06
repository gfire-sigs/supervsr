package replication

import (
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestWALRecoverInstalledViewExcludesOnlyOlderTail(t *testing.T) {
	for _, test := range []struct {
		name        string
		view        protocol.View
		corrupt     bool
		staleHeader bool
		wantHead    protocol.Op
		wantFaulty  uint32
	}{
		{name: "old intact tail", view: 0},
		{name: "old corrupt tail remains uncertain", view: 0, corrupt: true, wantFaulty: 1},
		{name: "new intact tail", view: 1, wantHead: 1},
		{name: "new corrupt tail", view: 1, corrupt: true, wantFaulty: 1},
		{name: "stale header new corrupt tail", view: 1, corrupt: true, staleHeader: true, wantFaulty: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
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
			oldHeader := [protocol.HeaderSize]byte(prepare[:protocol.HeaderSize])
			header, _, reason := protocol.DecodeFrame(prepare, protocol.GroupID{1}, uint32(config.MessageSizeMax), 3)
			if reason != protocol.RejectNone {
				t.Fatal(reason)
			}
			header.View = test.view
			header.Author = protocol.ReplicaIndex(test.view)
			if err := protocol.SealFrame(prepare, &header); err != nil {
				t.Fatal(err)
			}
			if err := wal.Append(prepare, 0); err != nil {
				t.Fatal(err)
			}
			if test.corrupt {
				layout := wal.Layout()
				storage.working[layout.PrepareBase+layout.PrepareStride+protocol.HeaderSize] ^= 0x80
				if test.staleHeader {
					copy(storage.working[layout.HeaderBase+protocol.HeaderSize:layout.HeaderBase+2*protocol.HeaderSize], oldHeader[:])
				}
				if err := storage.Sync(); err != nil {
					t.Fatal(err)
				}
			}
			storage.Crash()

			wal, err = NewWAL(storage, config, protocol.GroupID{1}, 3)
			if err != nil {
				t.Fatal(err)
			}
			report, err = wal.Recover(checkpoint, 0, WALRecoveryView{LogView: 1, HeadOp: 0})
			if err != nil {
				t.Fatal(err)
			}
			if report.HeadOp != test.wantHead || report.FaultySlots != test.wantFaulty {
				t.Fatalf("recovery = %+v, want head %d faulty %d", report, test.wantHead, test.wantFaulty)
			}
			_, present, nack := wal.JoinEvidence(1)
			if test.wantFaulty != 0 {
				if present || nack {
					t.Fatalf("uncertain tail evidence: present=%t nack=%t", present, nack)
				}
			} else if test.view == 0 && (present || !nack) {
				t.Fatalf("excluded tail evidence: present=%t nack=%t", present, nack)
			}
		})
	}
}

func TestWALDiscardAfterSurvivesNeighborHeaderWrite(t *testing.T) {
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

	wal.DiscardAfter(0)
	if _, present, nack := wal.JoinEvidence(1); present || !nack {
		t.Fatalf("discarded tail evidence: present=%t nack=%t", present, nack)
	}
	if _, found := wal.RecoveredHeader(1); found {
		t.Fatal("discarded tail exposed as recovered")
	}
	if err := wal.Append(checkpoint.Header[:], 0); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	wal, err = NewWAL(storage, config, protocol.GroupID{1}, 3)
	if err != nil {
		t.Fatal(err)
	}
	report, err = wal.Recover(checkpoint, 0, WALRecoveryView{LogView: 1, HeadOp: 0})
	if err != nil {
		t.Fatal(err)
	}
	if report.HeadOp != 0 || report.FaultySlots != 0 {
		t.Fatalf("discarded tail resurrected: %+v", report)
	}
}

func TestWALRecoverStaleExcludedHeaderCannotHideCurrentCorruption(t *testing.T) {
	config, storage, checkpoint := formattedWALFixture(t)
	wal, err := NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	report, err := wal.Recover(checkpoint, 0, WALRecoveryView{})
	if err != nil {
		t.Fatal(err)
	}
	oldFrame := connectedPrepareFrame(t, config, report.HeadHeader, 1)
	current := append([]byte(nil), oldFrame...)
	header, _, reason := protocol.DecodeFrame(current, protocol.GroupID{1}, uint32(config.MessageSizeMax), 1)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	header.View = 1
	if err := protocol.SealFrame(current, &header); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(current, 0); err != nil {
		t.Fatal(err)
	}
	layout := wal.Layout()
	copy(storage.working[layout.HeaderBase+protocol.HeaderSize:], oldFrame[:protocol.HeaderSize])
	storage.working[layout.PrepareBase+layout.PrepareStride] ^= 0x80
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	wal, err = NewWAL(storage, config, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report, err := wal.Recover(checkpoint, 0, WALRecoveryView{LogView: 1, HeadOp: 0}); !errors.Is(err, ErrWALUncertainSolo) {
		t.Fatalf("stale excluded header hid a durable current-view prepare: report=%+v err=%v", report, err)
	}
}
