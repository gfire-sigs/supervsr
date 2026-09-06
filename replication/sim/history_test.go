package sim

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
)

func ledgerReply(token, value uint64) []byte {
	body := make([]byte, 16)
	binary.LittleEndian.PutUint64(body[:8], token)
	binary.LittleEndian.PutUint64(body[8:], value)
	return body
}

func newTestHistory(t *testing.T, capacity int) *History {
	t.Helper()
	history, err := NewHistory(capacity)
	if err != nil {
		t.Fatal(err)
	}
	return history
}

func TestHistoryAcceptsConcurrentRepliesOutOfOrder(t *testing.T) {
	history := newTestHistory(t, 3)
	for _, token := range []uint64{10, 20, 30} {
		if err := history.Submit(token); err != nil {
			t.Fatal(err)
		}
	}
	machine, err := NewLedgerMachine(DefaultConfig(1).Cluster)
	if err != nil {
		t.Fatal(err)
	}
	// Execution order need not match submission or response order.
	for index, token := range []uint64{20, 10, 30} {
		body := EncodeLedgerRequest(token)
		if _, err := machine.Commit(replication.CommitInput{Operation: LedgerIncrement, Body: body[:], Op: replication.Op(index + 1)}, 0, make([]byte, 16)); err != nil {
			t.Fatal(err)
		}
	}
	for _, result := range [][2]uint64{{30, 3}, {20, 1}, {10, 2}} {
		if err := history.Complete(result[0], ledgerReply(result[0], result[1])); err != nil {
			t.Fatal(err)
		}
	}
	if err := history.CheckFinal(machine.Snapshot()); err != nil {
		t.Fatal(err)
	}
}

func TestHistoryRejectsInvalidCompletions(t *testing.T) {
	for _, test := range []struct {
		name  string
		token uint64
		reply []byte
	}{
		{name: "wrong token", token: 2, reply: ledgerReply(1, 2)},
		{name: "unknown request", token: 3, reply: ledgerReply(3, 1)},
		{name: "duplicate result", token: 2, reply: ledgerReply(2, 1)},
		{name: "duplicate reply", token: 1, reply: ledgerReply(1, 1)},
		{name: "more effects than submissions", token: 2, reply: ledgerReply(2, 3)},
		{name: "zero result", token: 2, reply: ledgerReply(2, 0)},
		{name: "truncated reply", token: 2, reply: []byte{2}},
	} {
		t.Run(test.name, func(t *testing.T) {
			history := newTestHistory(t, 2)
			for _, token := range []uint64{1, 2} {
				if err := history.Submit(token); err != nil {
					t.Fatal(err)
				}
			}
			if err := history.Complete(1, ledgerReply(1, 1)); err != nil {
				t.Fatal(err)
			}
			if err := history.Complete(test.token, test.reply); !errors.Is(err, ErrHistory) {
				t.Fatalf("completion error=%v, want invalid history", err)
			}
		})
	}
}

func TestHistoryRejectsRealTimeInversion(t *testing.T) {
	history := newTestHistory(t, 3)
	for _, token := range []uint64{1, 2} {
		if err := history.Submit(token); err != nil {
			t.Fatal(err)
		}
	}
	if err := history.Complete(2, ledgerReply(2, 2)); err != nil {
		t.Fatal(err)
	}
	if err := history.Submit(3); err != nil {
		t.Fatal(err)
	}
	// Value 1 is unused, but token 3 was invoked after value 2 returned.
	if err := history.Complete(3, ledgerReply(3, 1)); !errors.Is(err, ErrHistory) {
		t.Fatalf("real-time inversion error=%v", err)
	}
}

func TestHistoryRejectsCapacityOverflowWithoutDroppingRequests(t *testing.T) {
	history := newTestHistory(t, 1)
	if err := history.Submit(1); err != nil {
		t.Fatal(err)
	}
	if err := history.Submit(2); !errors.Is(err, ErrHistoryCapacity) {
		t.Fatalf("overflow error=%v", err)
	}
	if err := history.Complete(1, ledgerReply(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := history.Submit(1); !errors.Is(err, ErrHistory) {
		t.Fatalf("reused retired token error=%v", err)
	}
	if err := history.Submit(2); err != nil {
		t.Fatalf("rejected submission consumed token: %v", err)
	}
}

func TestHistoryBoundsOutOfOrderResults(t *testing.T) {
	history := newTestHistory(t, 2)
	if err := history.Submit(1); err != nil {
		t.Fatal(err)
	}
	for token := uint64(2); token <= 3; token++ {
		if err := history.Submit(token); err != nil {
			t.Fatal(err)
		}
		if err := history.Complete(token, ledgerReply(token, token)); err != nil {
			t.Fatal(err)
		}
	}
	if err := history.Submit(4); err != nil {
		t.Fatal(err)
	}
	if err := history.Complete(4, ledgerReply(4, 4)); !errors.Is(err, ErrHistoryCapacity) {
		t.Fatalf("result frontier overflow=%v", err)
	}
	if err := history.Complete(1, ledgerReply(1, 1)); err != nil {
		t.Fatal(err)
	}
	if err := history.Complete(4, ledgerReply(4, 4)); err != nil {
		t.Fatalf("overflow consumed pending result: %v", err)
	}
}

func TestHistoryFinalConservationOutlivesObservationWindow(t *testing.T) {
	history := newTestHistory(t, 1)
	machine, err := NewLedgerMachine(DefaultConfig(1).Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for token := uint64(1); token <= 3*CommitHistoryMax; token++ {
		if err := history.Submit(token); err != nil {
			t.Fatal(err)
		}
		body := EncodeLedgerRequest(token)
		var reply [16]byte
		if _, err := machine.Commit(replication.CommitInput{Operation: LedgerIncrement, Body: body[:], Op: replication.Op(token)}, 0, reply[:]); err != nil {
			t.Fatal(err)
		}
		if err := history.Complete(token, reply[:]); err != nil {
			t.Fatal(err)
		}
	}
	state := machine.Snapshot()
	if err := history.CheckFinal(state); err != nil {
		t.Fatal(err)
	}
	for _, delta := range []int64{-1, 1} {
		changed := state
		changed.Value = uint64(int64(changed.Value) + delta)
		if err := history.CheckFinal(changed); !errors.Is(err, ErrHistory) {
			t.Fatalf("value delta %d error=%v", delta, err)
		}
	}
	state.Digest[0] ^= 1
	if err := history.CheckFinal(state); !errors.Is(err, ErrHistory) {
		t.Fatalf("same-count substitution error=%v", err)
	}
	if err := history.Submit(3*CommitHistoryMax + 1); err != nil {
		t.Fatal(err)
	}
	if err := history.CheckFinal(machine.Snapshot()); !errors.Is(err, ErrHistory) {
		t.Fatalf("unfinished request error=%v", err)
	}
}
