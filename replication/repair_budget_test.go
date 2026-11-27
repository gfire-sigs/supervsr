package replication

import (
	"errors"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
)

func TestJournalRepairBudgetCapsPeersAndReleasesOperation(t *testing.T) {
	budget, err := newJournalRepairBudget(3, 0)
	if err != nil {
		t.Fatal(err)
	}
	random := newDeterministicRandom(7)
	first, ok, err := budget.Reserve(8, 10, &random)
	if err != nil || !ok {
		t.Fatalf("first reservation rejected: ok=%t err=%v", ok, err)
	}
	second, ok, err := budget.Reserve(8, 20, &random)
	if err != nil || !ok || second == first {
		t.Fatalf("duplicate peer reservation: first=%d second=%d ok=%v err=%v", first, second, ok, err)
	}
	if _, ok, err := budget.Reserve(8, 30, &random); err != nil || ok {
		t.Fatalf("same operation reserved twice from a peer: ok=%t err=%v", ok, err)
	}
	if _, ok, err := budget.Reserve(9, 30, &random); err != nil || !ok {
		t.Fatalf("second per-peer slot unavailable: ok=%t err=%v", ok, err)
	}
	if _, ok, err := budget.Reserve(10, 30, &random); err != nil || !ok {
		t.Fatalf("remaining per-peer slot unavailable: ok=%t err=%v", ok, err)
	}
	if _, ok, err := budget.Reserve(11, 30, &random); err != nil || ok {
		t.Fatalf("budget exceeded capacity: ok=%t err=%v", ok, err)
	}
	if released, err := budget.Complete(8, uint64(2*time.Millisecond)); err != nil || released != 2 || budget.outstanding != 2 {
		t.Fatalf("released=%d outstanding=%d err=%v", released, budget.outstanding, err)
	}
}

func TestJournalRepairBudgetExpiryUpdatesEWMA(t *testing.T) {
	budget, err := newJournalRepairBudget(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	random := newDeterministicRandom(1)
	peer, ok, err := budget.Reserve(protocol.Op(4), 1, &random)
	if err != nil || !ok {
		t.Fatalf("reservation rejected: ok=%t err=%v", ok, err)
	}
	if expired, err := budget.Expire(1 + uint64(2*time.Millisecond) - 1); err != nil || expired != 0 {
		t.Fatalf("expired early=%d err=%v", expired, err)
	}
	if expired, err := budget.Expire(1 + uint64(2*time.Millisecond)); err != nil || expired != 1 {
		t.Fatalf("expired=%d err=%v", expired, err)
	}
	if budget.ewma[peer] != uint64(1200*time.Microsecond) || budget.outstanding != 0 {
		t.Fatalf("ewma=%s outstanding=%d", time.Duration(budget.ewma[peer]), budget.outstanding)
	}
}

func TestJournalRepairBudgetInvariantViolationReturnsError(t *testing.T) {
	budget, err := newJournalRepairBudget(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	budget.outstanding = budget.capacity + 1
	if _, err := budget.Expire(1); !errors.Is(err, ErrReplicaInvariant) {
		t.Fatalf("Expire error = %v, want ErrReplicaInvariant", err)
	}
}

func BenchmarkJournalRepairBudget(b *testing.B) {
	budget, err := newJournalRepairBudget(6, 0)
	if err != nil {
		b.Fatal(err)
	}
	random := newDeterministicRandom(9)
	b.ReportAllocs()
	for index := range b.N {
		op := protocol.Op(index + 1)
		if _, ok, err := budget.Reserve(op, uint64(index+1), &random); err != nil || !ok {
			b.Fatalf("reservation rejected: ok=%t err=%v", ok, err)
		}
		if _, err := budget.Complete(op, uint64(index+2)); err != nil {
			b.Fatal(err)
		}
	}
}
