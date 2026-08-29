package replication

import (
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestJournalRepairBudgetCapsPeersAndReleasesOperation(t *testing.T) {
	budget, err := newJournalRepairBudget(3, 0)
	if err != nil {
		t.Fatal(err)
	}
	random := NewDeterministicRandom(7)
	first, ok := budget.Reserve(8, 10, &random)
	if !ok {
		t.Fatal("first reservation rejected")
	}
	second, ok := budget.Reserve(8, 20, &random)
	if !ok || second == first {
		t.Fatalf("duplicate peer reservation: first=%d second=%d ok=%v", first, second, ok)
	}
	if _, ok := budget.Reserve(8, 30, &random); ok {
		t.Fatal("same operation reserved twice from a peer")
	}
	if _, ok := budget.Reserve(9, 30, &random); !ok {
		t.Fatal("second per-peer slot unavailable")
	}
	if _, ok := budget.Reserve(10, 30, &random); !ok {
		t.Fatal("remaining per-peer slot unavailable")
	}
	if _, ok := budget.Reserve(11, 30, &random); ok {
		t.Fatal("budget exceeded capacity")
	}
	if released := budget.Complete(8, uint64(2*time.Millisecond)); released != 2 || budget.outstanding != 2 {
		t.Fatalf("released=%d outstanding=%d", released, budget.outstanding)
	}
}

func TestJournalRepairBudgetExpiryUpdatesEWMA(t *testing.T) {
	budget, err := newJournalRepairBudget(2, 0)
	if err != nil {
		t.Fatal(err)
	}
	random := NewDeterministicRandom(1)
	peer, ok := budget.Reserve(protocol.Op(4), 1, &random)
	if !ok {
		t.Fatal("reservation rejected")
	}
	if expired := budget.Expire(1 + uint64(2*time.Millisecond) - 1); expired != 0 {
		t.Fatalf("expired early=%d", expired)
	}
	if expired := budget.Expire(1 + uint64(2*time.Millisecond)); expired != 1 {
		t.Fatalf("expired=%d", expired)
	}
	if budget.ewma[peer] != uint64(1200*time.Microsecond) || budget.outstanding != 0 {
		t.Fatalf("ewma=%s outstanding=%d", time.Duration(budget.ewma[peer]), budget.outstanding)
	}
}

func BenchmarkJournalRepairBudget(b *testing.B) {
	budget, err := newJournalRepairBudget(6, 0)
	if err != nil {
		b.Fatal(err)
	}
	random := NewDeterministicRandom(9)
	b.ReportAllocs()
	for index := range b.N {
		op := protocol.Op(index + 1)
		if _, ok := budget.Reserve(op, uint64(index+1), &random); !ok {
			b.Fatal("reservation rejected")
		}
		budget.Complete(op, uint64(index+2))
	}
}
