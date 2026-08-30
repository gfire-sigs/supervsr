package replication

import (
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestBlockRepairBudgetRetryExpiryAndFulfillment(t *testing.T) {
	budget, err := newBlockRepairBudget(3, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	first := BlockReference{Address: 1, Checksum: protocol.Checksum{1}}
	second := BlockReference{Address: 2, Checksum: protocol.Checksum{2}}
	if !budget.Reserve(1, []BlockReference{first, second}, 1) {
		t.Fatal("initial batch rejected")
	}
	if budget.Reserve(1, []BlockReference{first}, uint64(200*time.Millisecond)) {
		t.Fatal("duplicate peer request accepted")
	}
	if budget.Reserve(2, []BlockReference{first}, uint64(50*time.Millisecond)) {
		t.Fatal("early cross-peer retry accepted")
	}
	if !budget.Reserve(2, []BlockReference{first}, uint64(101*time.Millisecond)) {
		t.Fatal("eligible cross-peer retry rejected")
	}
	if released := budget.Fulfill(first); released != 2 {
		t.Fatalf("released = %d, want 2", released)
	}
	if budget.Outstanding(first) {
		t.Fatal("fulfilled reference remains outstanding")
	}
	if expired := budget.Expire(uint64(251 * time.Millisecond)); expired != 1 {
		t.Fatalf("expired = %d, want 1", expired)
	}
	if !budget.valid() {
		t.Fatal("budget invariant failed")
	}
}

func TestBlockRepairBudgetDestinationRequiresFullBatch(t *testing.T) {
	budget, err := newBlockRepairBudget(3, 0, 2)
	if err != nil {
		t.Fatal(err)
	}
	references := []BlockReference{
		{Address: 1, Checksum: protocol.Checksum{1}},
		{Address: 2, Checksum: protocol.Checksum{2}},
	}
	if !budget.Reserve(1, references, 1) {
		t.Fatal("reservation rejected")
	}
	random := NewDeterministicRandom(1)
	for range 32 {
		peer, ok := budget.Destination(&random)
		if !ok || peer != 2 {
			t.Fatalf("destination = %d, %t; want peer 2", peer, ok)
		}
	}
}

func BenchmarkBlockRepairBudget(b *testing.B) {
	budget, err := newBlockRepairBudget(6, 0, 4)
	if err != nil {
		b.Fatal(err)
	}
	random := NewDeterministicRandom(1)
	reference := BlockReference{Address: 1, Checksum: protocol.Checksum{1}}
	b.ReportAllocs()
	b.ResetTimer()
	for index := 0; index < b.N; index++ {
		peer, ok := budget.Destination(&random)
		if !ok || !budget.Reserve(peer, []BlockReference{reference}, uint64(index)*blockRepairExpires) {
			b.Fatal("budget operation failed")
		}
		budget.Fulfill(reference)
	}
}
