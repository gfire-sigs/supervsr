package replication

import (
	"errors"
	"math"
	"testing"
)

func TestEWAHCanonicalMarkerVector(t *testing.T) {
	set, err := NewFixedBitSet(6 * 64)
	if err != nil {
		t.Fatal(err)
	}
	set.words[0] = 0
	set.words[1] = 0
	set.words[2] = 0x1234
	set.words[3] = math.MaxUint64
	set.words[4] = math.MaxUint64
	set.words[5] = 0xab
	encoded := make([]uint64, 6)
	count, err := set.EncodeEWAH(encoded)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{0x0000000100000004, 0x1234, 0x0000000100000005, 0xab}
	if count != len(want) {
		t.Fatalf("encoded words = %d want %d", count, len(want))
	}
	for index := range want {
		if encoded[index] != want[index] {
			t.Fatalf("word %d = %016x want %016x", index, encoded[index], want[index])
		}
	}
	decoded, err := NewFixedBitSet(6 * 64)
	if err != nil {
		t.Fatal(err)
	}
	if err := decoded.DecodeEWAH(want); err != nil {
		t.Fatal(err)
	}
	for index := range set.words {
		if decoded.words[index] != set.words[index] {
			t.Fatalf("decoded word %d = %016x want %016x", index, decoded.words[index], set.words[index])
		}
	}
}

func TestEWAHRejectsNoncanonicalLiteralFillAndLengthOverflow(t *testing.T) {
	set, err := NewFixedBitSet(64)
	if err != nil {
		t.Fatal(err)
	}
	if err := set.DecodeEWAH([]uint64{1 << 32, 0}); !errors.Is(err, ErrInvalidEWAH) {
		t.Fatalf("literal fill error = %v", err)
	}
	if _, err := NewFixedBitSet(math.MaxUint64); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("overflow error = %v", err)
	}
}

func TestFixedBitSetTailAndPopulation(t *testing.T) {
	set, err := NewFixedBitSet(65)
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []uint64{0, 63, 64} {
		if !set.Set(index) {
			t.Fatalf("set %d failed", index)
		}
	}
	if set.Count() != 3 || !set.Test(64) || set.Test(65) {
		t.Fatalf("bitset count=%d tail=%t overflow=%t", set.Count(), set.Test(64), set.Test(65))
	}
	set.words[1] |= 1 << 1
	if _, err := set.EncodeEWAH(make([]uint64, 4)); !errors.Is(err, ErrInvalidEWAH) {
		t.Fatalf("tail error = %v", err)
	}
}

func BenchmarkFixedBitSetEncodeEWAH(b *testing.B) {
	set, err := NewFixedBitSet(4096)
	if err != nil {
		b.Fatal(err)
	}
	for index := uint64(0); index < set.Len(); index += 17 {
		set.Set(index)
	}
	destination := make([]uint64, len(set.words)+1)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := set.EncodeEWAH(destination); err != nil {
			b.Fatal(err)
		}
	}
}
