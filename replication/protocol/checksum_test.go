package protocol

import (
	"encoding/hex"
	"errors"
	"math"
	"testing"
)

func TestChecksumVectors(t *testing.T) {
	tests := []struct {
		name  string
		input []byte
		want  string
	}{
		{name: "empty", want: "83cc600dc4e3e7e62d4055826174f149"},
		{name: "sixteen zero bytes", input: make([]byte, 16), want: "f72ad48dd05dd1656133101cd4be3a26"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual := ChecksumBytes(test.input)
			if hex.EncodeToString(actual[:]) != test.want {
				t.Fatalf("checksum = %x, want %s", actual, test.want)
			}
		})
	}
}

func TestChecksumChunkingIsCanonical(t *testing.T) {
	input := make([]byte, 4097)
	for index := range input {
		input[index] = byte(index*131 + 17)
	}
	want := ChecksumBytes(input)

	for chunkSize := 1; chunkSize <= 65; chunkSize++ {
		hasher := NewChecksumHasher()
		for offset := 0; offset < len(input); offset += chunkSize {
			limit := min(offset+chunkSize, len(input))
			if _, err := hasher.Write(input[offset:limit]); err != nil {
				t.Fatalf("chunk size %d: %v", chunkSize, err)
			}
		}
		if actual := hasher.Sum(); actual != want {
			t.Fatalf("chunk size %d: checksum = %x, want %x", chunkSize, actual, want)
		}
	}
}

func TestChecksumBackendMatchesPortable(t *testing.T) {
	var state [8][16]byte
	for lane := range state {
		for index := range state[lane] {
			state[lane][index] = byte(lane*31 + index*17)
		}
	}
	input0 := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9}
	input1 := [16]byte{10, 11, 12, 13, 14, 15, 16}
	want := state
	aegisUpdatePortable(&want, &input0, &input1)
	aegisUpdate(&state, &input0, &input1)
	if state != want {
		t.Fatalf("%s update = %x, want %x", ChecksumBackend(), state, want)
	}
}

func TestChecksumSumDoesNotConsumeHasher(t *testing.T) {
	hasher := NewChecksumHasher()
	if _, err := hasher.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	first := hasher.Sum()
	if _, err := hasher.Write([]byte(" second")); err != nil {
		t.Fatal(err)
	}
	second := hasher.Sum()
	if first != ChecksumBytes([]byte("first")) {
		t.Fatalf("first checksum = %x", first)
	}
	if second != ChecksumBytes([]byte("first second")) {
		t.Fatalf("second checksum = %x", second)
	}
}

func TestChecksumHasherRejectsBitLengthOverflow(t *testing.T) {
	const maxBytes = math.MaxUint64 / 8
	hasher := NewChecksumHasher()
	hasher.length = maxBytes - 1
	if written, err := hasher.Write([]byte{1}); err != nil || written != 1 {
		t.Fatalf("boundary write = (%d, %v), want (1, nil)", written, err)
	}
	if hasher.length != maxBytes {
		t.Fatalf("length = %d, want %d", hasher.length, uint64(maxBytes))
	}
	before := hasher
	if written, err := hasher.Write([]byte{2}); !errors.Is(err, ErrChecksumLengthOverflow) || written != 0 {
		t.Fatalf("overflow write = (%d, %v), want (0, %v)", written, err, ErrChecksumLengthOverflow)
	}
	if hasher != before {
		t.Fatal("overflow write changed hasher state")
	}

	invalid := NewChecksumHasher()
	invalid.length = maxBytes + 1
	before = invalid
	if written, err := invalid.Write(nil); !errors.Is(err, ErrChecksumLengthOverflow) || written != 0 {
		t.Fatalf("invalid-state write = (%d, %v), want (0, %v)", written, err, ErrChecksumLengthOverflow)
	}
	if invalid != before {
		t.Fatal("invalid-state write changed hasher state")
	}
}

func TestChecksumSteadyStateHasNoAllocations(t *testing.T) {
	input := make([]byte, 1024)
	allocations := testing.AllocsPerRun(10_000, func() {
		_ = ChecksumBytes(input)
	})
	if allocations != 0 {
		t.Fatalf("allocations per checksum = %f, want 0", allocations)
	}
}

func BenchmarkChecksum1KiB(b *testing.B) {
	input := make([]byte, 1024)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		_ = ChecksumBytes(input)
	}
}
