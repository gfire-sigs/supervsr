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
			actual, err := ChecksumBytes(ChecksumAEGIS128L, test.input)
			if err != nil {
				t.Fatal(err)
			}
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
	want, err := ChecksumBytes(ChecksumAEGIS128L, input)
	if err != nil {
		t.Fatal(err)
	}

	for chunkSize := 1; chunkSize <= 65; chunkSize++ {
		hasher, err := NewChecksumHasher(ChecksumAEGIS128L)
		if err != nil {
			t.Fatal(err)
		}
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
	hasher, err := NewChecksumHasher(ChecksumAEGIS128L)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hasher.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	first := hasher.Sum()
	if _, err := hasher.Write([]byte(" second")); err != nil {
		t.Fatal(err)
	}
	second := hasher.Sum()
	wantFirst, err := ChecksumBytes(ChecksumAEGIS128L, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	wantSecond, err := ChecksumBytes(ChecksumAEGIS128L, []byte("first second"))
	if err != nil {
		t.Fatal(err)
	}
	if first != wantFirst {
		t.Fatalf("first checksum = %x", first)
	}
	if second != wantSecond {
		t.Fatalf("second checksum = %x", second)
	}
}

func TestChecksumHasherRejectsBitLengthOverflow(t *testing.T) {
	const maxBytes = math.MaxUint64 / 8
	hasher, err := NewChecksumHasher(ChecksumAEGIS128L)
	if err != nil {
		t.Fatal(err)
	}
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

	invalid, err := NewChecksumHasher(ChecksumAEGIS128L)
	if err != nil {
		t.Fatal(err)
	}
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
	var sumErr error
	allocations := testing.AllocsPerRun(10_000, func() {
		if _, err := ChecksumBytes(ChecksumAEGIS128L, input); err != nil {
			sumErr = err
		}
	})
	if sumErr != nil {
		t.Fatal(sumErr)
	}
	if allocations != 0 {
		t.Fatalf("allocations per checksum = %f, want 0", allocations)
	}
}

func BenchmarkChecksum1KiB(b *testing.B) {
	input := make([]byte, 1024)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ChecksumBytes(ChecksumAEGIS128L, input); err != nil {
			b.Fatal(err)
		}
	}
}

func TestWyhash128ChunkingIsCanonical(t *testing.T) {
	input := make([]byte, 4097)
	for index := range input {
		input[index] = byte(index*131 + 17)
	}
	want, err := ChecksumBytes(ChecksumWyhash128, input)
	if err != nil {
		t.Fatal(err)
	}

	for chunkSize := 1; chunkSize <= 65; chunkSize++ {
		hasher, err := NewChecksumHasher(ChecksumWyhash128)
		if err != nil {
			t.Fatal(err)
		}
		for offset := 0; offset < len(input); offset += chunkSize {
			limit := min(offset+chunkSize, len(input))
			if _, err := hasher.Write(input[offset:limit]); err != nil {
				t.Fatalf("chunk size %d: %v", chunkSize, err)
			}
		}
		if got := hasher.Sum(); got != want {
			t.Fatalf("chunk size %d checksum = %x, want %x", chunkSize, got, want)
		}
	}
}

func TestWyhash128SumDoesNotConsumeHasher(t *testing.T) {
	hasher, err := NewChecksumHasher(ChecksumWyhash128)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := hasher.Write([]byte("first")); err != nil {
		t.Fatal(err)
	}
	first := hasher.Sum()
	if _, err := hasher.Write([]byte(" second")); err != nil {
		t.Fatal(err)
	}
	second := hasher.Sum()
	wantFirst, err := ChecksumBytes(ChecksumWyhash128, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	wantSecond, err := ChecksumBytes(ChecksumWyhash128, []byte("first second"))
	if err != nil {
		t.Fatal(err)
	}
	if first != wantFirst {
		t.Fatalf("first checksum = %x, want %x", first, wantFirst)
	}
	if second != wantSecond {
		t.Fatalf("second checksum = %x, want %x", second, wantSecond)
	}
}

func TestWyhash128DistinctNonZeroAndRejectsInvalidAlgorithm(t *testing.T) {
	empty, err := ChecksumBytes(ChecksumWyhash128, nil)
	if err != nil {
		t.Fatal(err)
	}
	if empty.IsZero() {
		t.Fatal("empty wyhash128 checksum unexpectedly zero")
	}
	zeroBytes, err := ChecksumBytes(ChecksumWyhash128, make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	if zeroBytes.IsZero() || zeroBytes == empty {
		t.Fatalf("zeroBytes = %x, empty = %x", zeroBytes, empty)
	}
	flipped := make([]byte, 32)
	flipped[31] = 1
	altered, err := ChecksumBytes(ChecksumWyhash128, flipped)
	if err != nil {
		t.Fatal(err)
	}
	if altered == zeroBytes {
		t.Fatal("single-bit flip produced identical wyhash128 checksum")
	}

	if _, err := ChecksumBytes(ChecksumAlgorithm(99), flipped); !errors.Is(err, ErrInvalidChecksumAlgorithm) {
		t.Fatalf("ChecksumBytes(99) err = %v, want %v", err, ErrInvalidChecksumAlgorithm)
	}
	if _, err := NewChecksumHasher(ChecksumAlgorithm(99)); !errors.Is(err, ErrInvalidChecksumAlgorithm) {
		t.Fatalf("NewChecksumHasher(99) err = %v, want %v", err, ErrInvalidChecksumAlgorithm)
	}
}

func TestWyhash128SteadyStateHasNoAllocations(t *testing.T) {
	input := make([]byte, 1024)
	allocations := testing.AllocsPerRun(10_000, func() {
		if _, err := ChecksumBytes(ChecksumWyhash128, input); err != nil {
			bFail(t, err)
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per wyhash128 checksum = %f, want 0", allocations)
	}
}

func bFail(t *testing.T, err error) {
	t.Helper()
	t.Fatal(err)
}

func BenchmarkChecksumWyhash1281KiB(b *testing.B) {
	input := make([]byte, 1024)
	b.SetBytes(int64(len(input)))
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ChecksumBytes(ChecksumWyhash128, input); err != nil {
			b.Fatal(err)
		}
	}
}
