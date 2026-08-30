package replication

import (
	"bytes"
	"flag"
	"os"
	"testing"
)

var updateStorageGolden = flag.Bool("update-storage-golden", false, "update canonical durable format fixtures")

func TestCanonicalSuperblockFixture(t *testing.T) {
	validation, superblock := validSuperblockFixture(t)
	actual := make([]byte, int(validation.Cluster.SuperblockCopies)*SuperblockBytes)
	candidates := make([]SuperblockCandidate, 0, validation.Cluster.SuperblockCopies)
	for index := range uint16(validation.Cluster.SuperblockCopies) {
		encoded := actual[int(index)*SuperblockBytes : int(index+1)*SuperblockBytes]
		if err := superblock.Encode(encoded, index, validation); err != nil {
			t.Fatal(err)
		}
		candidate, err := DecodeSuperblock(encoded, index, validation)
		if err != nil {
			t.Fatal(err)
		}
		candidates = append(candidates, candidate)
	}
	const fixturePath = "testdata/superblocks.golden"
	if *updateStorageGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixturePath, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatal("canonical durable format fixture changed")
	}
	decoded := make([]SuperblockCandidate, 0, validation.Cluster.SuperblockCopies)
	for index := range uint16(validation.Cluster.SuperblockCopies) {
		encoded := expected[int(index)*SuperblockBytes : int(index+1)*SuperblockBytes]
		candidate, err := DecodeSuperblock(encoded, index, validation)
		if err != nil {
			t.Fatalf("decode fixture copy %d: %v", index, err)
		}
		decoded = append(decoded, candidate)
	}
	selected, err := SelectSuperblock(decoded, validation.Cluster.SuperblockCopies)
	if err != nil {
		t.Fatal(err)
	}
	if selected.Superblock != superblock {
		t.Fatal("decoded durable fixture changed state")
	}
}
