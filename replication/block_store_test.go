package replication

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestBlockStoreWritesAndReadsManifestWithZeroPadding(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, ok := cluster.BlockBase()
	if !ok {
		t.Fatal("block base overflow")
	}
	storage := &crashStorage{}
	if err := storage.Resize(base + cluster.BlockSize); err != nil {
		t.Fatal(err)
	}
	store, err := NewBlockStore(storage, cluster, protocol.GroupID{1}, 1)
	if err != nil {
		t.Fatal(err)
	}
	var metadata [96]byte
	binary.LittleEndian.PutUint32(metadata[40:44], 1)
	body := []byte("manifest-entry")
	reference, err := store.Write(base, 7, protocol.BlockManifest, metadata, body)
	if err != nil {
		t.Fatal(err)
	}
	destination := make([]byte, cluster.BlockSize)
	result, err := store.Read(reference, protocol.BlockManifest, destination)
	if err != nil {
		t.Fatal(err)
	}
	if result.Type != protocol.BlockManifest || result.Snapshot != 7 || result.BodySize != uint32(len(body)) || string(destination[:result.BodySize]) != string(body) {
		t.Fatalf("read result=%+v body=%q", result, destination[:result.BodySize])
	}
	storage.working[base+cluster.BlockSize-1] = 1
	if _, err := store.Read(reference, protocol.BlockManifest, destination); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("padding error = %v", err)
	}
}

func TestBlockStoreRejectsWrongReferenceAndMalformedMetadata(t *testing.T) {
	cluster := compactTestClusterConfig()
	base, _ := cluster.BlockBase()
	storage := &crashStorage{}
	if err := storage.Resize(base + cluster.BlockSize); err != nil {
		t.Fatal(err)
	}
	store, err := NewBlockStore(storage, cluster, protocol.GroupID{2}, 1)
	if err != nil {
		t.Fatal(err)
	}
	var metadata [96]byte
	binary.LittleEndian.PutUint32(metadata[:4], 1)
	binary.LittleEndian.PutUint32(metadata[4:8], 1)
	binary.LittleEndian.PutUint32(metadata[8:12], 3)
	reference, err := store.Write(base, 0, protocol.BlockValue, metadata, []byte{1, 2, 3})
	if err != nil {
		t.Fatal(err)
	}
	wrong := reference
	wrong.Checksum[0] ^= 1
	if _, err := store.Read(wrong, protocol.BlockValue, make([]byte, 3)); !errors.Is(err, ErrBlockMissing) {
		t.Fatalf("wrong reference error = %v", err)
	}
	binary.LittleEndian.PutUint32(metadata[4:8], 2)
	if _, err := store.Write(base, 0, protocol.BlockValue, metadata, []byte{1, 2, 3}); !errors.Is(err, ErrInvalidBlock) {
		t.Fatalf("malformed body error = %v", err)
	}
}
