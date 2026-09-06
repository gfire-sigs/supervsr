package sim

import (
	"bytes"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication"
)

func TestStorageFaultWaitsForMatchingOperation(t *testing.T) {
	storage := NewStorage()
	if err := storage.Resize(4); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("safe"), 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.Arm(StorageFault{At: storage.NextOperation(), Kind: replication.IOWrite, Effect: FaultFail}); err != nil {
		t.Fatal(err)
	}
	var body [4]byte
	if err := storage.ReadAt(body[:], 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt([]byte("lost"), 0); !errors.Is(err, ErrInjectedFault) {
		t.Fatalf("selected write error = %v, want injected fault", err)
	}
	if err := storage.ReadAt(body[:], 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body[:], []byte("safe")) {
		t.Fatalf("failed write changed durable data to %q", body)
	}
	if err := storage.WriteAt([]byte("next"), 0); err != nil {
		t.Fatalf("one-shot fault affected a later write: %v", err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	storage.Crash()
	if err := storage.ReadAt(body[:], 0); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(body[:], []byte("next")) {
		t.Fatalf("later successful write recovered %q, want next", body)
	}
}
