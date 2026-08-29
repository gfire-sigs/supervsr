package replication

import (
	"bytes"
	"errors"
	"path/filepath"
	"runtime"
	"testing"
)

func TestFileStorageDurabilityAndExactIO(t *testing.T) {
	path := filepath.Join(t.TempDir(), "replica.data")
	storage, err := OpenFileStorage(path, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := storage.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})

	if err := storage.Resize(2 * SectorSize); err != nil {
		t.Fatal(err)
	}
	if size, err := storage.Size(); err != nil || size != 2*SectorSize {
		t.Fatalf("size = (%d,%v), want (%d,nil)", size, err, 2*SectorSize)
	}
	written := bytes.Repeat([]byte{0xa5}, SectorSize)
	if err := storage.WriteAt(written, SectorSize); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.SyncParent(); err != nil {
		t.Fatal(err)
	}
	read := make([]byte, SectorSize)
	if err := storage.ReadAt(read, SectorSize); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read, written) {
		t.Fatal("read bytes differ from durable write")
	}
	if err := storage.ReadAt(read, 2*SectorSize); !errors.Is(err, ErrShortIO) {
		t.Fatalf("short read error = %v, want %v", err, ErrShortIO)
	}
}

func TestDirectStorageRequiresAlignedBuffers(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("direct I/O hook unavailable")
	}
	path := filepath.Join(t.TempDir(), "direct.data")
	storage, err := OpenFileStorage(path, true, true)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := storage.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	if err := storage.Resize(SectorSize); err != nil {
		t.Fatal(err)
	}
	aligned, err := NewAlignedBuffer(SectorSize, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt(aligned, 0); err != nil {
		t.Fatal(err)
	}
	if err := storage.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := storage.WriteAt(aligned[1:], 0); !errors.Is(err, ErrStorageAlignment) {
		t.Fatalf("unaligned error = %v, want %v", err, ErrStorageAlignment)
	}
}

func TestAlignedBufferAlignmentAndBounds(t *testing.T) {
	buffer, err := NewAlignedBuffer(SectorSize, SectorSize)
	if err != nil {
		t.Fatal(err)
	}
	storage := &FileStorage{direct: true}
	if err := storage.validateIO(buffer, 0); !errors.Is(err, ErrStorageClosed) {
		t.Fatalf("closed validation error = %v, want %v", err, ErrStorageClosed)
	}
	for _, input := range [][2]uint64{{0, SectorSize}, {SectorSize, 0}, {SectorSize, 3}} {
		if _, err := NewAlignedBuffer(input[0], input[1]); !errors.Is(err, ErrStorageAlignment) {
			t.Fatalf("NewAlignedBuffer(%d,%d) error = %v", input[0], input[1], err)
		}
	}
}
