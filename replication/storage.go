package replication

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"unsafe"
)

var (
	ErrStorage          = errors.New("replication: storage failure")
	ErrStorageAlignment = errors.New("replication: unaligned direct I/O")
	ErrStorageClosed    = errors.New("replication: storage closed")
	ErrShortIO          = errors.New("replication: short storage I/O")
)

type Storage interface {
	ReadAt(buffer []byte, offset uint64) error
	WriteAt(buffer []byte, offset uint64) error
	Sync() error
	Resize(size uint64) error
	Size() (uint64, error)
	SyncParent() error
	Close() error
}

type FileStorage struct {
	file   *os.File
	path   string
	direct bool

	closeOnce sync.Once
	closeErr  error
}

func OpenFileStorage(path string, create, direct bool) (*FileStorage, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0o600)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s: %w", ErrStorage, path, err)
	}
	if direct {
		if err := configureDirectIO(file); err != nil {
			_ = file.Close()
			return nil, fmt.Errorf("%w: direct I/O %s: %w", ErrStorage, path, err)
		}
	}
	return &FileStorage{file: file, path: path, direct: direct}, nil
}

func (storage *FileStorage) ReadAt(buffer []byte, offset uint64) error {
	if err := storage.validateIO(buffer, offset); err != nil {
		return err
	}
	read, err := storage.file.ReadAt(buffer, int64(offset))
	if err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: read offset %d: %w", ErrStorage, offset, err)
	}
	if read != len(buffer) {
		return fmt.Errorf("%w: read offset %d: got %d, want %d", ErrShortIO, offset, read, len(buffer))
	}
	return nil
}

func (storage *FileStorage) WriteAt(buffer []byte, offset uint64) error {
	if err := storage.validateIO(buffer, offset); err != nil {
		return err
	}
	written, err := storage.file.WriteAt(buffer, int64(offset))
	if err != nil {
		return fmt.Errorf("%w: write offset %d: %w", ErrStorage, offset, err)
	}
	if written != len(buffer) {
		return fmt.Errorf("%w: write offset %d: got %d, want %d", ErrShortIO, offset, written, len(buffer))
	}
	return nil
}

func (storage *FileStorage) Sync() error {
	if storage.file == nil {
		return ErrStorageClosed
	}
	if err := durableSync(storage.file); err != nil {
		return fmt.Errorf("%w: sync %s: %w", ErrStorage, storage.path, err)
	}
	return nil
}

func (storage *FileStorage) Resize(size uint64) error {
	if storage.file == nil {
		return ErrStorageClosed
	}
	if size > uint64(^uint64(0)>>1) {
		return fmt.Errorf("%w: size %d exceeds int64", ErrStorage, size)
	}
	if err := preallocateFile(storage.file, int64(size)); err != nil {
		return fmt.Errorf("%w: preallocate %s: %w", ErrStorage, storage.path, err)
	}
	if err := storage.file.Truncate(int64(size)); err != nil {
		return fmt.Errorf("%w: resize %s: %w", ErrStorage, storage.path, err)
	}
	return nil
}

func (storage *FileStorage) Size() (uint64, error) {
	if storage.file == nil {
		return 0, ErrStorageClosed
	}
	info, err := storage.file.Stat()
	if err != nil {
		return 0, fmt.Errorf("%w: stat %s: %w", ErrStorage, storage.path, err)
	}
	return uint64(info.Size()), nil
}

func (storage *FileStorage) SyncParent() error {
	directory, err := os.Open(filepath.Dir(storage.path))
	if err != nil {
		return fmt.Errorf("%w: open parent %s: %w", ErrStorage, storage.path, err)
	}
	syncErr := directory.Sync()
	closeErr := directory.Close()
	if syncErr != nil || closeErr != nil {
		return fmt.Errorf("%w: sync parent %s: %w", ErrStorage, storage.path, errors.Join(syncErr, closeErr))
	}
	return nil
}

func (storage *FileStorage) Close() error {
	storage.closeOnce.Do(func() {
		if storage.file == nil {
			return
		}
		storage.closeErr = storage.file.Close()
		storage.file = nil
	})
	if storage.closeErr != nil {
		return fmt.Errorf("%w: close %s: %w", ErrStorage, storage.path, storage.closeErr)
	}
	return nil
}

func (storage *FileStorage) validateIO(buffer []byte, offset uint64) error {
	if storage.file == nil {
		return ErrStorageClosed
	}
	if offset > uint64(^uint64(0)>>1) {
		return fmt.Errorf("%w: offset %d exceeds int64", ErrStorage, offset)
	}
	if !storage.direct || len(buffer) == 0 {
		return nil
	}
	address := uintptr(unsafe.Pointer(&buffer[0]))
	if offset%SectorSize != 0 || uint64(len(buffer))%SectorSize != 0 || address%SectorSize != 0 {
		return ErrStorageAlignment
	}
	return nil
}

func NewAlignedBuffer(size, alignment uint64) ([]byte, error) {
	if size == 0 || alignment == 0 || alignment&(alignment-1) != 0 || size > uint64(int(^uint(0)>>1))-alignment {
		return nil, ErrStorageAlignment
	}
	allocation := make([]byte, int(size+alignment-1))
	address := uintptr(unsafe.Pointer(&allocation[0]))
	offset := (uintptr(alignment) - address%uintptr(alignment)) % uintptr(alignment)
	return allocation[offset : offset+uintptr(size) : offset+uintptr(size)], nil
}
