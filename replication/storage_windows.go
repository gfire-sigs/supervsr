//go:build windows

package replication

import (
	"errors"
	"os"
	"unsafe"

	"golang.org/x/sys/windows"
)

func openStorageFile(path string, create, direct bool) (*os.File, error) {
	if !direct {
		return openStandardStorageFile(path, create, 0)
	}
	pathPtr, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	createmode := uint32(windows.OPEN_EXISTING)
	if create {
		createmode = windows.OPEN_ALWAYS
	}
	handle, err := windows.CreateFile(
		pathPtr,
		windows.GENERIC_READ|windows.GENERIC_WRITE,
		windows.FILE_SHARE_READ|windows.FILE_SHARE_WRITE,
		nil,
		createmode,
		windows.FILE_ATTRIBUTE_NORMAL|windows.FILE_FLAG_NO_BUFFERING|windows.FILE_FLAG_WRITE_THROUGH,
		0,
	)
	if err != nil {
		return nil, err
	}
	return os.NewFile(uintptr(handle), path), nil
}

func durableSync(file *os.File) error {
	return file.Sync()
}

func preallocateFile(file *os.File, size int64) error {
	if size <= 0 {
		return nil
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() >= size {
		return nil
	}
	raw, err := file.SyscallConn()
	if err != nil {
		return err
	}
	var allocErr error
	controlErr := raw.Control(func(fd uintptr) {
		alloc := size
		allocErr = windows.SetFileInformationByHandle(
			windows.Handle(fd),
			windows.FileAllocationInfo,
			(*byte)(unsafe.Pointer(&alloc)),
			uint32(unsafe.Sizeof(alloc)),
		)
	})
	if controlErr != nil {
		return controlErr
	}
	if allocErr != nil {
		if errors.Is(allocErr, windows.ERROR_NOT_SUPPORTED) || errors.Is(allocErr, windows.ERROR_INVALID_FUNCTION) {
			return nil
		}
		return allocErr
	}
	return nil
}

func syncParentDir(_ string) error {
	return nil
}
