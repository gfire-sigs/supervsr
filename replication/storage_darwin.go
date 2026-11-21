//go:build darwin

package replication

import (
	"os"

	"golang.org/x/sys/unix"
)

func configureDirectIO(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_NOCACHE, 1)
	return err
}

func durableSync(file *os.File) error {
	_, err := unix.FcntlInt(file.Fd(), unix.F_FULLFSYNC, 0)
	return err
}

func preallocateFile(file *os.File, size int64) error {
	allocation := unix.Fstore_t{
		Flags:   unix.F_ALLOCATEALL,
		Posmode: unix.F_PEOFPOSMODE,
		Offset:  0,
		Length:  size,
	}
	return unix.FcntlFstore(file.Fd(), unix.F_PREALLOCATE, &allocation)
}

func openStorageFile(path string, create, direct bool) (*os.File, error) {
	file, err := openStandardStorageFile(path, create, 0)
	if err != nil {
		return nil, err
	}
	if direct {
		if err := configureDirectIO(file); err != nil {
			_ = file.Close()
			return nil, err
		}
	}
	return file, nil
}

func syncParentDir(dir string) error {
	return syncParentDirectoryFD(dir)
}
