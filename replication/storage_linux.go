//go:build linux

package replication

import (
	"os"

	"golang.org/x/sys/unix"
)

func configureDirectIO(file *os.File) error {
	flags, err := unix.FcntlInt(file.Fd(), unix.F_GETFL, 0)
	if err != nil {
		return err
	}
	_, err = unix.FcntlInt(file.Fd(), unix.F_SETFL, flags|unix.O_DIRECT)
	return err
}

func durableSync(file *os.File) error {
	return file.Sync()
}

func preallocateFile(file *os.File, size int64) error {
	return unix.Fallocate(int(file.Fd()), 0, 0, size)
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
