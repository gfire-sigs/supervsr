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
