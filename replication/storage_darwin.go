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
