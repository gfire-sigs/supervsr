//go:build !darwin && !linux && !windows

package replication

import (
	"os"
)

func openStorageFile(path string, create, direct bool) (*os.File, error) {
	if direct {
		return nil, ErrStorageAlignment
	}
	return openStandardStorageFile(path, create, 0)
}

func durableSync(file *os.File) error {
	return file.Sync()
}

func preallocateFile(file *os.File, size int64) error {
	return zeroExtendFile(file, size)
}

func syncParentDir(dir string) error {
	return syncParentDirectoryFD(dir)
}
