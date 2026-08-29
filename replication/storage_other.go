//go:build !darwin && !linux

package replication

import (
	"errors"
	"os"
)

var errPreallocationUnsupported = errors.New("replication: durable preallocation unsupported on this platform")

func configureDirectIO(_ *os.File) error {
	return ErrStorageAlignment
}

func durableSync(file *os.File) error {
	return file.Sync()
}

func preallocateFile(_ *os.File, _ int64) error {
	return errPreallocationUnsupported
}
