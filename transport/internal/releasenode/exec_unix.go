//go:build unix

package main

import (
	"os"
	"syscall"
)

func execHandoff(bin string, args []string) error {
	argv := make([]string, 0, len(args)+1)
	argv = append(argv, bin)
	argv = append(argv, args...)
	return syscall.Exec(bin, argv, os.Environ())
}
