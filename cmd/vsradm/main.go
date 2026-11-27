// Command vsradm manages VSR replica storage files safely across cluster
// bootstrap, read-only superblock inspection, and fenced lost-replica
// replacement.
package main

import (
	"context"
	"fmt"
	"os"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "vsradm: %v\n", err)
		os.Exit(1)
	}
}
