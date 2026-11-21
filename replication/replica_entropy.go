package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

func newReplicaRandom(entropy io.Reader) (deterministicRandom, error) {
	if entropy == nil {
		return deterministicRandom{}, ErrInvalidConfiguration
	}
	var seed [8]byte
	if _, err := io.ReadFull(entropy, seed[:]); err != nil {
		return deterministicRandom{}, fmt.Errorf("replication: initialize replica entropy: %w", err)
	}
	return newDeterministicRandom(binary.LittleEndian.Uint64(seed[:])), nil
}
