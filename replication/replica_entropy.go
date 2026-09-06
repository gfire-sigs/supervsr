package replication

import (
	"encoding/binary"
	"fmt"
	"io"
)

func newReplicaRandom(entropy io.Reader) (DeterministicRandom, error) {
	if entropy == nil {
		return DeterministicRandom{}, ErrInvalidConfiguration
	}
	var seed [8]byte
	if _, err := io.ReadFull(entropy, seed[:]); err != nil {
		return DeterministicRandom{}, fmt.Errorf("replication: initialize replica entropy: %w", err)
	}
	return NewDeterministicRandom(binary.LittleEndian.Uint64(seed[:])), nil
}
