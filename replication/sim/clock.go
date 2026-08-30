package sim

import (
	"sync"
	"time"

	"github.com/gfire-sigs/supervsr/replication"
)

type Clock struct {
	mu           sync.Mutex
	wall         uint64
	monotonic    uint64
	synchronized bool
}

func NewClock() *Clock {
	return &Clock{synchronized: true}
}

func (clock *Clock) Now() replication.TimeSample {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return replication.TimeSample{Wall: clock.wall, Monotonic: clock.monotonic, Synchronized: clock.synchronized}
}

func (clock *Clock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	delta := uint64(duration)
	clock.wall += delta
	clock.monotonic += delta
}

func (clock *Clock) SetSynchronized(synchronized bool) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.synchronized = synchronized
}
