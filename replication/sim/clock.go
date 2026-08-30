package sim

import (
	"math/bits"
	"sync"
	"time"

	"github.com/gfire-sigs/supervsr/replication"
)

const clockRate = uint64(1_000_000)

type Clock struct {
	mu           sync.Mutex
	wall         uint64
	monotonic    uint64
	driftPPM     int64
	remainder    uint64
	synchronized bool
	frozen       bool
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
	if duration <= 0 || clock.frozen {
		return
	}
	multiplier := uint64(int64(clockRate) + clock.driftPPM)
	delta := scaledClockDelta(uint64(duration), multiplier, &clock.remainder)
	clock.wall = saturatingAdd(clock.wall, delta)
	clock.monotonic = saturatingAdd(clock.monotonic, delta)
}

func (clock *Clock) SetDrift(partsPerMillion int64) error {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if partsPerMillion <= -int64(clockRate) || partsPerMillion > int64(clockRate) {
		return replication.ErrInvalidConfiguration
	}
	clock.driftPPM = partsPerMillion
	clock.remainder = 0
	return nil
}

func (clock *Clock) JumpWall(delta time.Duration) error {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	value, ok := jumpClock(clock.wall, int64(delta))
	if !ok {
		return replication.ErrInvalidConfiguration
	}
	clock.wall = value
	return nil
}

func (clock *Clock) JumpMonotonic(delta time.Duration) error {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	value, ok := jumpClock(clock.monotonic, int64(delta))
	if !ok {
		return replication.ErrInvalidConfiguration
	}
	clock.monotonic = value
	return nil
}

func (clock *Clock) SetTime(wall, monotonic uint64) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.wall = wall
	clock.monotonic = monotonic
	clock.remainder = 0
}

func (clock *Clock) Freeze(frozen bool) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.frozen = frozen
}

func (clock *Clock) SetSynchronized(synchronized bool) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.synchronized = synchronized
}

func scaledClockDelta(duration, multiplier uint64, remainder *uint64) uint64 {
	high, low := bits.Mul64(duration, multiplier)
	low, carry := bits.Add64(low, *remainder, 0)
	high, overflow := bits.Add64(high, 0, carry)
	if overflow != 0 || high >= clockRate {
		*remainder = 0
		return ^uint64(0)
	}
	quotient, nextRemainder := bits.Div64(high, low, clockRate)
	*remainder = nextRemainder
	return quotient
}

func jumpClock(value uint64, delta int64) (uint64, bool) {
	if delta >= 0 {
		increase := uint64(delta)
		if ^uint64(0)-value < increase {
			return 0, false
		}
		return value + increase, true
	}
	decrease := uint64(-(delta + 1)) + 1
	if value < decrease {
		return 0, false
	}
	return value - decrease, true
}
