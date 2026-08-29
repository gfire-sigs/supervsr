package replication

import (
	"cmp"
	"errors"
	"math"
	"time"
)

var ErrTimeoutInvariant = errors.New("replication: timeout handler did not reset or stop")

type Timeout struct {
	running   bool
	elapsed   uint64
	threshold uint64
	initial   uint64
	attempts  uint8
	peer      uint8
	measured  uint64
}

func NewTimeout(period, tick time.Duration, peer uint8) (Timeout, error) {
	threshold, ok := durationTicks(period, tick)
	if !ok {
		return Timeout{}, ErrInvalidConfiguration
	}
	return Timeout{initial: threshold, peer: peer}, nil
}

func (timeout *Timeout) Start() {
	timeout.running = true
	timeout.elapsed = 0
	timeout.threshold = timeout.initial
	timeout.attempts = 0
	timeout.measured = 0
}

func (timeout *Timeout) Stop() {
	initial := timeout.initial
	peer := timeout.peer
	*timeout = Timeout{initial: initial, peer: peer}
}

func (timeout *Timeout) Reset() {
	timeout.running = true
	timeout.elapsed = 0
	timeout.threshold = timeout.initial
	timeout.attempts = 0
}

func (timeout *Timeout) Tick() (bool, error) {
	if !timeout.running {
		return false, nil
	}
	if timeout.elapsed >= timeout.threshold {
		return false, ErrTimeoutInvariant
	}
	timeout.elapsed++
	return timeout.elapsed == timeout.threshold, nil
}

func (timeout *Timeout) Backoff(process ProcessConfig, measuredRTT time.Duration, random *DeterministicRandom) error {
	if !timeout.running || random == nil {
		return ErrTimeoutInvariant
	}
	rttTicks, ok := durationTicks(measuredRTT, process.Tick)
	if !ok {
		rttTicks = 1
	}
	maximumRTT, ok := durationTicks(process.MaximumRTT, process.Tick)
	if !ok {
		return ErrInvalidConfiguration
	}
	rttTicks = min(maximumRTT, max(uint64(1), rttTicks))
	minimum, ok := durationTicks(process.BackoffMin, process.Tick)
	if !ok {
		return ErrInvalidConfiguration
	}
	maximum, ok := durationTicks(process.BackoffMax, process.Tick)
	if !ok || maximum < minimum {
		return ErrInvalidConfiguration
	}
	timeout.attempts++
	exponent := min(uint8(63), timeout.attempts)
	span := saturatingShift(max(uint64(1), minimum), exponent)
	span = min(maximum-minimum, span)
	backoff := minimum + random.Uniform(span+1)
	threshold := cmp.Or(saturatingAdd(saturatingMultiply(uint64(process.RTTMultiplier), rttTicks), backoff), uint64(1))
	timeout.elapsed = 0
	timeout.threshold = threshold
	timeout.measured = rttTicks
	return nil
}

func (timeout *Timeout) ResetJitter(random *DeterministicRandom) error {
	if !timeout.running || random == nil {
		return ErrTimeoutInvariant
	}
	timeout.attempts++
	half := max(uint64(1), timeout.initial/2)
	span := saturatingAdd(timeout.initial, 1)
	timeout.elapsed = 0
	timeout.threshold = saturatingAdd(half, random.Uniform(span))
	return nil
}

func (timeout *Timeout) Running() bool     { return timeout.running }
func (timeout *Timeout) Attempts() uint8   { return timeout.attempts }
func (timeout *Timeout) Peer() uint8       { return timeout.peer }
func (timeout *Timeout) Threshold() uint64 { return timeout.threshold }

type DeterministicRandom struct {
	state uint64
}

func NewDeterministicRandom(seed uint64) DeterministicRandom {
	return DeterministicRandom{state: cmp.Or(seed, uint64(0x9e3779b97f4a7c15))}
}

func (random *DeterministicRandom) Next() uint64 {
	value := random.state
	value ^= value >> 12
	value ^= value << 25
	value ^= value >> 27
	random.state = value
	return value * 0x2545f4914f6cdd1d
}

func (random *DeterministicRandom) Uniform(bound uint64) uint64 {
	if bound <= 1 {
		return 0
	}
	threshold := -bound % bound
	for {
		value := random.Next()
		if value >= threshold {
			return value % bound
		}
	}
}

func durationTicks(period, tick time.Duration) (uint64, bool) {
	if period <= 0 || tick <= 0 {
		return 0, false
	}
	ticks := uint64(period / tick)
	return max(uint64(1), ticks), true
}

func saturatingShift(value uint64, exponent uint8) uint64 {
	if exponent >= 64 || value > math.MaxUint64>>exponent {
		return math.MaxUint64
	}
	return value << exponent
}

func saturatingMultiply(left, right uint64) uint64 {
	if left != 0 && right > math.MaxUint64/left {
		return math.MaxUint64
	}
	return left * right
}
