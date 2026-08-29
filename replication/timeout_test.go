package replication

import (
	"errors"
	"testing"
	"time"
)

func TestTimeoutRequiresResetAfterFire(t *testing.T) {
	timeout, err := NewTimeout(30*time.Millisecond, 10*time.Millisecond, 2)
	if err != nil {
		t.Fatal(err)
	}
	timeout.Start()
	for tick := 1; tick <= 3; tick++ {
		fired, tickErr := timeout.Tick()
		if tickErr != nil {
			t.Fatal(tickErr)
		}
		if fired != (tick == 3) {
			t.Fatalf("tick %d fired=%t", tick, fired)
		}
	}
	if _, err := timeout.Tick(); !errors.Is(err, ErrTimeoutInvariant) {
		t.Fatalf("late tick error = %v", err)
	}
	timeout.Reset()
	if timeout.Attempts() != 0 || timeout.Threshold() != 3 {
		t.Fatalf("reset timeout = %+v", timeout)
	}
}

func TestTimeoutBackoffIsBoundedAndDeterministic(t *testing.T) {
	process := DefaultProcessConfig()
	first, err := NewTimeout(250*time.Millisecond, process.Tick, 1)
	if err != nil {
		t.Fatal(err)
	}
	second := first
	first.Start()
	second.Start()
	randomA := NewDeterministicRandom(17)
	randomB := NewDeterministicRandom(17)
	for attempt := 1; attempt <= 255; attempt++ {
		if err := first.Backoff(process, process.MaximumRTT*2, &randomA); err != nil {
			t.Fatal(err)
		}
		if err := second.Backoff(process, process.MaximumRTT*2, &randomB); err != nil {
			t.Fatal(err)
		}
		if first.Threshold() != second.Threshold() {
			t.Fatalf("attempt %d diverged: %d != %d", attempt, first.Threshold(), second.Threshold())
		}
		minimumRTT := uint64(process.RTTMultiplier) * uint64(process.MaximumRTT/process.Tick)
		minimumBackoff := uint64(process.BackoffMin / process.Tick)
		maximumBackoff := uint64(process.BackoffMax / process.Tick)
		if first.Threshold() < minimumRTT+minimumBackoff || first.Threshold() > minimumRTT+maximumBackoff {
			t.Fatalf("attempt %d threshold %d outside bounds", attempt, first.Threshold())
		}
	}
	if first.Attempts() != 255 {
		t.Fatalf("attempts = %d", first.Attempts())
	}
	if err := first.Backoff(process, process.InitialRTT, &randomA); err != nil {
		t.Fatal(err)
	}
	if first.Attempts() != 0 {
		t.Fatalf("attempt counter did not wrap: %d", first.Attempts())
	}
}

func TestTimeoutFixedJitterStaysWithinHalfToOneAndHalfPeriods(t *testing.T) {
	timeout, err := NewTimeout(time.Second, 10*time.Millisecond, 0)
	if err != nil {
		t.Fatal(err)
	}
	timeout.Start()
	random := NewDeterministicRandom(23)
	for range 1000 {
		if err := timeout.ResetJitter(&random); err != nil {
			t.Fatal(err)
		}
		if timeout.Threshold() < 50 || timeout.Threshold() > 150 {
			t.Fatalf("jitter threshold = %d", timeout.Threshold())
		}
	}
}

func BenchmarkTimeoutBackoff(b *testing.B) {
	process := DefaultProcessConfig()
	timeout, err := NewTimeout(250*time.Millisecond, process.Tick, 0)
	if err != nil {
		b.Fatal(err)
	}
	timeout.Start()
	random := NewDeterministicRandom(31)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := timeout.Backoff(process, process.InitialRTT, &random); err != nil {
			b.Fatal(err)
		}
	}
}
