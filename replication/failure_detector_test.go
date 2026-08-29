package replication

import (
	"testing"
	"time"
)

func TestFailureDetectorThresholdBoundaries(t *testing.T) {
	detector := NewFailureDetector(1)
	ewma := uint64(2 * time.Second)
	if level := detector.Level(1 + 3*ewma/2); level != FailureGreen {
		t.Fatalf("green boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma/2 + 1); level != FailureYellow {
		t.Fatalf("yellow boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma); level != FailureYellow {
		t.Fatalf("yellow upper boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma + 1); level != FailureRed {
		t.Fatalf("red boundary = %d", level)
	}
}

func TestFailureDetectorEWMAClampsSignals(t *testing.T) {
	detector := NewFailureDetector(1)
	detector.Signal(2)
	want := time.Duration((4*uint64(2*time.Second) + uint64(100*time.Millisecond)) / 5)
	if detector.EWMA() != want {
		t.Fatalf("minimum-clamped EWMA = %s want %s", detector.EWMA(), want)
	}
	previous := detector.EWMA()
	detector.Signal(2)
	if detector.EWMA() != previous {
		t.Fatalf("stale signal changed EWMA: %s != %s", detector.EWMA(), previous)
	}
}
