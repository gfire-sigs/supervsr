package replication

import (
	"testing"
	"time"
)

func TestFailureDetectorThresholdBoundaries(t *testing.T) {
	detector := newLivenessDetector(1)
	ewma := uint64(2 * time.Second)
	if level := detector.Level(1 + 3*ewma/2); level != failureGreen {
		t.Fatalf("green boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma/2 + 1); level != failureYellow {
		t.Fatalf("yellow boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma); level != failureYellow {
		t.Fatalf("yellow upper boundary = %d", level)
	}
	if level := detector.Level(1 + 3*ewma + 1); level != failureRed {
		t.Fatalf("red boundary = %d", level)
	}
}

func TestFailureDetectorEWMAClampsSignals(t *testing.T) {
	detector := newLivenessDetector(1)
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

func TestFailureDetectorCustomWANBoundsAndPermilleThresholds(t *testing.T) {
	process := DefaultProcessConfig()
	process.FailureIntervalMin = 500 * time.Millisecond
	process.FailureIntervalMax = 6 * time.Second
	process.FailureYellowPermille = 2000
	process.FailureRedPermille = 5000

	detector := newLivenessDetectorFromProcess(1, process)
	if detector.EWMA() != 6*time.Second {
		t.Fatalf("initial EWMA = %s, want 6s", detector.EWMA())
	}
	ewma := uint64(6 * time.Second)
	if level := detector.Level(1 + 2*ewma); level != failureGreen {
		t.Fatalf("green boundary at 2.0x = %d, want green", level)
	}
	if level := detector.Level(1 + 2*ewma + 1); level != failureYellow {
		t.Fatalf("yellow boundary above 2.0x = %d, want yellow", level)
	}
	if level := detector.Level(1 + 5*ewma); level != failureYellow {
		t.Fatalf("yellow upper boundary at 5.0x = %d, want yellow", level)
	}
	if level := detector.Level(1 + 5*ewma + 1); level != failureRed {
		t.Fatalf("red boundary above 5.0x = %d, want red", level)
	}

	// Lower clamp uses configured 500ms minimum.
	detector.Signal(2)
	want := time.Duration((4*uint64(6*time.Second) + uint64(500*time.Millisecond)) / 5)
	if detector.EWMA() != want {
		t.Fatalf("clamped EWMA = %s, want %s", detector.EWMA(), want)
	}

	// Extreme elapsed monotonic values must not overflow into a false Green/Yellow result.
	if level := detector.Level(^uint64(0)); level != failureRed {
		t.Fatalf("max monotonic level = %d, want red", level)
	}
}
