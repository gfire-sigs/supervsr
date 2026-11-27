package replication

import (
	"math/bits"
	"time"
)

type failureLevel uint8

const (
	failureGreen failureLevel = iota
	failureYellow
	failureRed
)

type livenessDetector struct {
	last           uint64
	ewma           uint64
	intervalMin    uint64
	intervalMax    uint64
	yellowPermille uint64
	redPermille    uint64
}

func newLivenessDetector(monotonic uint64) livenessDetector {
	return newLivenessDetectorFromProcess(monotonic, ProcessConfig{})
}

func newLivenessDetectorFromProcess(monotonic uint64, process ProcessConfig) livenessDetector {
	resolved := process.resolvedTimers()
	return livenessDetector{
		last:           monotonic,
		ewma:           uint64(resolved.failureIntervalMax),
		intervalMin:    uint64(resolved.failureIntervalMin),
		intervalMax:    uint64(resolved.failureIntervalMax),
		yellowPermille: uint64(resolved.failureYellowPermille),
		redPermille:    uint64(resolved.failureRedPermille),
	}
}

func (detector *livenessDetector) Signal(monotonic uint64) {
	if monotonic <= detector.last {
		return
	}
	intervalMin := detector.intervalMin
	intervalMax := detector.intervalMax
	if intervalMax == 0 {
		intervalMin = uint64(defaultFailureIntervalMin)
		intervalMax = uint64(defaultFailureIntervalMax)
	}
	sample := min(intervalMax, max(intervalMin, monotonic-detector.last))
	detector.ewma = saturatingAdd(saturatingMultiply(4, detector.ewma), sample) / 5
	detector.last = monotonic
}

func (detector livenessDetector) Level(monotonic uint64) failureLevel {
	if monotonic <= detector.last {
		return failureGreen
	}
	yellow := detector.yellowPermille
	red := detector.redPermille
	if yellow == 0 || red == 0 {
		yellow = uint64(defaultFailureYellowPermille)
		red = uint64(defaultFailureRedPermille)
	}
	elapsed := monotonic - detector.last
	if scaledLessOrEqual(elapsed, 1000, detector.ewma, yellow) {
		return failureGreen
	}
	if scaledLessOrEqual(elapsed, 1000, detector.ewma, red) {
		return failureYellow
	}
	return failureRed
}

func (detector livenessDetector) EWMA() time.Duration { return time.Duration(detector.ewma) }

func scaledLessOrEqual(a, b, c, d uint64) bool {
	hi1, lo1 := bits.Mul64(a, b)
	hi2, lo2 := bits.Mul64(c, d)
	return hi1 < hi2 || (hi1 == hi2 && lo1 <= lo2)
}
