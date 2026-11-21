package replication

import "time"

type failureLevel uint8

const (
	failureGreen failureLevel = iota
	failureYellow
	failureRed
)

const (
	failureIntervalMin = uint64(100 * time.Millisecond)
	failureIntervalMax = uint64(2 * time.Second)
)

type livenessDetector struct {
	last uint64
	ewma uint64
}

func newLivenessDetector(monotonic uint64) livenessDetector {
	return livenessDetector{last: monotonic, ewma: failureIntervalMax}
}

func (detector *livenessDetector) Signal(monotonic uint64) {
	if monotonic <= detector.last {
		return
	}
	sample := min(failureIntervalMax, max(failureIntervalMin, monotonic-detector.last))
	detector.ewma = saturatingAdd(saturatingMultiply(4, detector.ewma), sample) / 5
	detector.last = monotonic
}

func (detector livenessDetector) Level(monotonic uint64) failureLevel {
	if monotonic <= detector.last {
		return failureGreen
	}
	twiceElapsed := saturatingMultiply(2, monotonic-detector.last)
	threeEWMA := saturatingMultiply(3, detector.ewma)
	if twiceElapsed <= threeEWMA {
		return failureGreen
	}
	if twiceElapsed <= saturatingMultiply(6, detector.ewma) {
		return failureYellow
	}
	return failureRed
}

func (detector livenessDetector) EWMA() time.Duration { return time.Duration(detector.ewma) }
