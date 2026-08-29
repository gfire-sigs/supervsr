package replication

import "time"

type FailureLevel uint8

const (
	FailureGreen FailureLevel = iota
	FailureYellow
	FailureRed
)

const (
	failureIntervalMin = uint64(100 * time.Millisecond)
	failureIntervalMax = uint64(2 * time.Second)
)

type FailureDetector struct {
	last uint64
	ewma uint64
}

func NewFailureDetector(monotonic uint64) FailureDetector {
	return FailureDetector{last: monotonic, ewma: failureIntervalMax}
}

func (detector *FailureDetector) Signal(monotonic uint64) {
	if monotonic <= detector.last {
		return
	}
	sample := min(failureIntervalMax, max(failureIntervalMin, monotonic-detector.last))
	detector.ewma = saturatingAdd(saturatingMultiply(4, detector.ewma), sample) / 5
	detector.last = monotonic
}

func (detector FailureDetector) Level(monotonic uint64) FailureLevel {
	if monotonic <= detector.last {
		return FailureGreen
	}
	twiceElapsed := saturatingMultiply(2, monotonic-detector.last)
	threeEWMA := saturatingMultiply(3, detector.ewma)
	if twiceElapsed <= threeEWMA {
		return FailureGreen
	}
	if twiceElapsed <= saturatingMultiply(6, detector.ewma) {
		return FailureYellow
	}
	return FailureRed
}

func (detector FailureDetector) EWMA() time.Duration { return time.Duration(detector.ewma) }
