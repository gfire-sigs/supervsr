package replication

import (
	"errors"
	"math"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

var ErrClockSample = errors.New("replication: invalid cluster clock sample")

type LocalClock interface {
	Wall() uint64
	Monotonic() uint64
}

type clockSample struct {
	valid  bool
	offset int64
	oneWay uint64
}

type clockEndpoint struct {
	value  int64
	lower  bool
	source uint8
}

type ClockInterval struct {
	Lower     int64
	Upper     int64
	Overlap   uint8
	Excluded  uint8
	Tolerance uint64
}

type ClockSynchronizer struct {
	local        LocalClock
	localIndex   protocol.ReplicaIndex
	activeCount  uint8
	quorum       uint8
	toleranceMax uint64
	windowMin    uint64
	windowMax    uint64
	epochMax     uint64
	windowWall   uint64
	windowMono   uint64
	epochMono    uint64
	samples      [MembersMax]clockSample
	endpoints    [MembersMax * 2]clockEndpoint
	installed    ClockInterval
	synchronized bool
}

func NewClockSynchronizer(local LocalClock, localIndex protocol.ReplicaIndex, activeCount, replicationQuorum uint8, process ProcessConfig) (*ClockSynchronizer, error) {
	if local == nil || activeCount == 0 || activeCount > ActiveMax {
		return nil, ErrInvalidConfiguration
	}
	if uint8(localIndex) >= MembersMax || replicationQuorum == 0 || replicationQuorum > activeCount {
		return nil, ErrInvalidConfiguration
	}
	quorum := replicationQuorum
	if uint8(localIndex) >= activeCount {
		quorum++
	}
	clock := &ClockSynchronizer{
		local:        local,
		localIndex:   localIndex,
		activeCount:  activeCount,
		quorum:       quorum,
		toleranceMax: uint64(process.ClockOffsetToleranceMax),
		windowMin:    uint64(process.ClockWindowMin),
		windowMax:    uint64(process.ClockWindowMax),
		epochMax:     uint64(process.ClockEpochMax),
	}
	clock.resetWindow(local.Monotonic(), local.Wall())
	return clock, nil
}

func (clock *ClockSynchronizer) BeginPing() uint64 {
	monotonic := clock.local.Monotonic()
	clock.rotateWindow(monotonic, clock.local.Wall())
	return monotonic
}

func (clock *ClockSynchronizer) Observe(source protocol.ReplicaIndex, pingMonotonic, peerWall, receiveMonotonic uint64) error {
	if uint8(source) >= clock.activeCount || source == clock.localIndex || pingMonotonic > receiveMonotonic || pingMonotonic < clock.windowMono {
		return ErrClockSample
	}
	clock.rotateWindow(receiveMonotonic, clock.local.Wall())
	if pingMonotonic < clock.windowMono || receiveMonotonic-clock.windowMono > clock.windowMax {
		return ErrClockSample
	}
	oneWay := (receiveMonotonic - pingMonotonic) / 2
	localWall := saturatingAdd(clock.windowWall, receiveMonotonic-clock.windowMono)
	peerAtReceive := saturatingAdd(peerWall, oneWay)
	offset := signedDifference(peerAtReceive, localWall)
	sample := &clock.samples[source]
	if !sample.valid || oneWay < sample.oneWay {
		*sample = clockSample{valid: true, offset: offset, oneWay: oneWay}
	}
	clock.install(receiveMonotonic)
	return nil
}

func (clock *ClockSynchronizer) Now() TimeSample {
	monotonic := clock.local.Monotonic()
	wall := clock.local.Wall()
	clock.rotateWindow(monotonic, wall)
	if !clock.synchronized || monotonic < clock.epochMono || monotonic-clock.epochMono > clock.epochMax {
		clock.synchronized = false
		return TimeSample{Wall: wall, Monotonic: monotonic}
	}
	correction := int64(0)
	if correction < clock.installed.Lower {
		correction = clock.installed.Lower
	}
	if correction > clock.installed.Upper {
		correction = clock.installed.Upper
	}
	return TimeSample{Wall: addSigned(wall, correction), Monotonic: monotonic, Synchronized: true}
}

func (clock *ClockSynchronizer) Interval() (ClockInterval, bool) {
	return clock.installed, clock.synchronized
}

func (clock *ClockSynchronizer) install(monotonic uint64) {
	if monotonic-clock.windowMono < clock.windowMin {
		return
	}
	tolerance := clock.toleranceMax
	best, ok := clock.interval(tolerance)
	if !ok {
		return
	}
	for range 64 {
		if tolerance == 0 {
			break
		}
		next := tolerance / 2
		candidate, valid := clock.interval(next)
		if !valid {
			break
		}
		best = candidate
		tolerance = next
	}
	clock.installed = best
	clock.epochMono = monotonic
	clock.synchronized = true
}

func (clock *ClockSynchronizer) interval(tolerance uint64) (ClockInterval, bool) {
	count := 0
	for source := range clock.activeCount {
		sample := clock.samples[source]
		if protocol.ReplicaIndex(source) == clock.localIndex {
			sample = clockSample{valid: true}
		}
		if !sample.valid {
			continue
		}
		radius := saturatingSigned(sample.oneWay, tolerance)
		clock.endpoints[count] = clockEndpoint{value: saturatingSubtractInt64(sample.offset, radius), lower: true, source: source}
		clock.endpoints[count+1] = clockEndpoint{value: saturatingAddInt64(sample.offset, radius), source: source}
		count += 2
	}
	if uint8(clock.localIndex) >= clock.activeCount {
		source := uint8(clock.localIndex)
		radius := saturatingSigned(tolerance)
		clock.endpoints[count] = clockEndpoint{value: -radius, lower: true, source: source}
		clock.endpoints[count+1] = clockEndpoint{value: radius, source: source}
		count += 2
	}
	if uint8(count/2) < clock.quorum {
		return ClockInterval{}, false
	}
	insertionSortEndpoints(clock.endpoints[:count])
	overlap := uint8(0)
	bestOverlap := uint8(0)
	bestLower := int64(0)
	bestUpper := int64(0)
	bestWidth := uint64(math.MaxUint64)
	for index := 0; index+1 < count; index++ {
		if clock.endpoints[index].lower {
			overlap++
		} else {
			overlap--
		}
		lower := clock.endpoints[index].value
		upper := clock.endpoints[index+1].value
		width := signedWidth(lower, upper)
		if overlap > bestOverlap || overlap == bestOverlap && width < bestWidth {
			bestOverlap = overlap
			bestLower = lower
			bestUpper = upper
			bestWidth = width
		}
	}
	if bestOverlap < clock.quorum {
		return ClockInterval{}, false
	}
	return ClockInterval{Lower: bestLower, Upper: bestUpper, Overlap: bestOverlap, Excluded: uint8(count/2) - bestOverlap, Tolerance: tolerance}, true
}

func (clock *ClockSynchronizer) rotateWindow(monotonic, wall uint64) {
	if monotonic >= clock.windowMono && monotonic-clock.windowMono <= clock.windowMax {
		return
	}
	clock.resetWindow(monotonic, wall)
}

func (clock *ClockSynchronizer) resetWindow(monotonic, wall uint64) {
	clock.windowMono = monotonic
	clock.windowWall = wall
	clear(clock.samples[:])
	if uint8(clock.localIndex) < clock.activeCount {
		clock.samples[clock.localIndex] = clockSample{valid: true}
	}
}

func insertionSortEndpoints(endpoints []clockEndpoint) {
	for index := 1; index < len(endpoints); index++ {
		value := endpoints[index]
		position := index
		for position > 0 && endpointLess(value, endpoints[position-1]) {
			endpoints[position] = endpoints[position-1]
			position--
		}
		endpoints[position] = value
	}
}

func endpointLess(left, right clockEndpoint) bool {
	if left.value != right.value {
		return left.value < right.value
	}
	if left.lower != right.lower {
		return left.lower
	}
	return left.source < right.source
}

func signedDifference(left, right uint64) int64 {
	if left >= right {
		difference := left - right
		if difference > math.MaxInt64 {
			return math.MaxInt64
		}
		return int64(difference)
	}
	difference := right - left
	if difference > math.MaxInt64 {
		return math.MinInt64
	}
	return -int64(difference)
}

func saturatingSigned(values ...uint64) int64 {
	total := uint64(0)
	for _, value := range values {
		if math.MaxUint64-total < value {
			return math.MaxInt64
		}
		total += value
	}
	if total > math.MaxInt64 {
		return math.MaxInt64
	}
	return int64(total)
}

func saturatingAdd(left, right uint64) uint64 {
	if math.MaxUint64-left < right {
		return math.MaxUint64
	}
	return left + right
}

func saturatingAddInt64(left, right int64) int64 {
	if right > 0 && left > math.MaxInt64-right {
		return math.MaxInt64
	}
	if right < 0 && left < math.MinInt64-right {
		return math.MinInt64
	}
	return left + right
}

func saturatingSubtractInt64(left, right int64) int64 {
	if right == math.MinInt64 {
		return math.MaxInt64
	}
	return saturatingAddInt64(left, -right)
}

func signedWidth(lower, upper int64) uint64 {
	if upper < lower {
		return math.MaxUint64
	}
	return uint64(upper) - uint64(lower)
}

func addSigned(value uint64, offset int64) uint64 {
	if offset >= 0 {
		return saturatingAdd(value, uint64(offset))
	}
	magnitude := uint64(-(offset + 1)) + 1
	if magnitude > value {
		return 0
	}
	return value - magnitude
}
