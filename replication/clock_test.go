package replication

import (
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestClockSynchronizerFindsTightIntersectingInterval(t *testing.T) {
	raw := &manualLocalClock{wall: 1_000, monotonic: 0}
	process := DefaultProcessConfig()
	clock, err := NewClockSynchronizer(raw, 0, 3, 2, process)
	if err != nil {
		t.Fatal(err)
	}
	if err := clock.Observe(1, uint64(3*time.Second-20*time.Millisecond), uint64(3*time.Second+90*time.Millisecond+1_000), uint64(3*time.Second)); err != nil {
		t.Fatal(err)
	}
	raw.monotonic = uint64(3 * time.Second)
	raw.wall = uint64(3*time.Second) + 1_000
	reading := clock.Now()
	if !reading.Synchronized || reading.Wall <= raw.wall {
		t.Fatalf("reading = %+v local=%d", reading, raw.wall)
	}
	interval, ok := clock.Interval()
	if !ok || interval.Overlap < 2 || interval.Lower > interval.Upper || interval.Tolerance == 0 {
		t.Fatalf("interval = %+v valid=%t", interval, ok)
	}
}

func TestClockSynchronizerStandbyCannotReplaceActiveSource(t *testing.T) {
	raw := &manualLocalClock{wall: 1, monotonic: 0}
	clock, err := NewClockSynchronizer(raw, 3, 3, 2, DefaultProcessConfig())
	if err != nil {
		t.Fatal(err)
	}
	receive := uint64(3 * time.Second)
	ping := receive - uint64(20*time.Millisecond)
	peerWall := receive - uint64(10*time.Millisecond) + 1
	if err := clock.Observe(0, ping, peerWall, receive); err != nil {
		t.Fatal(err)
	}
	raw.monotonic = receive
	raw.wall = receive + 1
	if clock.Now().Synchronized {
		t.Fatal("standby synchronized with only one active source")
	}
	if err := clock.Observe(1, ping, peerWall, receive); err != nil {
		t.Fatal(err)
	}
	if !clock.Now().Synchronized {
		t.Fatal("standby did not synchronize with two active sources plus itself")
	}
}

func TestClockSynchronizerRejectsAndExpiresSamples(t *testing.T) {
	raw := &manualLocalClock{wall: 1, monotonic: 10}
	clock, err := NewClockSynchronizer(raw, 0, 3, 2, DefaultProcessConfig())
	if err != nil {
		t.Fatal(err)
	}
	if err := clock.Observe(1, 11, 1, 10); err == nil {
		t.Fatal("future ping accepted")
	}
	raw.monotonic = 3_000_000_010
	raw.wall = 3_000_000_001
	if err := clock.Observe(1, 2_980_000_010, 2_990_000_001, 3_000_000_010); err != nil {
		t.Fatal(err)
	}
	if !clock.Now().Synchronized {
		t.Fatal("clock did not synchronize")
	}
	raw.monotonic += uint64(DefaultProcessConfig().ClockEpochMax) + 1
	raw.wall += uint64(DefaultProcessConfig().ClockEpochMax) + 1
	if clock.Now().Synchronized {
		t.Fatal("expired clock epoch remained synchronized")
	}
}

func BenchmarkClockSynchronizerNow(b *testing.B) {
	raw := &manualLocalClock{wall: 1, monotonic: 1}
	clock, err := NewClockSynchronizer(raw, protocol.ReplicaIndex(0), 1, 1, DefaultProcessConfig())
	if err != nil {
		b.Fatal(err)
	}
	clock.synchronized = true
	clock.epochMono = 1
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		_ = clock.Now()
	}
}

type manualLocalClock struct {
	wall      uint64
	monotonic uint64
}

func (clock *manualLocalClock) Wall() uint64      { return clock.wall }
func (clock *manualLocalClock) Monotonic() uint64 { return clock.monotonic }
