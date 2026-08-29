package replication

import (
	"errors"
	"runtime"
	"sync"
	"testing"
)

func TestMPSCRingRejectsInvalidCapacity(t *testing.T) {
	for _, capacity := range []uint64{0, 1, 3, 6, 1 << 63} {
		_, err := NewMPSCRing[uint64](capacity)
		if !errors.Is(err, ErrInvalidRingCapacity) {
			t.Fatalf("capacity %d: error = %v, want %v", capacity, err, ErrInvalidRingCapacity)
		}
	}
}

func TestMPSCRingPreservesFIFOAcrossWrap(t *testing.T) {
	ring, err := NewMPSCRing[uint64](4)
	if err != nil {
		t.Fatal(err)
	}
	if ring.Capacity() != 4 {
		t.Fatalf("capacity = %d, want 4", ring.Capacity())
	}

	for value := uint64(1); value <= 4; value++ {
		if !ring.TryPush(value) {
			t.Fatalf("push %d failed", value)
		}
	}
	if ring.TryPush(5) {
		t.Fatal("push to full ring succeeded")
	}
	if ring.Len() != 4 {
		t.Fatalf("full length = %d, want 4", ring.Len())
	}

	for expected := uint64(1); expected <= 2; expected++ {
		var actual uint64
		if !ring.TryPop(&actual) {
			t.Fatalf("pop %d failed", expected)
		}
		if actual != expected {
			t.Fatalf("pop = %d, want %d", actual, expected)
		}
	}
	for value := uint64(5); value <= 6; value++ {
		if !ring.TryPush(value) {
			t.Fatalf("wrapped push %d failed", value)
		}
	}
	for expected := uint64(3); expected <= 6; expected++ {
		var actual uint64
		if !ring.TryPop(&actual) {
			t.Fatalf("wrapped pop %d failed", expected)
		}
		if actual != expected {
			t.Fatalf("wrapped pop = %d, want %d", actual, expected)
		}
	}
	var value uint64
	if ring.TryPop(&value) {
		t.Fatal("pop from empty ring succeeded")
	}
	if ring.Len() != 0 {
		t.Fatalf("empty length = %d, want 0", ring.Len())
	}
}

func TestMPSCRingReleasesPoppedReference(t *testing.T) {
	ring, err := NewMPSCRing[*int](2)
	if err != nil {
		t.Fatal(err)
	}
	value := 7
	if !ring.TryPush(&value) {
		t.Fatal("push failed")
	}
	var actual *int
	if !ring.TryPop(&actual) {
		t.Fatal("pop failed")
	}
	if actual != &value {
		t.Fatalf("pop = %p, want %p", actual, &value)
	}
	if ring.values[0] != nil {
		t.Fatal("popped slot retained its pointer")
	}
}

func TestMPSCRingConcurrentProducers(t *testing.T) {
	const (
		producerCount     = 8
		valuesPerProducer = 20_000
	)
	ring, err := NewMPSCRing[uint64](1024)
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	var producers sync.WaitGroup
	producers.Add(producerCount)
	for producer := range producerCount {
		go func() {
			defer producers.Done()
			<-start
			for sequence := range valuesPerProducer {
				value := uint64(producer)<<32 | uint64(sequence)
				for !ring.TryPush(value) {
					runtime.Gosched()
				}
			}
		}()
	}
	close(start)

	last := make([]int, producerCount)
	for index := range last {
		last[index] = -1
	}
	for count := 0; count < producerCount*valuesPerProducer; {
		var value uint64
		if !ring.TryPop(&value) {
			runtime.Gosched()
			continue
		}
		producer := int(value >> 32)
		sequence := int(uint32(value))
		if producer < 0 || producer >= producerCount {
			t.Fatalf("producer = %d, want [0,%d)", producer, producerCount)
		}
		if sequence != last[producer]+1 {
			t.Fatalf("producer %d sequence = %d, want %d", producer, sequence, last[producer]+1)
		}
		last[producer] = sequence
		count++
	}
	producers.Wait()
}

func TestMPSCRingSteadyStateHasNoAllocations(t *testing.T) {
	ring, err := NewMPSCRing[uint64](2)
	if err != nil {
		t.Fatal(err)
	}
	var value uint64
	allocations := testing.AllocsPerRun(10_000, func() {
		if !ring.TryPush(42) || !ring.TryPop(&value) {
			panic("ring operation failed")
		}
	})
	if allocations != 0 {
		t.Fatalf("allocations per push/pop = %f, want 0", allocations)
	}
}

func BenchmarkMPSCRingPushPop(b *testing.B) {
	ring, err := NewMPSCRing[uint64](1024)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	var value uint64
	for b.Loop() {
		if !ring.TryPush(1) || !ring.TryPop(&value) {
			b.Fatal("ring operation failed")
		}
	}
}
