package replication

import (
	"errors"
	"sync/atomic"
)

const cacheLineSize = 64

var ErrInvalidRingCapacity = errors.New("replication: ring capacity must be a power of two greater than one")

type ringCursor struct {
	position atomic.Uint64
	_        [cacheLineSize - 8]byte
}

type ringSequence struct {
	value atomic.Uint64
	_     [cacheLineSize - 8]byte
}

// MPSCRing is a bounded, nonblocking multi-producer, single-consumer queue.
// It allocates its slots at construction and retains no references after pop.
type MPSCRing[T any] struct {
	enqueue ringCursor
	dequeue ringCursor

	mask      uint64
	capacity  uint64
	sequences []ringSequence
	values    []T
}

func NewMPSCRing[T any](capacity uint64) (*MPSCRing[T], error) {
	maxInt := uint64(^uint(0) >> 1)
	if capacity < 2 || capacity&(capacity-1) != 0 || capacity > maxInt {
		return nil, ErrInvalidRingCapacity
	}

	ring := &MPSCRing[T]{
		mask:      capacity - 1,
		capacity:  capacity,
		sequences: make([]ringSequence, int(capacity)),
		values:    make([]T, int(capacity)),
	}
	for position := range capacity {
		ring.sequences[position].value.Store(position)
	}
	return ring, nil
}

func (r *MPSCRing[T]) Capacity() uint64 {
	return r.capacity
}

// Len returns an instantaneous upper bound that includes producer-reserved slots.
func (r *MPSCRing[T]) Len() uint64 {
	length := r.enqueue.position.Load() - r.dequeue.position.Load()
	return min(length, r.capacity)
}

func (r *MPSCRing[T]) TryPush(value T) bool {
	position := r.enqueue.position.Load()
	for {
		sequence := r.sequences[position&r.mask].value.Load()
		difference := int64(sequence - position)
		switch {
		case difference == 0:
			if !r.enqueue.position.CompareAndSwap(position, position+1) {
				position = r.enqueue.position.Load()
				continue
			}
			r.values[position&r.mask] = value
			r.sequences[position&r.mask].value.Store(position + 1)
			return true
		case difference < 0:
			return false
		default:
			position = r.enqueue.position.Load()
		}
	}
}

// TryPop writes the oldest value to destination. Only one goroutine may call it.
func (r *MPSCRing[T]) TryPop(destination *T) bool {
	position := r.dequeue.position.Load()
	index := position & r.mask
	sequence := r.sequences[index].value.Load()
	if int64(sequence-(position+1)) != 0 {
		return false
	}

	*destination = r.values[index]
	var zero T
	r.values[index] = zero
	r.sequences[index].value.Store(position + r.capacity)
	r.dequeue.position.Store(position + 1)
	return true
}
