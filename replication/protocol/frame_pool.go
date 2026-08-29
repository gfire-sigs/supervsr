package protocol

import (
	"errors"
	"math"
	"sync/atomic"
)

var (
	ErrInvalidFramePool = errors.New("protocol: invalid frame pool configuration")
	ErrFramePoolEmpty   = errors.New("protocol: frame pool exhausted")
	ErrFrameTooLarge    = errors.New("protocol: frame exceeds configured maximum")
	ErrFrameSealed      = errors.New("protocol: frame is immutable")
)

type FramePool struct {
	head           atomic.Uint64
	available      atomic.Uint32
	messageSizeMax uint32
	frames         []Frame
	storage        []byte
}

type Frame struct {
	pool   *FramePool
	buffer []byte
	index  uint32
	next   atomic.Uint32
	refs   atomic.Int32
	sealed atomic.Bool
	size   uint32
}

func NewFramePool(count, messageSizeMax uint32) (*FramePool, error) {
	if count == 0 || count == math.MaxUint32 || messageSizeMax < HeaderSize {
		return nil, ErrInvalidFramePool
	}
	storageSize := uint64(count) * uint64(messageSizeMax)
	if storageSize > uint64(int(^uint(0)>>1)) {
		return nil, ErrInvalidFramePool
	}

	pool := &FramePool{
		messageSizeMax: messageSizeMax,
		frames:         make([]Frame, int(count)),
		storage:        make([]byte, int(storageSize)),
	}
	for index := range pool.frames {
		start := index * int(messageSizeMax)
		frame := &pool.frames[index]
		frame.pool = pool
		frame.buffer = pool.storage[start : start+int(messageSizeMax) : start+int(messageSizeMax)]
		frame.index = uint32(index)
		if index+1 < len(pool.frames) {
			frame.next.Store(uint32(index + 2))
		}
	}
	pool.head.Store(1)
	pool.available.Store(count)
	return pool, nil
}

func (pool *FramePool) Acquire(bodySize uint32) (*Frame, error) {
	if uint64(bodySize)+HeaderSize > uint64(pool.messageSizeMax) {
		return nil, ErrFrameTooLarge
	}
	frame := pool.pop()
	if frame == nil {
		return nil, ErrFramePoolEmpty
	}
	frame.size = bodySize + HeaderSize
	frame.sealed.Store(false)
	frame.refs.Store(1)
	clear(frame.buffer[:frame.size])
	return frame, nil
}

func (pool *FramePool) Available() uint32 {
	return pool.available.Load()
}

func (pool *FramePool) Capacity() uint32 {
	return uint32(len(pool.frames))
}

func (pool *FramePool) MessageSizeMax() uint32 {
	return pool.messageSizeMax
}

func (frame *Frame) Body() ([]byte, error) {
	if frame.sealed.Load() || frame.refs.Load() <= 0 {
		return nil, ErrFrameSealed
	}
	return frame.buffer[HeaderSize:frame.size:frame.size], nil
}

func (frame *Frame) ResizeBody(bodySize uint32) error {
	if frame.sealed.Load() || frame.refs.Load() != 1 {
		return ErrFrameSealed
	}
	if uint64(bodySize)+HeaderSize > uint64(frame.pool.messageSizeMax) {
		return ErrFrameTooLarge
	}
	frame.size = bodySize + HeaderSize
	return nil
}

func (frame *Frame) Seal(header *Header) error {
	if frame.sealed.Load() || frame.refs.Load() != 1 {
		return ErrFrameSealed
	}
	if err := SealFrame(frame.buffer[:frame.size], header); err != nil {
		return err
	}
	frame.sealed.Store(true)
	return nil
}

func (frame *Frame) Bytes() ([]byte, error) {
	if !frame.sealed.Load() || frame.refs.Load() <= 0 {
		return nil, ErrFrameEncoding
	}
	return frame.buffer[:frame.size:frame.size], nil
}

func (frame *Frame) Retain() bool {
	if !frame.sealed.Load() {
		return false
	}
	for {
		references := frame.refs.Load()
		if references <= 0 || references == math.MaxInt32 {
			return false
		}
		if frame.refs.CompareAndSwap(references, references+1) {
			return true
		}
	}
}

func (frame *Frame) Release() {
	remaining := frame.refs.Add(-1)
	if remaining > 0 {
		return
	}
	if remaining < 0 {
		panic("protocol: frame reference underflow")
	}
	frame.sealed.Store(false)
	frame.size = 0
	frame.pool.push(frame)
}

func (pool *FramePool) pop() *Frame {
	for {
		head := pool.head.Load()
		indexPlusOne := uint32(head)
		if indexPlusOne == 0 {
			return nil
		}
		frame := &pool.frames[indexPlusOne-1]
		next := frame.next.Load()
		tag := uint32(head>>32) + 1
		if pool.head.CompareAndSwap(head, uint64(tag)<<32|uint64(next)) {
			pool.available.Add(^uint32(0))
			return frame
		}
	}
}

func (pool *FramePool) push(frame *Frame) {
	for {
		head := pool.head.Load()
		frame.next.Store(uint32(head))
		tag := uint32(head>>32) + 1
		if pool.head.CompareAndSwap(head, uint64(tag)<<32|uint64(frame.index+1)) {
			pool.available.Add(1)
			return
		}
	}
}
