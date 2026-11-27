package replication

import (
	"errors"
	"math"
)

var errInvalidEWAH = errors.New("replication: invalid EWAH bitset")

const (
	ewahFillMax    = uint64((1 << 31) - 1)
	ewahLiteralMax = uint64(math.MaxUint32)
)

type fixedBitSet struct {
	words  []uint64
	length uint64
}

func newFixedBitSet(length uint64) (fixedBitSet, error) {
	if length > math.MaxUint64-63 {
		return fixedBitSet{}, ErrInvalidConfiguration
	}
	wordCount := (length + 63) / 64
	if wordCount > uint64(int(^uint(0)>>1)) {
		return fixedBitSet{}, ErrInvalidConfiguration
	}
	return fixedBitSet{words: make([]uint64, int(wordCount)), length: length}, nil
}
func (set *fixedBitSet) grow(length uint64) error {
	if length <= set.length {
		return nil
	}
	if length > math.MaxUint64-63 {
		return ErrInvalidConfiguration
	}
	wordCount := (length + 63) / 64
	if wordCount > uint64(int(^uint(0)>>1)) {
		return ErrInvalidConfiguration
	}
	words := make([]uint64, int(wordCount))
	copy(words, set.words)
	set.words = words
	set.length = length
	return nil
}

func (set *fixedBitSet) Len() uint64 { return set.length }

func (set *fixedBitSet) Test(index uint64) bool {
	return index < set.length && set.words[index/64]&(uint64(1)<<(index%64)) != 0
}

func (set *fixedBitSet) Set(index uint64) bool {
	if index >= set.length {
		return false
	}
	set.words[index/64] |= uint64(1) << (index % 64)
	return true
}

func (set *fixedBitSet) Clear(index uint64) bool {
	if index >= set.length {
		return false
	}
	set.words[index/64] &^= uint64(1) << (index % 64)
	return true
}

func (set fixedBitSet) Count() uint64 {
	var count uint64
	for _, word := range set.words {
		count += uint64(populationCount(word))
	}
	return count
}

func (set *fixedBitSet) EncodeEWAH(destination []uint64) (int, error) {
	if !set.validTail() {
		return 0, errInvalidEWAH
	}
	written := 0
	for cursor := 0; cursor < len(set.words); {
		fill := uint64(0)
		fillWord := uint64(0)
		if set.words[cursor] == 0 || set.words[cursor] == math.MaxUint64 {
			fillWord = set.words[cursor]
			for cursor < len(set.words) && set.words[cursor] == fillWord && fill < ewahFillMax {
				fill++
				cursor++
			}
		}
		literalStart := cursor
		for cursor < len(set.words) && uint64(cursor-literalStart) < ewahLiteralMax {
			if set.words[cursor] == 0 || set.words[cursor] == math.MaxUint64 {
				break
			}
			cursor++
		}
		literals := uint64(cursor - literalStart)
		if fill == 0 && literals == 0 {
			continue
		}
		if written >= len(destination) || literals > uint64(len(destination)-written-1) {
			return 0, errInvalidEWAH
		}
		marker := fill << 1
		if fillWord == math.MaxUint64 {
			marker |= 1
		}
		marker |= literals << 32
		destination[written] = marker
		written++
		copy(destination[written:written+int(literals)], set.words[literalStart:cursor])
		written += int(literals)
	}
	return written, nil
}

func (set *fixedBitSet) DecodeEWAH(encoded []uint64) error {
	clear(set.words)
	cursor := 0
	for offset := 0; offset < len(encoded); {
		marker := encoded[offset]
		offset++
		fill := (marker >> 1) & ewahFillMax
		literals := marker >> 32
		if fill == 0 && literals == 0 {
			return errInvalidEWAH
		}
		if literals > uint64(len(encoded)-offset) || fill > uint64(len(set.words)-cursor) {
			return errInvalidEWAH
		}
		fillWord := uint64(0)
		if marker&1 != 0 {
			fillWord = math.MaxUint64
		}
		for range fill {
			set.words[cursor] = fillWord
			cursor++
		}
		if literals > uint64(len(set.words)-cursor) {
			return errInvalidEWAH
		}
		for _, literal := range encoded[offset : offset+int(literals)] {
			if literal == 0 || literal == math.MaxUint64 {
				return errInvalidEWAH
			}
			set.words[cursor] = literal
			cursor++
		}
		offset += int(literals)
	}
	if cursor != len(set.words) || !set.validTail() {
		return errInvalidEWAH
	}
	return nil
}

func (set *fixedBitSet) validTail() bool {
	if set.length == 0 || set.length%64 == 0 {
		return true
	}
	mask := uint64(1)<<(set.length%64) - 1
	return set.words[len(set.words)-1]&^mask == 0
}

func populationCount(value uint64) uint8 {
	value -= value >> 1 & 0x5555555555555555
	value = value&0x3333333333333333 + value>>2&0x3333333333333333
	value = (value + value>>4) & 0x0f0f0f0f0f0f0f0f
	return uint8(value * 0x0101010101010101 >> 56)
}
