package protocol

import (
	"encoding/binary"
	"errors"
	"math"
)

var ErrChecksumLengthOverflow = errors.New("protocol: checksum input length overflow")

var aesSBox = [256]byte{
	0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
	0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
	0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
	0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
	0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
	0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
	0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
	0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
	0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
	0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
	0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
	0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
	0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
	0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
	0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
	0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
}

var (
	aegisConstant0 = [16]byte{0xdb, 0x3d, 0x18, 0x55, 0x6d, 0xc2, 0x2f, 0xf1, 0x20, 0x11, 0x31, 0x42, 0x73, 0xb5, 0x28, 0xdd}
	aegisConstant1 = [16]byte{0x00, 0x01, 0x01, 0x02, 0x03, 0x05, 0x08, 0x0d, 0x15, 0x22, 0x37, 0x59, 0x90, 0xe9, 0x79, 0x62}
)

type ChecksumHasher struct {
	state    [8][16]byte
	buffer   [32]byte
	buffered uint8
	length   uint64
}

func NewChecksumHasher() ChecksumHasher {
	var hasher ChecksumHasher
	hasher.Reset()
	return hasher
}

func (hasher *ChecksumHasher) Reset() {
	*hasher = ChecksumHasher{}
	hasher.state[1] = aegisConstant0
	hasher.state[2] = aegisConstant1
	hasher.state[3] = aegisConstant0
	hasher.state[5] = aegisConstant1
	hasher.state[6] = aegisConstant0
	hasher.state[7] = aegisConstant1
	for range 10 {
		hasher.update([16]byte{}, [16]byte{})
	}
}

func (hasher *ChecksumHasher) Write(input []byte) (int, error) {
	if uint64(len(input)) > math.MaxUint64/8-hasher.length {
		return 0, ErrChecksumLengthOverflow
	}
	inputLength := len(input)
	hasher.length += uint64(inputLength)

	if hasher.buffered != 0 {
		needed := 32 - int(hasher.buffered)
		copied := copy(hasher.buffer[int(hasher.buffered):], input)
		hasher.buffered += uint8(copied)
		input = input[copied:]
		if copied < needed {
			return inputLength, nil
		}
		hasher.update(
			[16]byte(hasher.buffer[:16]),
			[16]byte(hasher.buffer[16:]),
		)
		hasher.buffered = 0
	}

	for len(input) >= 32 {
		hasher.update([16]byte(input[:16]), [16]byte(input[16:32]))
		input = input[32:]
	}
	if len(input) != 0 {
		hasher.buffered = uint8(copy(hasher.buffer[:], input))
	}
	return inputLength, nil
}

func (hasher ChecksumHasher) Sum() Checksum {
	if hasher.buffered != 0 {
		clear(hasher.buffer[hasher.buffered:])
		hasher.update([16]byte(hasher.buffer[:16]), [16]byte(hasher.buffer[16:]))
	}

	var lengths [16]byte
	binary.LittleEndian.PutUint64(lengths[:8], hasher.length*8)
	var finalInput [16]byte
	for index := range finalInput {
		finalInput[index] = lengths[index] ^ hasher.state[2][index]
	}
	for range 7 {
		hasher.update(finalInput, finalInput)
	}

	var checksum Checksum
	for lane := range 7 {
		for index := range checksum {
			checksum[index] ^= hasher.state[lane][index]
		}
	}
	return checksum
}

func ChecksumBytes(input []byte) Checksum {
	hasher := NewChecksumHasher()
	_, err := hasher.Write(input)
	if err != nil {
		panic(err)
	}
	return hasher.Sum()
}

func (hasher *ChecksumHasher) update(input0, input1 [16]byte) {
	aegisUpdate(&hasher.state, &input0, &input1)
}

func aegisUpdatePortable(state *[8][16]byte, input0, input1 *[16]byte) {
	old := *state
	state[0] = xorBlock(aesRoundPortable(old[7], old[0]), *input0)
	state[1] = aesRoundPortable(old[0], old[1])
	state[2] = aesRoundPortable(old[1], old[2])
	state[3] = aesRoundPortable(old[2], old[3])
	state[4] = xorBlock(aesRoundPortable(old[3], old[4]), *input1)
	state[5] = aesRoundPortable(old[4], old[5])
	state[6] = aesRoundPortable(old[5], old[6])
	state[7] = aesRoundPortable(old[6], old[7])
}

func aesRoundPortable(state, roundKey [16]byte) [16]byte {
	shifted := [16]byte{
		aesSBox[state[0]], aesSBox[state[5]], aesSBox[state[10]], aesSBox[state[15]],
		aesSBox[state[4]], aesSBox[state[9]], aesSBox[state[14]], aesSBox[state[3]],
		aesSBox[state[8]], aesSBox[state[13]], aesSBox[state[2]], aesSBox[state[7]],
		aesSBox[state[12]], aesSBox[state[1]], aesSBox[state[6]], aesSBox[state[11]],
	}
	var output [16]byte
	for column := range 4 {
		offset := column * 4
		a0 := shifted[offset]
		a1 := shifted[offset+1]
		a2 := shifted[offset+2]
		a3 := shifted[offset+3]
		xorAll := a0 ^ a1 ^ a2 ^ a3
		output[offset] = a0 ^ xorAll ^ xtime(a0^a1) ^ roundKey[offset]
		output[offset+1] = a1 ^ xorAll ^ xtime(a1^a2) ^ roundKey[offset+1]
		output[offset+2] = a2 ^ xorAll ^ xtime(a2^a3) ^ roundKey[offset+2]
		output[offset+3] = a3 ^ xorAll ^ xtime(a3^a0) ^ roundKey[offset+3]
	}
	return output
}

func xtime(value byte) byte {
	return value<<1 ^ byte(int8(value)>>7)&0x1b
}

func xorBlock(left, right [16]byte) [16]byte {
	for index := range left {
		left[index] ^= right[index]
	}
	return left
}
