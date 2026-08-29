//go:build amd64 && goexperiment.simd

package protocol

import (
	"simd/archsimd"

	"golang.org/x/sys/cpu"
)

func aegisUpdate(state *[8][16]byte, input0, input1 *[16]byte) {
	if !HardwareChecksumAvailable() {
		aegisUpdatePortable(state, input0, input1)
		return
	}

	state0 := archsimd.LoadUint8x16Array(&state[0])
	state1 := archsimd.LoadUint8x16Array(&state[1])
	state2 := archsimd.LoadUint8x16Array(&state[2])
	state3 := archsimd.LoadUint8x16Array(&state[3])
	state4 := archsimd.LoadUint8x16Array(&state[4])
	state5 := archsimd.LoadUint8x16Array(&state[5])
	state6 := archsimd.LoadUint8x16Array(&state[6])
	state7 := archsimd.LoadUint8x16Array(&state[7])

	result0 := state7.AESEncryptOneRound(state0.ReshapeToUint32s())
	result1 := state0.AESEncryptOneRound(state1.ReshapeToUint32s())
	result2 := state1.AESEncryptOneRound(state2.ReshapeToUint32s())
	result3 := state2.AESEncryptOneRound(state3.ReshapeToUint32s())
	result4 := state3.AESEncryptOneRound(state4.ReshapeToUint32s())
	result5 := state4.AESEncryptOneRound(state5.ReshapeToUint32s())
	result6 := state5.AESEncryptOneRound(state6.ReshapeToUint32s())
	result7 := state6.AESEncryptOneRound(state7.ReshapeToUint32s())

	result0 = result0.Xor(archsimd.LoadUint8x16Array(input0))
	result4 = result4.Xor(archsimd.LoadUint8x16Array(input1))
	result0.StoreArray(&state[0])
	result1.StoreArray(&state[1])
	result2.StoreArray(&state[2])
	result3.StoreArray(&state[3])
	result4.StoreArray(&state[4])
	result5.StoreArray(&state[5])
	result6.StoreArray(&state[6])
	result7.StoreArray(&state[7])
}

func HardwareChecksumAvailable() bool {
	return cpu.X86.HasAES && cpu.X86.HasAVX
}

func ChecksumBackend() string {
	if HardwareChecksumAvailable() {
		return "go1.27-simd-vaes"
	}
	return "portable"
}
