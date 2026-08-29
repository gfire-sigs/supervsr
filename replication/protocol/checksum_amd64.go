//go:build amd64 && !goexperiment.simd

package protocol

import "golang.org/x/sys/cpu"

//go:noescape
func aegisUpdateAVX(state *[8][16]byte, input0, input1 *[16]byte)

func aegisUpdate(state *[8][16]byte, input0, input1 *[16]byte) {
	if cpu.X86.HasAES && cpu.X86.HasAVX {
		aegisUpdateAVX(state, input0, input1)
		return
	}
	aegisUpdatePortable(state, input0, input1)
}

func HardwareChecksumAvailable() bool {
	return cpu.X86.HasAES && cpu.X86.HasAVX
}

func ChecksumBackend() string {
	if HardwareChecksumAvailable() {
		return "avx-aes"
	}
	return "portable"
}
