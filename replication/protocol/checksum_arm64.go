//go:build arm64

package protocol

import "golang.org/x/sys/cpu"

//go:noescape
func aegisUpdateNEON(state *[8][16]byte, input0, input1 *[16]byte)

func aegisUpdate(state *[8][16]byte, input0, input1 *[16]byte) {
	if cpu.ARM64.HasAES {
		aegisUpdateNEON(state, input0, input1)
		return
	}
	aegisUpdatePortable(state, input0, input1)
}

func HardwareChecksumAvailable() bool {
	return cpu.ARM64.HasAES
}

func ChecksumBackend() string {
	if cpu.ARM64.HasAES {
		return "neon-aes"
	}
	return "portable"
}
