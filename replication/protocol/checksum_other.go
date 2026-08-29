//go:build !amd64 && !arm64

package protocol

func aegisUpdate(state *[8][16]byte, input0, input1 *[16]byte) {
	aegisUpdatePortable(state, input0, input1)
}

func HardwareChecksumAvailable() bool {
	return false
}

func ChecksumBackend() string {
	return "portable"
}
