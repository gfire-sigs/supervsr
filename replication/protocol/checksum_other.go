//go:build !amd64 && !arm64

package protocol

func aegisUpdate(state *[8][16]byte, input0, input1 *[16]byte) {
	aegisUpdatePortable(state, input0, input1)
}

// HardwareChecksumAvailable reports whether CPU AES acceleration is active for ChecksumAEGIS128L.
func HardwareChecksumAvailable() bool {
	return false
}

func ChecksumBackend() string {
	return "portable"
}
