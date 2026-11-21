package protocol

import (
	"encoding/binary"
	"errors"
)

const (
	HeaderSize         = 256
	CommandFieldsSize  = 128
	HeaderChecksumFrom = 16
)

var ErrFrameEncoding = errors.New("protocol: invalid frame encoding")

type RejectReason uint8

const (
	RejectNone RejectReason = iota
	RejectHeaderLength
	RejectHeaderChecksum
	RejectChecksumExtension
	RejectBodyChecksumExtension
	RejectReservedNonce
	RejectWrongGroup
	RejectSize
	RejectProtocol
	RejectEpoch
	RejectCommand
	RejectAuthor
	RejectAuthentication
	RejectFrameReservation
	RejectBodyLength
	RejectBodyChecksum
	RejectCommandFields
	RejectBodyShape
)

// Header holds the 256-byte fixed wire and WAL frame header fields.
type Header struct {
	HeaderChecksum Checksum
	BodyChecksum   Checksum
	Group          GroupID
	Size           uint32
	Epoch          Epoch
	View           View
	Release        Release
	Protocol       uint16
	Command        Command
	Author         ReplicaIndex
	Fields         [CommandFieldsSize]byte
}

// EncodeHeader serializes header into destination (len == HeaderSize) and stamps
// HeaderChecksum using algorithm.
func EncodeHeader(destination []byte, header *Header, algorithm ChecksumAlgorithm) error {
	if len(destination) != HeaderSize || header.Size < HeaderSize || header.Protocol != ProtocolVersion || header.Epoch != 0 || !header.Command.IsDefined() {
		return ErrFrameEncoding
	}
	clear(destination)
	copy(destination[32:48], header.BodyChecksum[:])
	copy(destination[80:96], header.Group[:])
	binary.LittleEndian.PutUint32(destination[96:100], header.Size)
	binary.LittleEndian.PutUint32(destination[100:104], uint32(header.Epoch))
	binary.LittleEndian.PutUint32(destination[104:108], uint32(header.View))
	binary.LittleEndian.PutUint32(destination[108:112], uint32(header.Release))
	binary.LittleEndian.PutUint16(destination[112:114], header.Protocol)
	destination[114] = byte(header.Command)
	destination[115] = byte(header.Author)
	copy(destination[128:256], header.Fields[:])
	checksum, err := ChecksumBytes(algorithm, destination[HeaderChecksumFrom:])
	if err != nil {
		return errors.Join(ErrFrameEncoding, err)
	}
	header.HeaderChecksum = checksum
	copy(destination[:16], header.HeaderChecksum[:])
	return nil
}

// DecodeHeader verifies the header checksum using algorithm, group identity, size,
// and protocol invariants without trusting payload bytes.
func DecodeHeader(source []byte, expectedGroup GroupID, messageSizeMax uint32, memberCount uint8, algorithm ChecksumAlgorithm) (Header, RejectReason) {
	if len(source) != HeaderSize {
		return Header{}, RejectHeaderLength
	}
	var expectedChecksum Checksum
	copy(expectedChecksum[:], source[:16])
	computedChecksum, err := ChecksumBytes(algorithm, source[HeaderChecksumFrom:])
	if err != nil || computedChecksum != expectedChecksum {
		return Header{}, RejectHeaderChecksum
	}
	if !allZero(source[16:32]) {
		return Header{}, RejectChecksumExtension
	}
	if !allZero(source[48:64]) {
		return Header{}, RejectBodyChecksumExtension
	}
	if !allZero(source[64:80]) {
		return Header{}, RejectReservedNonce
	}

	var header Header
	header.HeaderChecksum = expectedChecksum
	copy(header.BodyChecksum[:], source[32:48])
	copy(header.Group[:], source[80:96])
	if header.Group != expectedGroup {
		return Header{}, RejectWrongGroup
	}
	header.Size = binary.LittleEndian.Uint32(source[96:100])
	if header.Size < HeaderSize || header.Size > messageSizeMax {
		return Header{}, RejectSize
	}
	header.Epoch = Epoch(binary.LittleEndian.Uint32(source[100:104]))
	header.View = View(binary.LittleEndian.Uint32(source[104:108]))
	header.Release = Release(binary.LittleEndian.Uint32(source[108:112]))
	header.Protocol = binary.LittleEndian.Uint16(source[112:114])
	header.Command = Command(source[114])
	header.Author = ReplicaIndex(source[115])
	if header.Protocol != ProtocolVersion && !(header.Command == CommandBlock && header.Protocol < ProtocolVersion) {
		return Header{}, RejectProtocol
	}
	if header.Epoch != 0 {
		return Header{}, RejectEpoch
	}
	if !header.Command.IsDefined() {
		return header, RejectCommand
	}
	if memberCount == 0 || uint8(header.Author) >= memberCount {
		return Header{}, RejectAuthor
	}
	if !allZero(source[116:128]) {
		return Header{}, RejectFrameReservation
	}
	copy(header.Fields[:], source[128:256])
	return header, RejectNone
}

// ValidateBody verifies that body matches header.Size and header.BodyChecksum
// under algorithm.
func ValidateBody(header *Header, body []byte, algorithm ChecksumAlgorithm) RejectReason {
	if uint64(len(body))+HeaderSize != uint64(header.Size) {
		return RejectBodyLength
	}
	computedChecksum, err := ChecksumBytes(algorithm, body)
	if err != nil || computedChecksum != header.BodyChecksum {
		return RejectBodyChecksum
	}
	return RejectNone
}

// DecodeFrame validates header and body integrity of a full frame slice and
// returns the decoded Header and payload slice without copying.
func DecodeFrame(frame []byte, expectedGroup GroupID, messageSizeMax uint32, memberCount uint8, algorithm ChecksumAlgorithm) (Header, []byte, RejectReason) {
	if len(frame) < HeaderSize {
		return Header{}, nil, RejectHeaderLength
	}
	header, reason := DecodeHeader(frame[:HeaderSize], expectedGroup, messageSizeMax, memberCount, algorithm)
	if reason != RejectNone {
		return Header{}, nil, reason
	}
	if uint64(len(frame)) != uint64(header.Size) {
		return Header{}, nil, RejectBodyLength
	}
	body := frame[HeaderSize:header.Size:header.Size]
	if reason := ValidateBody(&header, body, algorithm); reason != RejectNone {
		return Header{}, nil, reason
	}
	return header, body, RejectNone
}

// SealFrame populates Size, BodyChecksum, and HeaderChecksum inside frame using algorithm.
func SealFrame(frame []byte, header *Header, algorithm ChecksumAlgorithm) error {
	if len(frame) < HeaderSize || len(frame) > int(^uint32(0)) {
		return ErrFrameEncoding
	}
	checksum, err := ChecksumBytes(algorithm, frame[HeaderSize:])
	if err != nil {
		return errors.Join(ErrFrameEncoding, err)
	}
	header.Size = uint32(len(frame))
	header.BodyChecksum = checksum
	return EncodeHeader(frame[:HeaderSize], header, algorithm)
}

func (command Command) IsDefined() bool {
	return command >= CommandPing && command <= CommandView && command != CommandRetired12 && command != CommandRetired21 && command != CommandRetired22 && command != CommandRetired23
}

func allZero(input []byte) bool {
	var accumulated byte
	for _, value := range input {
		accumulated |= value
	}
	return accumulated == 0
}
