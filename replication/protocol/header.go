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

func EncodeHeader(destination []byte, header *Header) error {
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
	header.HeaderChecksum = ChecksumBytes(destination[HeaderChecksumFrom:])
	copy(destination[:16], header.HeaderChecksum[:])
	return nil
}

func DecodeHeader(source []byte, expectedGroup GroupID, messageSizeMax uint32, memberCount uint8) (Header, RejectReason) {
	if len(source) != HeaderSize {
		return Header{}, RejectHeaderLength
	}
	var expectedChecksum Checksum
	copy(expectedChecksum[:], source[:16])
	if ChecksumBytes(source[HeaderChecksumFrom:]) != expectedChecksum {
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
		return Header{}, RejectCommand
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

func ValidateBody(header *Header, body []byte) RejectReason {
	if uint64(len(body))+HeaderSize != uint64(header.Size) {
		return RejectBodyLength
	}
	if ChecksumBytes(body) != header.BodyChecksum {
		return RejectBodyChecksum
	}
	return RejectNone
}

func DecodeFrame(frame []byte, expectedGroup GroupID, messageSizeMax uint32, memberCount uint8) (Header, []byte, RejectReason) {
	if len(frame) < HeaderSize {
		return Header{}, nil, RejectHeaderLength
	}
	header, reason := DecodeHeader(frame[:HeaderSize], expectedGroup, messageSizeMax, memberCount)
	if reason != RejectNone {
		return Header{}, nil, reason
	}
	if uint64(len(frame)) != uint64(header.Size) {
		return Header{}, nil, RejectBodyLength
	}
	body := frame[HeaderSize:header.Size:header.Size]
	if reason := ValidateBody(&header, body); reason != RejectNone {
		return Header{}, nil, reason
	}
	return header, body, RejectNone
}

func SealFrame(frame []byte, header *Header) error {
	if len(frame) < HeaderSize || len(frame) > int(^uint32(0)) {
		return ErrFrameEncoding
	}
	header.Size = uint32(len(frame))
	header.BodyChecksum = ChecksumBytes(frame[HeaderSize:])
	return EncodeHeader(frame[:HeaderSize], header)
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
