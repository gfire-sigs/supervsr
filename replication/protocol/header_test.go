package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func testValidationContext() ValidationContext {
	return ValidationContext{
		Authenticated:           true,
		Sender:                  1,
		ActiveCount:             3,
		MemberCount:             3,
		PipelineMax:             8,
		ReleaseHistoryMax:       64,
		ApplicationBatchSizeMax: 1<<20 - HeaderSize,
		ApplicationReplySizeMax: 1<<20 - HeaderSize,
		RepairRequestsMax:       4,
		CurrentRelease:          1,
		ClientReleaseMin:        1,
		Group:                   GroupID{1},
		MessageSizeMax:          1 << 20,
	}
}

func TestFrameRoundTrip(t *testing.T) {
	context := testValidationContext()
	body := []byte("value")
	frame := make([]byte, HeaderSize+len(body))
	copy(frame[HeaderSize:], body)
	header := Header{
		Group:    context.Group,
		Epoch:    0,
		View:     0,
		Release:  1,
		Protocol: ProtocolVersion,
		Command:  CommandRequest,
		Author:   0,
	}
	header.Fields[32] = 9
	binary.LittleEndian.PutUint64(header.Fields[48:56], 1)
	binary.LittleEndian.PutUint32(header.Fields[64:68], 1)
	header.Fields[68] = byte(OperationApplicationMin)
	if err := SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}

	decoded, decodedBody, reason := DecodeFrame(frame, context.Group, context.MessageSizeMax, context.MemberCount)
	if reason != RejectNone {
		t.Fatalf("decode reason = %d", reason)
	}
	if decoded.Command != CommandRequest || decoded.HeaderChecksum != header.HeaderChecksum || decoded.BodyChecksum != header.BodyChecksum {
		t.Fatalf("decoded header = %+v", decoded)
	}
	if string(decodedBody) != "value" {
		t.Fatalf("body = %q, want value", decodedBody)
	}
	if reason := ValidateSemantics(&decoded, decodedBody, context); reason != RejectNone {
		t.Fatalf("semantic reason = %d", reason)
	}
}

func TestHeaderChecksumPrecedesUntrustedSize(t *testing.T) {
	context := testValidationContext()
	frame := makeValidEmptyFrame(t, context, CommandExitView, context.Sender, 0)
	frame[96] ^= 0xff
	if _, _, reason := DecodeFrame(frame, context.Group, context.MessageSizeMax, context.MemberCount); reason != RejectHeaderChecksum {
		t.Fatalf("reason = %d, want %d", reason, RejectHeaderChecksum)
	}
}

func TestFrameRejectsReservedAndCorruptBytes(t *testing.T) {
	context := testValidationContext()
	tests := []struct {
		name   string
		mutate func([]byte)
		want   RejectReason
	}{
		{name: "checksum extension", mutate: func(frame []byte) { frame[16] = 1; resealHeaderChecksum(frame) }, want: RejectChecksumExtension},
		{name: "body checksum extension", mutate: func(frame []byte) { frame[48] = 1; resealHeaderChecksum(frame) }, want: RejectBodyChecksumExtension},
		{name: "reserved nonce", mutate: func(frame []byte) { frame[64] = 1; resealHeaderChecksum(frame) }, want: RejectReservedNonce},
		{name: "frame reservation", mutate: func(frame []byte) { frame[116] = 1; resealHeaderChecksum(frame) }, want: RejectFrameReservation},
		{name: "unknown command", mutate: func(frame []byte) { frame[114] = 255; resealHeaderChecksum(frame) }, want: RejectCommand},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			frame := makeValidEmptyFrame(t, context, CommandExitView, context.Sender, 0)
			test.mutate(frame)
			if _, _, reason := DecodeFrame(frame, context.Group, context.MessageSizeMax, context.MemberCount); reason != test.want {
				t.Fatalf("reason = %d, want %d", reason, test.want)
			}
		})
	}
}

func TestFrameRejectsCorruptBody(t *testing.T) {
	context := testValidationContext()
	frame := make([]byte, HeaderSize+1)
	header := Header{Group: context.Group, Protocol: ProtocolVersion, Command: CommandRequest, Release: 1}
	if err := SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	frame[HeaderSize] ^= 1
	if _, _, reason := DecodeFrame(frame, context.Group, context.MessageSizeMax, context.MemberCount); reason != RejectBodyChecksum {
		t.Fatalf("reason = %d, want %d", reason, RejectBodyChecksum)
	}
}

func TestFramePoolOwnershipAndBackpressure(t *testing.T) {
	pool, err := NewFramePool(2, 1024)
	if err != nil {
		t.Fatal(err)
	}
	first, err := pool.Acquire(4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Acquire(0); !errors.Is(err, ErrFramePoolEmpty) {
		t.Fatalf("third acquire error = %v, want %v", err, ErrFramePoolEmpty)
	}
	body, err := first.Body()
	if err != nil {
		t.Fatal(err)
	}
	copy(body, "test")
	header := Header{Group: GroupID{1}, Protocol: ProtocolVersion, Command: CommandRequest, Release: 1}
	if err := first.Seal(&header); err != nil {
		t.Fatal(err)
	}
	if _, err := first.Body(); !errors.Is(err, ErrFrameSealed) {
		t.Fatalf("sealed body error = %v, want %v", err, ErrFrameSealed)
	}
	if !first.Retain() {
		t.Fatal("retain failed")
	}
	first.Release()
	if pool.Available() != 0 {
		t.Fatalf("available after retained release = %d, want 0", pool.Available())
	}
	second.Release()
	first.Release()
	if pool.Available() != pool.Capacity() {
		t.Fatalf("available = %d, want %d", pool.Available(), pool.Capacity())
	}
}

func TestFramePoolSteadyStateHasNoAllocations(t *testing.T) {
	pool, err := NewFramePool(1, 1024)
	if err != nil {
		t.Fatal(err)
	}
	header := Header{Group: GroupID{1}, Protocol: ProtocolVersion, Command: CommandExitView}
	allocations := testing.AllocsPerRun(10_000, func() {
		frame, acquireErr := pool.Acquire(0)
		if acquireErr != nil {
			panic(acquireErr)
		}
		if sealErr := frame.Seal(&header); sealErr != nil {
			panic(sealErr)
		}
		if _, bytesErr := frame.Bytes(); bytesErr != nil {
			panic(bytesErr)
		}
		frame.Release()
	})
	if allocations != 0 {
		t.Fatalf("allocations = %f, want 0", allocations)
	}
}

func makeValidEmptyFrame(t testing.TB, context ValidationContext, command Command, author ReplicaIndex, release Release) []byte {
	t.Helper()
	frame := make([]byte, HeaderSize)
	header := Header{Group: context.Group, Protocol: ProtocolVersion, Command: command, Author: author, Release: release}
	if err := SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame
}

func resealHeaderChecksum(frame []byte) {
	checksum := ChecksumBytes(frame[HeaderChecksumFrom:HeaderSize])
	copy(frame[:16], checksum[:])
}

func TestFramePoolAcquiresExactEncodedBytes(t *testing.T) {
	pool, err := NewFramePool(1, 4096)
	if err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, HeaderSize+3)
	copy(encoded[HeaderSize:], []byte{1, 2, 3})
	frame, err := pool.AcquireEncoded(encoded)
	if err != nil {
		t.Fatal(err)
	}
	actual, err := frame.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, encoded) {
		t.Fatalf("encoded frame changed")
	}
	frame.Release()
}
