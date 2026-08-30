package protocol

import (
	"encoding/binary"
	"testing"
)

func TestCommandAuthorRules(t *testing.T) {
	actualSender := []Command{
		CommandPing, CommandPong, CommandClientPong, CommandPrepareOK, CommandExitView,
		CommandJoinView, CommandGetView, CommandGetHeaders, CommandGetPrepare, CommandGetReply,
		CommandHeaders, CommandEviction, CommandGetBlocks,
	}
	for _, command := range actualSender {
		if !validAuthor(command, 2, 2, true, 1) || validAuthor(command, 1, 2, true, 1) || validAuthor(command, 2, 2, false, 1) {
			t.Fatalf("command %d actual-sender author rule failed", command)
		}
	}
	for _, command := range []Command{CommandRequest, CommandClientPing, CommandBlock} {
		if !validAuthor(command, 0, 2, true, 1) || validAuthor(command, 2, 2, true, 1) {
			t.Fatalf("command %d zero-author rule failed", command)
		}
	}
	for _, command := range []Command{CommandPrepare, CommandReply, CommandCommit, CommandView} {
		if !validAuthor(command, 1, 2, true, 1) || validAuthor(command, 2, 2, true, 1) {
			t.Fatalf("command %d primary-author rule failed", command)
		}
	}
}
func TestClientSourceCannotSendReplicaControlCommands(t *testing.T) {
	context := testValidationContext()
	context.ReplicaSource = false
	clientPing := Header{Command: CommandClientPing, Release: 1}
	clientPing.Fields[0] = 1
	binary.LittleEndian.PutUint64(clientPing.Fields[16:24], 1)
	if reason := ValidateSemantics(&clientPing, nil, context); reason != RejectNone {
		t.Fatalf("client ping reason = %d, want %d", reason, RejectNone)
	}
	exitView := Header{Command: CommandExitView, Author: context.Sender}
	if reason := ValidateSemantics(&exitView, nil, context); reason != RejectAuthentication {
		t.Fatalf("replica control reason = %d, want %d", reason, RejectAuthentication)
	}
}

func TestCommandReleaseRules(t *testing.T) {
	context := testValidationContext()
	for _, command := range []Command{CommandRequest, CommandReply, CommandPing, CommandPong, CommandClientPing, CommandClientPong, CommandEviction, CommandBlock} {
		header := Header{Command: command, Release: 1}
		if !validRelease(&header, context) {
			t.Fatalf("command %d rejected nonzero release", command)
		}
		header.Release = 0
		if validRelease(&header, context) {
			t.Fatalf("command %d accepted zero release", command)
		}
	}
	for _, command := range []Command{CommandCommit, CommandExitView, CommandJoinView, CommandView, CommandGetView, CommandGetHeaders, CommandGetPrepare, CommandGetReply, CommandHeaders, CommandGetBlocks} {
		header := Header{Command: command}
		if !validRelease(&header, context) {
			t.Fatalf("command %d rejected zero release", command)
		}
		header.Release = 1
		if validRelease(&header, context) {
			t.Fatalf("command %d accepted nonzero release", command)
		}
	}
}

func TestRegistrationPrepareValidationDoesNotReadPastRequestNumber(t *testing.T) {
	context := testValidationContext()
	header := Header{
		Group:    context.Group,
		View:     0,
		Release:  1,
		Protocol: ProtocolVersion,
		Command:  CommandPrepare,
		Author:   0,
	}
	header.Fields[80] = 7
	binary.LittleEndian.PutUint64(header.Fields[96:104], 1)
	binary.LittleEndian.PutUint64(header.Fields[112:120], 1)
	header.Fields[124] = byte(OperationRegister)
	body := make([]byte, 256)
	binary.LittleEndian.PutUint32(body[:4], context.ApplicationBatchSizeMax)
	if reason := ValidateSemantics(&header, body, context); reason != RejectNone {
		t.Fatalf("reason = %d, want %d", reason, RejectNone)
	}
}

func TestPingReleaseHistoryValidation(t *testing.T) {
	context := testValidationContext()
	header := Header{Command: CommandPing, Author: context.Sender, Release: 1}
	binary.LittleEndian.PutUint64(header.Fields[24:32], 100)
	binary.LittleEndian.PutUint16(header.Fields[32:34], 2)
	body := make([]byte, int(context.ReleaseHistoryMax)*4)
	binary.LittleEndian.PutUint32(body[:4], 1)
	binary.LittleEndian.PutUint32(body[4:8], 2)
	if reason := ValidateSemantics(&header, body, context); reason != RejectNone {
		t.Fatalf("reason = %d, want %d", reason, RejectNone)
	}

	binary.LittleEndian.PutUint32(body[4:8], 1)
	if reason := ValidateSemantics(&header, body, context); reason != RejectBodyShape {
		t.Fatalf("duplicate release reason = %d, want %d", reason, RejectBodyShape)
	}
}

func FuzzSemanticValidationDoesNotPanic(f *testing.F) {
	f.Add(byte(CommandPrepare), make([]byte, CommandFieldsSize), make([]byte, 256))
	f.Add(byte(CommandJoinView), make([]byte, CommandFieldsSize), make([]byte, HeaderSize))
	f.Add(byte(CommandGetBlocks), make([]byte, CommandFieldsSize), make([]byte, 32))
	context := testValidationContext()
	f.Fuzz(func(t *testing.T, commandByte byte, fieldsInput, bodyInput []byte) {
		if len(fieldsInput) > 4096 || len(bodyInput) > 4096 {
			t.Skip()
		}
		header := Header{
			Group:    context.Group,
			View:     View(commandByte),
			Release:  Release(commandByte & 1),
			Protocol: ProtocolVersion,
			Command:  Command(commandByte),
			Author:   ReplicaIndex(commandByte % context.MemberCount),
		}
		copy(header.Fields[:], fieldsInput)
		_ = ValidateSemantics(&header, bodyInput, context)
	})
}

func FuzzDecodeFrameDoesNotPanic(f *testing.F) {
	context := testValidationContext()
	f.Add(makeValidFuzzSeed(context))
	f.Add(make([]byte, HeaderSize))
	f.Fuzz(func(t *testing.T, frame []byte) {
		if len(frame) > int(context.MessageSizeMax)+1 {
			t.Skip()
		}
		_, _, _ = DecodeFrame(frame, context.Group, context.MessageSizeMax, context.MemberCount)
	})
}

func makeValidFuzzSeed(context ValidationContext) []byte {
	frame := make([]byte, HeaderSize)
	header := Header{Group: context.Group, Protocol: ProtocolVersion, Command: CommandExitView, Author: context.Sender}
	if err := SealFrame(frame, &header); err != nil {
		panic(err)
	}
	return frame
}
