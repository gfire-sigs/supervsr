package protocol

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"strings"
	"testing"
)

var updateProtocolGolden = flag.Bool("update-protocol-golden", false, "update canonical protocol frame fixtures")

type goldenFrameCase struct {
	name  string
	frame []byte
}

func TestCanonicalFrameFixtures(t *testing.T) {
	cases := canonicalFrameCases(t)
	assertAllCommandsHaveGoldenFrames(t, cases)
	actual := encodeGoldenFrames(cases)
	const fixturePath = "testdata/frames.golden"
	if *updateProtocolGolden {
		if err := os.MkdirAll("testdata", 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(fixturePath, actual, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	expected, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(actual, expected) {
		t.Fatalf("canonical frame fixtures changed at %s", firstDifferentGoldenLine(actual, expected))
	}

	context := goldenValidationContext()
	for _, test := range cases {
		header, body, reason := DecodeFrame(test.frame, context.Group, context.MessageSizeMax, context.MemberCount)
		if reason != RejectNone {
			t.Fatalf("%s decode rejected: %d", test.name, reason)
		}
		if reason := ValidateSemantics(&header, body, context); reason != RejectNone {
			t.Fatalf("%s semantics rejected: %d", test.name, reason)
		}
	}
}

func canonicalFrameCases(t *testing.T) []goldenFrameCase {
	t.Helper()
	group := goldenGroup()
	makeFrame := func(command Command, author ReplicaIndex, release Release, fields [CommandFieldsSize]byte, body []byte) []byte {
		frame := make([]byte, HeaderSize+len(body))
		copy(frame[HeaderSize:], body)
		header := Header{
			Group:    group,
			View:     0,
			Release:  release,
			Protocol: ProtocolVersion,
			Command:  command,
			Author:   author,
			Fields:   fields,
		}
		if err := SealFrame(frame, &header); err != nil {
			t.Fatalf("seal command %d: %v", command, err)
		}
		return frame
	}
	var fields [CommandFieldsSize]byte
	fillGolden16(fields[0:16], 0x10)
	binary.LittleEndian.PutUint64(fields[16:24], 7)
	binary.LittleEndian.PutUint64(fields[24:32], 0x0102030405060708)
	binary.LittleEndian.PutUint16(fields[32:34], 1)
	pingBody := make([]byte, 16)
	binary.LittleEndian.PutUint32(pingBody[:4], 1)
	ping := makeFrame(CommandPing, 1, 1, fields, pingBody)

	fields = [CommandFieldsSize]byte{}
	binary.LittleEndian.PutUint64(fields[0:8], 0x1112131415161718)
	binary.LittleEndian.PutUint64(fields[8:16], 0x2122232425262728)
	pong := makeFrame(CommandPong, 1, 1, fields, nil)

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x30)
	binary.LittleEndian.PutUint64(fields[16:24], 0x3132333435363738)
	binary.LittleEndian.PutUint64(fields[24:32], 9)
	clientPing := makeFrame(CommandClientPing, 0, 1, fields, nil)

	fields = [CommandFieldsSize]byte{}
	binary.LittleEndian.PutUint64(fields[0:8], 0x4142434445464748)
	clientPong := makeFrame(CommandClientPong, 1, 1, fields, nil)

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x50)
	fillGolden16(fields[32:48], 0x60)
	binary.LittleEndian.PutUint64(fields[48:56], 11)
	binary.LittleEndian.PutUint32(fields[64:68], 12)
	fields[68] = byte(OperationApplicationMin)
	binary.LittleEndian.PutUint32(fields[72:76], 13)
	request := makeFrame(CommandRequest, 0, 1, fields, []byte{0xa1, 0xa2})

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x70)
	fillGolden16(fields[32:48], 0x80)
	fillGolden16(fields[64:80], 0x90)
	fillGolden16(fields[80:96], 0xa0)
	binary.LittleEndian.PutUint64(fields[96:104], 2)
	binary.LittleEndian.PutUint64(fields[104:112], 1)
	binary.LittleEndian.PutUint64(fields[112:120], 0x5152535455565758)
	binary.LittleEndian.PutUint32(fields[120:124], 14)
	fields[124] = byte(OperationApplicationMin)
	prepare := makeFrame(CommandPrepare, 0, 1, fields, []byte{0xb1, 0xb2})

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0xb0)
	fillGolden16(fields[32:48], 0xc0)
	fillGolden16(fields[64:80], 0xd0)
	fillGolden16(fields[80:96], 0xe0)
	binary.LittleEndian.PutUint64(fields[96:104], 3)
	binary.LittleEndian.PutUint64(fields[104:112], 2)
	binary.LittleEndian.PutUint64(fields[112:120], 0x6162636465666768)
	binary.LittleEndian.PutUint32(fields[120:124], 15)
	fields[124] = byte(OperationApplicationMin)
	prepareOK := makeFrame(CommandPrepareOK, 1, 0, fields, nil)

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x11)
	fillGolden16(fields[32:48], 0x21)
	fillGolden16(fields[64:80], 0x31)
	binary.LittleEndian.PutUint64(fields[80:88], 4)
	binary.LittleEndian.PutUint64(fields[88:96], 4)
	binary.LittleEndian.PutUint64(fields[96:104], 0x7172737475767778)
	binary.LittleEndian.PutUint32(fields[104:108], 16)
	fields[108] = byte(OperationApplicationMin)
	reply := makeFrame(CommandReply, 0, 1, fields, []byte{0xc1})

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x41)
	fillGolden16(fields[32:48], 0x51)
	binary.LittleEndian.PutUint64(fields[48:56], 5)
	binary.LittleEndian.PutUint64(fields[56:64], 6)
	binary.LittleEndian.PutUint64(fields[64:72], 0x8182838485868788)
	commit := makeFrame(CommandCommit, 0, 0, fields, nil)

	exitView := makeFrame(CommandExitView, 1, 0, [CommandFieldsSize]byte{}, nil)

	fields = [CommandFieldsSize]byte{}
	fields[0] = 1
	fields[16] = 1
	binary.LittleEndian.PutUint64(fields[32:40], 2)
	binary.LittleEndian.PutUint64(fields[40:48], 1)
	binary.LittleEndian.PutUint64(fields[48:56], 0)
	binary.LittleEndian.PutUint32(fields[56:60], 1)
	joinView := makeFrame(CommandJoinView, 1, 0, fields, prepare[:HeaderSize])

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x61)
	getView := makeFrame(CommandGetView, 1, 0, fields, nil)

	fields = [CommandFieldsSize]byte{}
	binary.LittleEndian.PutUint64(fields[0:8], 7)
	binary.LittleEndian.PutUint64(fields[8:16], 9)
	getHeaders := makeFrame(CommandGetHeaders, 1, 0, fields, nil)

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x71)
	binary.LittleEndian.PutUint64(fields[32:40], 8)
	getPrepare := makeFrame(CommandGetPrepare, 1, 0, fields, nil)

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0x81)
	fillGolden16(fields[32:48], 0x91)
	binary.LittleEndian.PutUint64(fields[48:56], 10)
	getReply := makeFrame(CommandGetReply, 1, 0, fields, nil)

	headers := makeFrame(CommandHeaders, 1, 0, [CommandFieldsSize]byte{}, prepare[:HeaderSize])

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0xa1)
	fields[127] = byte(EvictionInvalidBody)
	eviction := makeFrame(CommandEviction, 1, 1, fields, nil)

	getBlocksBody := make([]byte, 32)
	fillGolden16(getBlocksBody[0:16], 0xb1)
	binary.LittleEndian.PutUint64(getBlocksBody[16:24], 1)
	getBlocks := makeFrame(CommandGetBlocks, 1, 0, [CommandFieldsSize]byte{}, getBlocksBody)

	fields = [CommandFieldsSize]byte{}
	binary.LittleEndian.PutUint32(fields[0:4], 1)
	binary.LittleEndian.PutUint32(fields[4:8], 1)
	binary.LittleEndian.PutUint32(fields[8:12], 2)
	binary.LittleEndian.PutUint64(fields[96:104], 1)
	binary.LittleEndian.PutUint64(fields[104:112], 11)
	fields[112] = byte(BlockValue)
	block := makeFrame(CommandBlock, 0, 1, fields, []byte{0xd1, 0xd2})

	fields = [CommandFieldsSize]byte{}
	fillGolden16(fields[0:16], 0xc1)
	binary.LittleEndian.PutUint64(fields[16:24], 12)
	binary.LittleEndian.PutUint64(fields[24:32], 11)
	binary.LittleEndian.PutUint64(fields[32:40], 10)
	view := makeFrame(CommandView, 0, 0, fields, make([]byte, 1024))

	return []goldenFrameCase{
		{name: "ping", frame: ping},
		{name: "pong", frame: pong},
		{name: "client_ping", frame: clientPing},
		{name: "client_pong", frame: clientPong},
		{name: "request", frame: request},
		{name: "prepare", frame: prepare},
		{name: "prepare_ok", frame: prepareOK},
		{name: "reply", frame: reply},
		{name: "commit", frame: commit},
		{name: "exit_view", frame: exitView},
		{name: "join_view", frame: joinView},
		{name: "get_view", frame: getView},
		{name: "get_headers", frame: getHeaders},
		{name: "get_prepare", frame: getPrepare},
		{name: "get_reply", frame: getReply},
		{name: "headers", frame: headers},
		{name: "eviction", frame: eviction},
		{name: "get_blocks", frame: getBlocks},
		{name: "block", frame: block},
		{name: "view", frame: view},
	}
}

func goldenValidationContext() ValidationContext {
	return ValidationContext{
		Authenticated:           true,
		ReplicaSource:           true,
		Sender:                  1,
		ActiveCount:             3,
		MemberCount:             3,
		PipelineMax:             4,
		ReleaseHistoryMax:       4,
		ApplicationBatchSizeMax: 1024,
		ApplicationReplySizeMax: 1024,
		RepairRequestsMax:       4,
		CurrentRelease:          1,
		ClientReleaseMin:        1,
		Group:                   goldenGroup(),
		MessageSizeMax:          4096,
	}
}

func goldenGroup() GroupID {
	var group GroupID
	fillGolden16(group[:], 1)
	return group
}

func fillGolden16(destination []byte, start byte) {
	for index := range 16 {
		destination[index] = start + byte(index)
	}
}

func assertAllCommandsHaveGoldenFrames(t *testing.T, cases []goldenFrameCase) {
	t.Helper()
	seen := [256]bool{}
	for _, test := range cases {
		command := Command(test.frame[114])
		if seen[command] {
			t.Fatalf("duplicate golden frame for command %d", command)
		}
		seen[command] = true
	}
	for command := CommandPing; command <= CommandView; command++ {
		if command.IsDefined() && !seen[command] {
			t.Fatalf("missing golden frame for command %d", command)
		}
	}
}

func encodeGoldenFrames(cases []goldenFrameCase) []byte {
	var output strings.Builder
	for _, test := range cases {
		fmt.Fprintf(&output, "%s %s\n", test.name, hex.EncodeToString(test.frame))
	}
	return []byte(output.String())
}

func firstDifferentGoldenLine(actual, expected []byte) string {
	actualLines := strings.Split(string(actual), "\n")
	expectedLines := strings.Split(string(expected), "\n")
	limit := min(len(actualLines), len(expectedLines))
	for index := range limit {
		if actualLines[index] != expectedLines[index] {
			return fmt.Sprintf("line %d", index+1)
		}
	}
	return fmt.Sprintf("line %d", limit+1)
}
