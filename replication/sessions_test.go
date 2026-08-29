package replication

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestSessionTableDecisionTable(t *testing.T) {
	table := newTestSessionTable(t, 2)
	client := protocol.ClientID{1}
	reply := makeSessionReply(t, client, 7, 1, protocol.OperationNoop)
	if _, err := table.Commit(reply, 7); err != nil {
		t.Fatal(err)
	}
	requestChecksum := replyRequestChecksum(&reply)
	context := replyContext(&reply)
	cases := []struct {
		name    string
		request SessionRequest
		want    SessionDecision
	}{
		{"missing", SessionRequest{Client: protocol.ClientID{2}}, SessionNoSession},
		{"session-low", SessionRequest{Client: client, Session: 6, Release: 1}, SessionTooLow},
		{"session-high", SessionRequest{Client: client, Session: 8, Release: 1}, SessionBehind},
		{"release", SessionRequest{Client: client, Session: 7, Release: 2}, SessionReleaseMismatch},
		{"old", SessionRequest{Client: client, Session: 7, Request: 0, Release: 1}, SessionDrop},
		{"duplicate", SessionRequest{Client: client, Session: 7, Request: 1, Release: 1, RequestChecksum: requestChecksum}, SessionDuplicate},
		{"duplicate-fork", SessionRequest{Client: client, Session: 7, Request: 1, Release: 1, RequestChecksum: protocol.Checksum{99}}, SessionClientFork},
		{"next", SessionRequest{Client: client, Session: 7, Request: 2, Release: 1, Parent: context}, SessionAdmit},
		{"parent-fork", SessionRequest{Client: client, Session: 7, Request: 2, Release: 1, Parent: protocol.Checksum{99}}, SessionClientFork},
		{"gap", SessionRequest{Client: client, Session: 7, Request: 3, Release: 1, Parent: context}, SessionDrop},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := table.Decide(test.request); got != test.want {
				t.Fatalf("decision = %v, want %v", got, test.want)
			}
		})
	}
}

func TestSessionTableTrailerRoundTripAndFixedSlots(t *testing.T) {
	table := newTestSessionTable(t, 2)
	first := makeSessionReply(t, protocol.ClientID{1}, 5, 1, protocol.OperationNoop)
	second := makeSessionReply(t, protocol.ClientID{2}, 6, 1, protocol.OperationNoop)
	firstSlot, err := table.Commit(first, 5)
	if err != nil {
		t.Fatal(err)
	}
	secondSlot, err := table.Commit(second, 6)
	if err != nil {
		t.Fatal(err)
	}
	if firstSlot == secondSlot {
		t.Fatal("distinct clients share a reply-zone slot")
	}
	if table.TrailerSize() != 2*protocol.HeaderSize+2*8 {
		t.Fatalf("trailer size = %d", table.TrailerSize())
	}
	encoded := make([]byte, table.TrailerSize())
	checksum, err := table.EncodeTrailer(encoded)
	if err != nil {
		t.Fatal(err)
	}
	recovered := newTestSessionTable(t, 2)
	if err := recovered.DecodeTrailer(encoded, checksum); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		client  protocol.ClientID
		session protocol.Session
		request protocol.RequestNo
		slot    uint32
	}{{protocol.ClientID{1}, 5, 1, firstSlot}, {protocol.ClientID{2}, 6, 1, secondSlot}} {
		reply, slot, ok := recovered.Reply(test.client, test.session, test.request)
		if !ok || slot != test.slot || replyClient(&reply) != test.client {
			t.Fatalf("reply lookup = slot %d ok %t header %+v", slot, ok, reply)
		}
	}
}

func TestSessionTableDecodeValidatesBeforePublication(t *testing.T) {
	table := newTestSessionTable(t, 2)
	reply := makeSessionReply(t, protocol.ClientID{1}, 5, 1, protocol.OperationNoop)
	if _, err := table.Commit(reply, 5); err != nil {
		t.Fatal(err)
	}
	encoded := make([]byte, table.TrailerSize())
	_, err := table.EncodeTrailer(encoded)
	if err != nil {
		t.Fatal(err)
	}
	recovered := newTestSessionTable(t, 2)
	if err := recovered.DecodeTrailer(encoded, protocol.Checksum{}); !errors.Is(err, ErrSessionChecksum) {
		t.Fatalf("checksum error = %v", err)
	}
	if recovered.Count() != 0 {
		t.Fatal("failed decode published a partial table")
	}

	corrupt := append([]byte(nil), encoded...)
	corrupt[protocol.HeaderSize] = 1
	corruptChecksum := protocol.ChecksumBytes(corrupt)
	if err := recovered.DecodeTrailer(corrupt, corruptChecksum); !errors.Is(err, ErrSessionEncoding) {
		t.Fatalf("free slot mismatch error = %v", err)
	}
	if recovered.Count() != 0 {
		t.Fatal("invalid free slot published a partial table")
	}
}

func TestSessionTableDeterministicEviction(t *testing.T) {
	table := newTestSessionTable(t, 2)
	secondClient := protocol.ClientID{2}
	firstClient := protocol.ClientID{1}
	for _, test := range []struct {
		client protocol.ClientID
		op     protocol.Op
	}{{secondClient, 5}, {firstClient, 5}, {protocol.ClientID{3}, 6}} {
		reply := makeSessionReply(t, test.client, test.op, 1, protocol.OperationNoop)
		if _, err := table.Commit(reply, protocol.Session(test.op)); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, found := table.Reply(firstClient, 5, 1); found {
		t.Fatal("tie-break client was not evicted")
	}
	if _, _, found := table.Reply(secondClient, 5, 1); !found {
		t.Fatal("higher tie-break client was evicted")
	}
	if table.Count() != 2 {
		t.Fatalf("count = %d, want 2", table.Count())
	}
}

func TestSessionCommitReservationHidesOverwrittenVictim(t *testing.T) {
	table := newTestSessionTable(t, 1)
	oldClient := protocol.ClientID{1}
	oldReply := makeSessionReply(t, oldClient, 5, 1, protocol.OperationNoop)
	if _, err := table.Commit(oldReply, 5); err != nil {
		t.Fatal(err)
	}
	newReply := makeSessionReply(t, protocol.ClientID{2}, 6, 1, protocol.OperationNoop)
	plan, err := table.PlanCommit(newReply, 6)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, found := table.Reply(oldClient, 5, 1); found {
		t.Fatal("victim remained readable during reply-slot overwrite")
	}
	if _, err := table.PlanCommit(newReply, 6); !errors.Is(err, ErrSessionCapacity) {
		t.Fatalf("second plan error = %v, want %v", err, ErrSessionCapacity)
	}
	if err := table.CommitAt(plan, newReply, 6); err != nil {
		t.Fatal(err)
	}
	if _, _, found := table.Reply(protocol.ClientID{2}, 6, 1); !found {
		t.Fatal("durable reservation was not published")
	}
	if table.Count() != 1 {
		t.Fatalf("count = %d, want 1", table.Count())
	}
}

func BenchmarkSessionTableDecision(b *testing.B) {
	table := newTestSessionTable(b, 64)
	client := protocol.ClientID{1}
	reply := makeSessionReply(b, client, 7, 1, protocol.OperationNoop)
	if _, err := table.Commit(reply, 7); err != nil {
		b.Fatal(err)
	}
	request := SessionRequest{Client: client, Session: 7, Request: 2, Release: 1, Parent: replyContext(&reply)}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if table.Decide(request) != SessionAdmit {
			b.Fatal("request rejected")
		}
	}
}

func newTestSessionTable(t testing.TB, clientsMax uint32) *SessionTable {
	t.Helper()
	table, err := NewSessionTable(SessionTableConfig{
		ClientsMax:              clientsMax,
		Group:                   protocol.GroupID{9},
		ActiveCount:             3,
		MessageSizeMax:          4096,
		ApplicationReplySizeMax: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	return table
}

func makeSessionReply(t testing.TB, client protocol.ClientID, op protocol.Op, request protocol.RequestNo, operation protocol.Operation) protocol.Header {
	t.Helper()
	header := protocol.Header{
		BodyChecksum: protocol.ChecksumBytes(nil),
		Group:        protocol.GroupID{9},
		Size:         protocol.HeaderSize,
		View:         0,
		Release:      1,
		Protocol:     protocol.ProtocolVersion,
		Command:      protocol.CommandReply,
		Author:       0,
	}
	header.Fields[0] = byte(request + 1)
	copy(header.Fields[64:80], client[:])
	binary.LittleEndian.PutUint64(header.Fields[80:88], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[88:96], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[96:104], 1)
	binary.LittleEndian.PutUint32(header.Fields[104:108], uint32(request))
	header.Fields[108] = byte(operation)
	var encoded [protocol.HeaderSize]byte
	if err := protocol.EncodeHeader(encoded[:], &header); err != nil {
		t.Fatal(err)
	}
	context := protocol.ChecksumBytes(encoded[protocol.HeaderChecksumFrom:])
	copy(header.Fields[32:48], context[:])
	if err := protocol.EncodeHeader(encoded[:], &header); err != nil {
		t.Fatal(err)
	}
	return header
}
