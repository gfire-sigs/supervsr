package replication

import (
	"bytes"
	"encoding/binary"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gfire-sigs/supervsr/replication/protocol"
)

func TestClientRegistrationRetryReplyAndEviction(t *testing.T) {
	bus := &clientCaptureBus{}
	clock := &manualClientClock{sample: TimeSample{Wall: 1, Monotonic: 1}}
	events := &clientEventRecorder{}
	client := newTestClient(t, bus, clock, events)
	defer client.Close()
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	if len(bus.frames) != 2 || bus.destinations[0] != 0 || bus.destinations[1] == 0 || !bytes.Equal(bus.frames[0], bus.frames[1]) {
		t.Fatalf("registration routes=%v frames=%d", bus.destinations, len(bus.frames))
	}
	registration, _, reason := protocol.DecodeFrame(bus.frames[0], protocol.GroupID{1}, 4096, 3)
	if reason != protocol.RejectNone || protocol.Operation(registration.Fields[68]) != protocol.OperationRegister || registration.View != 0 {
		t.Fatalf("registration header=%+v reason=%v", registration, reason)
	}
	registerReply := makeClientReply(t, registration, 1, 2, func(body []byte) {
		binary.LittleEndian.PutUint32(body[:4], 1024)
	})
	client.HandleFrame(2, registerReply)
	if client.State() != ClientRegistered || client.Session() != 1 || client.BatchLimit() != 1024 || events.replies != 1 {
		t.Fatalf("registered state=%d session=%d limit=%d replies=%d", client.State(), client.Session(), client.BatchLimit(), events.replies)
	}
	if err := client.Submit(protocol.OperationApplicationMin, []byte("set value")); err != nil {
		t.Fatal(err)
	}
	application := append([]byte(nil), bus.frames[2]...)
	requestHeader, _, reason := protocol.DecodeFrame(application, protocol.GroupID{1}, 4096, 3)
	if reason != protocol.RejectNone || requestHeader.View != 2 || binary.LittleEndian.Uint64(requestHeader.Fields[48:56]) != 1 || binary.LittleEndian.Uint32(requestHeader.Fields[64:68]) != 1 {
		t.Fatalf("application request=%+v reason=%v", requestHeader, reason)
	}
	for range 59 {
		if err := client.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if len(bus.frames) != 4 {
		t.Fatalf("request fired early: frames=%d", len(bus.frames))
	}
	if err := client.Tick(); err != nil {
		t.Fatal(err)
	}
	if len(bus.frames) != 6 || !bytes.Equal(application, bus.frames[4]) || !bytes.Equal(application, bus.frames[5]) {
		t.Fatal("retry changed immutable request bytes")
	}
	applicationReply := makeClientReply(t, requestHeader, 2, 2, func(body []byte) { copy(body, "ok") })
	client.HandleFrame(2, applicationReply)
	if events.replies != 2 || string(events.lastBody) != "ok" || client.RequestNo() != 1 {
		t.Fatalf("application events=%d body=%q request=%d", events.replies, events.lastBody, client.RequestNo())
	}
	if err := client.Submit(protocol.OperationUpgrade, nil); !errors.Is(err, ErrClientState) {
		t.Fatalf("upgrade submit error=%v", err)
	}
	eviction := makeClientEviction(t, client.config, 3, 1, protocol.EvictionNoSession)
	client.HandleFrame(1, eviction)
	if client.State() != ClientEvicted || events.evictions != 1 || events.lastEviction != protocol.EvictionNoSession {
		t.Fatalf("eviction state=%d count=%d reason=%d", client.State(), events.evictions, events.lastEviction)
	}
}

func TestClientPingPongRaisesViewWithoutRequestResend(t *testing.T) {
	bus := &clientCaptureBus{}
	clock := &manualClientClock{sample: TimeSample{Wall: 1, Monotonic: 10}}
	client := newTestClient(t, bus, clock, &clientEventRecorder{})
	defer client.Close()
	for range 3000 {
		if err := client.Tick(); err != nil {
			t.Fatal(err)
		}
	}
	if len(bus.frames) != 3 {
		t.Fatalf("client ping routes = %d", len(bus.frames))
	}
	ping, _, reason := protocol.DecodeFrame(bus.frames[0], protocol.GroupID{1}, 4096, 3)
	if reason != protocol.RejectNone || ping.Command != protocol.CommandClientPing {
		t.Fatalf("ping command=%d reason=%v", ping.Command, reason)
	}
	clock.sample.Monotonic = client.lastPing + uint64(300*time.Millisecond)
	pong := makeClientPong(t, client.config, 1, 5, client.lastPing)
	client.HandleFrame(1, pong)
	if client.View() != 5 || client.measuredRTT() != 300*time.Millisecond || len(bus.frames) != 3 {
		t.Fatalf("pong view=%d rtt=%s routes=%d", client.View(), client.measuredRTT(), len(bus.frames))
	}
}

func TestClientReplyCallbackCanSubmit(t *testing.T) {
	bus := &clientCaptureBus{}
	events := &clientEventRecorder{}
	client := newTestClient(t, bus, &manualClientClock{sample: TimeSample{Wall: 1, Monotonic: 1}}, events)
	defer client.Close()
	events.onReply = func(reply ClientReply) {
		if reply.Operation == protocol.OperationRegister {
			events.callbackErr = client.Submit(protocol.OperationApplicationMin, []byte("next"))
		}
	}
	if err := client.Register(); err != nil {
		t.Fatal(err)
	}
	registration, _, reason := protocol.DecodeFrame(bus.frames[0], protocol.GroupID{1}, 4096, 3)
	if reason != protocol.RejectNone {
		t.Fatal(reason)
	}
	client.HandleFrame(0, makeClientReply(t, registration, 1, 0, func(body []byte) {
		binary.LittleEndian.PutUint32(body[:4], 1024)
	}))
	if events.callbackErr != nil || len(bus.frames) != 4 {
		t.Fatalf("callback error=%v routes=%d", events.callbackErr, len(bus.frames))
	}
	if err := client.Submit(protocol.OperationApplicationMin, nil); !errors.Is(err, ErrClientInFlight) {
		t.Fatalf("second submit error=%v", err)
	}
}

func BenchmarkClientTick(b *testing.B) {
	client := newTestClient(b, discardClientBus{}, &manualClientClock{sample: TimeSample{Wall: 1, Monotonic: 1}}, &clientEventRecorder{})
	defer client.Close()
	if err := client.Register(); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := client.Tick(); err != nil {
			b.Fatal(err)
		}
	}
}

func newTestClient(t testing.TB, bus MessageBus, clock Clock, events ClientEvents) *Client {
	t.Helper()
	client, err := NewClient(ClientConfig{
		Group: protocol.GroupID{1}, ID: protocol.ClientID{9}, Release: 1, ActiveCount: 3,
		MessageSizeMax: 4096, Process: DefaultProcessConfig(),
	}, bus, clock, bytes.NewReader([]byte{1, 2, 3, 4, 5, 6, 7, 8}), events)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func makeClientReply(t testing.TB, request protocol.Header, op protocol.Op, view protocol.View, fill func([]byte)) []byte {
	t.Helper()
	bodySize := 2
	if protocol.Operation(request.Fields[68]) == protocol.OperationRegister {
		bodySize = 64
	}
	frame := make([]byte, protocol.HeaderSize+bodySize)
	fill(frame[protocol.HeaderSize:])
	header := protocol.Header{Group: request.Group, View: view, Release: request.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandReply, Author: protocol.ReplicaIndex(uint32(view) % 3)}
	copy(header.Fields[:16], request.HeaderChecksum[:])
	header.Fields[32] = 7
	copy(header.Fields[64:80], request.Fields[32:48])
	binary.LittleEndian.PutUint64(header.Fields[80:88], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[88:96], uint64(op))
	binary.LittleEndian.PutUint64(header.Fields[96:104], 10)
	copy(header.Fields[104:109], request.Fields[64:69])
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame
}

func makeClientEviction(t testing.TB, config ClientConfig, view protocol.View, author protocol.ReplicaIndex, reason protocol.EvictionReason) []byte {
	t.Helper()
	frame := make([]byte, protocol.HeaderSize)
	header := protocol.Header{Group: config.Group, View: view, Release: config.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandEviction, Author: author}
	copy(header.Fields[:16], config.ID[:])
	header.Fields[127] = byte(reason)
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame
}

func makeClientPong(t testing.TB, config ClientConfig, author protocol.ReplicaIndex, view protocol.View, ping uint64) []byte {
	t.Helper()
	frame := make([]byte, protocol.HeaderSize)
	header := protocol.Header{Group: config.Group, View: view, Release: config.Release, Protocol: protocol.ProtocolVersion, Command: protocol.CommandClientPong, Author: author}
	binary.LittleEndian.PutUint64(header.Fields[:8], ping)
	if err := protocol.SealFrame(frame, &header); err != nil {
		t.Fatal(err)
	}
	return frame
}

type clientCaptureBus struct {
	mu           sync.Mutex
	destinations []protocol.ReplicaIndex
	frames       [][]byte
}

func (bus *clientCaptureBus) SendReplica(to protocol.ReplicaIndex, message *Message) {
	frame, _ := message.Bytes()
	bus.mu.Lock()
	bus.destinations = append(bus.destinations, to)
	bus.frames = append(bus.frames, append([]byte(nil), frame...))
	bus.mu.Unlock()
}
func (bus *clientCaptureBus) SendClient(protocol.ClientID, *Message) {}
func (bus *clientCaptureBus) BroadcastReplicas(*Message)             {}

type discardClientBus struct{}

func (discardClientBus) SendReplica(protocol.ReplicaIndex, *Message) {}
func (discardClientBus) SendClient(protocol.ClientID, *Message)      {}
func (discardClientBus) BroadcastReplicas(*Message)                  {}

type manualClientClock struct{ sample TimeSample }

func (clock *manualClientClock) Now() TimeSample { return clock.sample }

type clientEventRecorder struct {
	replies      int
	evictions    int
	lastBody     []byte
	lastEviction protocol.EvictionReason
	onReply      func(ClientReply)
	callbackErr  error
}

func (events *clientEventRecorder) Reply(reply ClientReply) {
	events.replies++
	events.lastBody = append(events.lastBody[:0], reply.Body...)
	if events.onReply != nil {
		events.onReply(reply)
	}
}

func (events *clientEventRecorder) Evicted(reason protocol.EvictionReason) {
	events.evictions++
	events.lastEviction = reason
}
