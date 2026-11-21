package replication

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication/protocol"
)

type managedAsyncBus struct {
	mu     sync.Mutex
	held   bool
	queued [][]byte
	sent   chan struct{}
	inbox  chan []byte
}

func newManagedAsyncBus() *managedAsyncBus {
	return &managedAsyncBus{
		sent:  make(chan struct{}, 32),
		inbox: make(chan []byte, 32),
	}
}

func (bus *managedAsyncBus) SendReplica(_ protocol.ReplicaIndex, message *Message) {
	encoded, err := message.Bytes()
	if err != nil {
		return
	}
	copied := append([]byte(nil), encoded...)
	bus.mu.Lock()
	held := bus.held
	if held {
		bus.queued = append(bus.queued, copied)
	}
	bus.mu.Unlock()
	if !held {
		bus.inbox <- copied
	}
	select {
	case bus.sent <- struct{}{}:
	default:
	}
}

func (bus *managedAsyncBus) SendClient(protocol.ClientID, *Message) {}

func (bus *managedAsyncBus) BroadcastReplicas(message *Message) {
	bus.SendReplica(0, message)
}

func (bus *managedAsyncBus) setHeld(held bool) {
	bus.mu.Lock()
	bus.held = held
	var flushed [][]byte
	if !held && len(bus.queued) > 0 {
		flushed = bus.queued
		bus.queued = nil
	}
	bus.mu.Unlock()
	for _, frame := range flushed {
		bus.inbox <- frame
	}
}

func (bus *managedAsyncBus) drainSent() {
	for {
		select {
		case <-bus.sent:
		default:
			return
		}
	}
}

type managedResponder struct {
	t       testing.TB
	bus     *managedAsyncBus
	client  *ManagedClient
	config  ClientConfig
	session uint64
	stop    chan struct{}
	done    chan struct{}
}

func startManagedResponder(t testing.TB, holdRequests bool) (*ManagedClient, *managedAsyncBus, func()) {
	t.Helper()
	bus := newManagedAsyncBus()
	bus.setHeld(holdRequests)
	config := ClientConfig{
		Group:          protocol.GroupID{1},
		ID:             protocol.ClientID{2},
		Release:        1,
		ActiveCount:    3,
		MessageSizeMax: 4096,
		Process:        DefaultProcessConfig(),
	}
	config.Process.Tick = 2 * time.Millisecond
	config.Process.InitialRTT = 10 * time.Millisecond
	config.Process.MaximumRTT = 50 * time.Millisecond

	clock := &manualClientClock{sample: TimeSample{Wall: 1, Monotonic: 1}}
	managed, err := NewManagedClient(config, bus, clock, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	responder := &managedResponder{
		t: t, bus: bus, client: managed, config: config,
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	go responder.serve()

	cleanup := func() {
		close(responder.stop)
		<-responder.done
		if err := managed.Close(); err != nil {
			t.Errorf("close managed client: %v", err)
		}
	}
	return managed, bus, cleanup
}

func (r *managedResponder) serve() {
	defer close(r.done)
	for {
		select {
		case <-r.stop:
			return
		case raw := <-r.bus.inbox:
			header, body, reason := protocol.DecodeFrame(raw, r.config.Group, r.config.MessageSizeMax, r.config.ActiveCount, r.config.ChecksumAlgorithm)
			if reason != protocol.RejectNone || header.Command != protocol.CommandRequest {
				continue
			}
			op := protocol.Operation(header.Fields[68])
			requestNo := protocol.RequestNo(binary.LittleEndian.Uint32(header.Fields[64:68]))
			r.session++
			var reply []byte
			if op == protocol.OperationRegister {
				reply = makeClientReply(r.t, header, protocol.Op(r.session), 1, func(dst []byte) {
					binary.LittleEndian.PutUint32(dst[:4], 1024)
				})
			} else if string(body) == "evict" {
				reply = makeClientEviction(r.t, r.config, 2, 0, EvictionInvalidOperation)
			} else {
				reply = makeClientReply(r.t, header, protocol.Op(100+uint64(requestNo)), 1, func(dst []byte) {
					copy(dst, body)
				})
			}
			r.client.HandleFrame(0, reply)
		}
	}
}

func TestManagedClientConcurrentSubmitCorrelatesReplies(t *testing.T) {
	managed, _, cleanup := startManagedResponder(t, false)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := managed.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if managed.State() != ClientRegistered || managed.Session() != 1 || managed.BatchLimit() != 1024 {
		t.Fatalf("state=%d session=%d limit=%d", managed.State(), managed.Session(), managed.BatchLimit())
	}

	const workers = 8
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for index := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			payload := fmt.Sprintf("k%d", id)
			reply, err := managed.Submit(ctx, OperationApplicationMin, []byte(payload))
			if err != nil {
				errs <- err
				return
			}
			want := payload
			if string(reply.Body) != want {
				errs <- fmt.Errorf("reply = %q, want %q", reply.Body, want)
			}
		}(index)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

func TestManagedClientCanceledSubmitDrainsBeforeNextSubmit(t *testing.T) {
	managed, bus, cleanup := startManagedResponder(t, false)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := managed.Register(ctx); err != nil {
		t.Fatal(err)
	}

	bus.drainSent()
	bus.setHeld(true)
	firstCtx, firstCancel := context.WithCancel(ctx)
	firstDone := make(chan error, 1)
	go func() {
		_, err := managed.Submit(firstCtx, OperationApplicationMin, []byte("1a"))
		firstDone <- err
	}()
	select {
	case <-bus.sent:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	firstCancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submit err = %v, want context.Canceled", err)
	}

	secondDone := make(chan ClientReply, 1)
	secondErr := make(chan error, 1)
	go func() {
		reply, err := managed.Submit(ctx, OperationApplicationMin, []byte("2b"))
		if err != nil {
			secondErr <- err
			return
		}
		secondDone <- reply
	}()
	bus.setHeld(false)
	select {
	case err := <-secondErr:
		t.Fatal(err)
	case reply := <-secondDone:
		if string(reply.Body) != "2b" {
			t.Fatalf("second reply = %q, want 2b", reply.Body)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

func TestManagedClientReturnsEvictionErrorAndUnblocksOnClose(t *testing.T) {
	managed, _, cleanup := startManagedResponder(t, false)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := managed.Register(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := managed.Submit(ctx, OperationApplicationMin, []byte("evict")); !errors.Is(err, ErrClientEvicted) {
		t.Fatalf("eviction err = %v, want ErrClientEvicted", err)
	}

	blocked, bus, blockedCleanup := startManagedResponder(t, true)
	defer blockedCleanup()
	closedErr := make(chan error, 1)
	go func() {
		closedErr <- blocked.Register(ctx)
	}()
	select {
	case <-bus.sent:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := blocked.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-closedErr; !errors.Is(err, ErrClientState) {
		t.Fatalf("blocked register after Close = %v, want ErrClientState", err)
	}
}
