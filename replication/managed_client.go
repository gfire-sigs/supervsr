package replication

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

// ErrClientEvicted reports that the cluster evicted the client session.
var ErrClientEvicted = errors.New("replication: client session evicted")

// ClientEvictionError records the cluster reason for evicting a client session.
type ClientEvictionError struct {
	Reason EvictionReason
}

func (err *ClientEvictionError) Error() string {
	return fmt.Sprintf("%s: reason %d", ErrClientEvicted, err.Reason)
}

func (err *ClientEvictionError) Unwrap() error {
	return ErrClientEvicted
}

// ManagedClient wraps Client with goroutine-safe synchronization, background
// ticks, serialized request execution, and context-aware Register and Submit
// calls.
type ManagedClient struct {
	mu         sync.Mutex
	turn       chan struct{}
	notify     chan struct{}
	done       chan struct{}
	stopTick   chan struct{}
	tickerDone chan struct{}
	stopOnce   sync.Once

	inner    *Client
	closed   bool
	evicted  bool
	reason   EvictionReason
	hasReply bool
	last     ClientReply
}

type managedClientEvents struct {
	owner *ManagedClient
}

func (events managedClientEvents) Reply(reply ClientReply) {
	reply.Body = append([]byte(nil), reply.Body...)
	owner := events.owner
	owner.last = reply
	owner.hasReply = true
	select {
	case owner.notify <- struct{}{}:
	default:
	}
}

func (events managedClientEvents) Evicted(reason EvictionReason) {
	owner := events.owner
	owner.evicted = true
	owner.reason = reason
	select {
	case owner.notify <- struct{}{}:
	default:
	}
}

// NewManagedClient constructs a ManagedClient and starts its background retry
// and keepalive ticker using config.Process.Tick. MessageBus implementations must
// deliver inbound replies asynchronously without calling HandleFrame
// synchronously inside SendReplica. Call Close when finished.
func NewManagedClient(config ClientConfig, bus MessageBus, clock Clock, entropy io.Reader) (*ManagedClient, error) {
	managed := &ManagedClient{
		turn:       make(chan struct{}, 1),
		notify:     make(chan struct{}, 1),
		done:       make(chan struct{}),
		stopTick:   make(chan struct{}),
		tickerDone: make(chan struct{}),
	}
	inner, err := NewClient(config, bus, clock, entropy, managedClientEvents{owner: managed})
	if err != nil {
		return nil, err
	}
	managed.inner = inner
	go managed.runTicks(config.Process.Tick)
	return managed, nil
}

// HandleFrame delivers a raw inbound replica frame to the client session. It is
// safe for concurrent calls and ignores frames once Close has run.
func (client *ManagedClient) HandleFrame(sender ReplicaIndex, frame []byte) {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.inner == nil {
		return
	}
	client.inner.HandleFrame(sender, frame)
}

// Register registers the client session and waits until the session is ready,
// evicted (*ClientEvictionError), closed (ErrClientState), or ctx is canceled.
func (client *ManagedClient) Register(ctx context.Context) error {
	if err := client.acquireTurn(ctx); err != nil {
		return err
	}
	defer client.releaseTurn()

	if err := client.waitIdle(ctx); err != nil {
		return err
	}
	client.mu.Lock()
	if err := client.readyToSendLocked(); err != nil {
		client.mu.Unlock()
		return err
	}
	if client.inner.State() == ClientRegistered {
		client.hasReply = false
		client.mu.Unlock()
		return nil
	}
	if err := client.inner.Register(); err != nil {
		client.mu.Unlock()
		return err
	}
	client.mu.Unlock()

	return client.waitUntil(ctx, func() (bool, error) {
		if err := client.checkClosedOrEvictedLocked(); err != nil {
			return true, err
		}
		if client.inner.State() == ClientRegistered {
			client.hasReply = false
			return true, nil
		}
		return false, nil
	})
}

// Submit sends an application operation and waits for its correlated reply.
// Concurrent callers serialize in arrival order, and any previously timed-out
// in-flight request drains before the next request enters the session chain.
func (client *ManagedClient) Submit(ctx context.Context, operation Operation, body []byte) (ClientReply, error) {
	if err := client.acquireTurn(ctx); err != nil {
		return ClientReply{}, err
	}
	defer client.releaseTurn()

	if err := client.waitIdle(ctx); err != nil {
		return ClientReply{}, err
	}
	client.mu.Lock()
	if err := client.readyToSendLocked(); err != nil {
		client.mu.Unlock()
		return ClientReply{}, err
	}
	client.hasReply = false
	if err := client.inner.Submit(operation, body); err != nil {
		client.mu.Unlock()
		return ClientReply{}, err
	}
	expected := client.inner.RequestNo()
	client.mu.Unlock()

	var result ClientReply
	err := client.waitUntil(ctx, func() (bool, error) {
		if err := client.checkClosedOrEvictedLocked(); err != nil {
			return true, err
		}
		if client.hasReply && client.last.Request == expected {
			result = client.last
			result.Body = append([]byte(nil), client.last.Body...)
			client.hasReply = false
			return true, nil
		}
		return false, nil
	})
	return result, err
}

// State returns the current client session lifecycle state.
func (client *ManagedClient) State() ClientState {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inner == nil {
		return ClientClosed
	}
	return client.inner.State()
}

// Session returns the assigned cluster session identifier, or 0 if unregistered.
func (client *ManagedClient) Session() Session {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inner == nil {
		return 0
	}
	return client.inner.Session()
}

// View returns the highest view number observed by the client session.
func (client *ManagedClient) View() View {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inner == nil {
		return 0
	}
	return client.inner.View()
}

// RequestNo returns the latest request number assigned by the session.
func (client *ManagedClient) RequestNo() RequestNo {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inner == nil {
		return 0
	}
	return client.inner.RequestNo()
}

// BatchLimit returns the negotiated maximum request payload size in bytes.
func (client *ManagedClient) BatchLimit() uint32 {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.inner == nil {
		return 0
	}
	return client.inner.BatchLimit()
}

// Close stops background ticking, releases internal frames, and wakes any
// blocked Register or Submit callers with ErrClientState.
func (client *ManagedClient) Close() error {
	client.stopOnce.Do(func() {
		close(client.done)
		close(client.stopTick)
	})
	<-client.tickerDone

	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed {
		return nil
	}
	client.closed = true
	if client.inner == nil {
		return nil
	}
	client.inner.clearInflight()
	return client.inner.Close()
}

func (client *ManagedClient) runTicks(interval time.Duration) {
	defer close(client.tickerDone)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			client.tickOnce()
		case <-client.stopTick:
			return
		}
	}
}

func (client *ManagedClient) tickOnce() {
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.closed || client.inner == nil {
		return
	}
	_ = client.inner.Tick()
}

func (client *ManagedClient) acquireTurn(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case client.turn <- struct{}{}:
		return nil
	case <-client.done:
		return ErrClientState
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (client *ManagedClient) releaseTurn() {
	<-client.turn
}

func (client *ManagedClient) waitIdle(ctx context.Context) error {
	return client.waitUntil(ctx, func() (bool, error) {
		if err := client.checkClosedOrEvictedLocked(); err != nil {
			return true, err
		}
		return client.inner.inflight.frame == nil, nil
	})
}

func (client *ManagedClient) readyToSendLocked() error {
	return client.checkClosedOrEvictedLocked()
}

func (client *ManagedClient) checkClosedOrEvictedLocked() error {
	if client.closed {
		return ErrClientState
	}
	if client.evicted {
		return &ClientEvictionError{Reason: client.reason}
	}
	return nil
}

func (client *ManagedClient) waitUntil(ctx context.Context, ready func() (bool, error)) error {
	for {
		client.mu.Lock()
		done, err := ready()
		client.mu.Unlock()
		if done || err != nil {
			return err
		}
		select {
		case <-client.notify:
		case <-client.done:
			return ErrClientState
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
