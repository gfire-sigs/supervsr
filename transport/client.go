package transport

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

// ClientConfig configures a socket-based ClientTransport connecting a VSR
// Client or ManagedClient to cluster replicas.
type ClientConfig struct {
	Group             protocol.GroupID
	ClientID          protocol.ClientID
	ActiveCount       uint8
	Members           [replication.MembersMax]protocol.MemberID
	PeerAddrs         [replication.MembersMax]string
	MessageSizeMax    uint32
	ChecksumAlgorithm protocol.ChecksumAlgorithm
	FramePoolSize     uint32
	SendQueueSize     int
	BackoffMin        time.Duration
	BackoffMax        time.Duration
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
	Authenticator     Authenticator
	Dial              DialFunc
	OnFrame           func(from protocol.ReplicaIndex, frame []byte)
}

type clientPeer struct {
	index  protocol.ReplicaIndex
	member protocol.MemberID
	addr   string
	queue  chan *protocol.Frame
}

// ClientTransport implements replication.MessageBus for a VSR client over TCP
// (or an injected Authenticator such as transport/mtls).
type ClientTransport struct {
	config  ClientConfig
	auth    Authenticator
	dial    DialFunc
	pool    *protocol.FramePool
	metrics transportMetrics
	peers   [replication.ActiveMax]*clientPeer

	sendMu     sync.RWMutex
	closedFlag atomic.Bool

	mu      sync.Mutex
	started bool
	closed  bool
	conns   map[net.Conn]struct{}
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
}

// NewClientTransport validates config and allocates bounded send queues and a
// receive frame pool for client-to-replica traffic.
func NewClientTransport(config ClientConfig) (*ClientTransport, error) {
	if config.Group.IsZero() || config.ClientID.IsZero() || config.ActiveCount == 0 || config.ActiveCount > replication.ActiveMax || config.OnFrame == nil {
		return nil, ErrInvalidConfig
	}
	if config.MessageSizeMax < protocol.HeaderSize || !config.ChecksumAlgorithm.Valid() {
		return nil, ErrInvalidConfig
	}
	if config.FramePoolSize == 0 {
		config.FramePoolSize = 64
	}
	if config.SendQueueSize <= 0 {
		config.SendQueueSize = 32
	}
	if config.BackoffMin <= 0 {
		config.BackoffMin = defaultBackoffMin
	}
	if config.BackoffMax <= 0 {
		config.BackoffMax = defaultBackoffMax
	}
	if config.BackoffMax < config.BackoffMin {
		config.BackoffMax = config.BackoffMin
	}
	if config.DialTimeout <= 0 {
		config.DialTimeout = defaultDialTimeout
	}
	if config.WriteTimeout <= 0 {
		config.WriteTimeout = defaultWriteTimeout
	}
	pool, err := protocol.NewFramePool(config.FramePoolSize, config.MessageSizeMax)
	if err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	auth := config.Authenticator
	if auth == nil {
		auth = &PlainAuthenticator{
			Group:   config.Group,
			Local:   PeerIdentity{Role: PeerRoleClient, Client: config.ClientID},
			Timeout: config.DialTimeout,
		}
	}
	dial := config.Dial
	if dial == nil {
		dialer := &net.Dialer{Timeout: config.DialTimeout}
		dial = dialer.DialContext
	}
	ct := &ClientTransport{
		config: config,
		auth:   auth,
		dial:   dial,
		pool:   pool,
		conns:  make(map[net.Conn]struct{}),
	}
	for index := range config.ActiveCount {
		ct.peers[index] = &clientPeer{
			index:  protocol.ReplicaIndex(index),
			member: config.Members[index],
			addr:   config.PeerAddrs[index],
			queue:  make(chan *protocol.Frame, config.SendQueueSize),
		}
	}
	return ct, nil
}

// Start launches per-replica connection and send/receive loops.
func (ct *ClientTransport) Start(ctx context.Context) error {
	ct.mu.Lock()
	if ct.closed {
		ct.mu.Unlock()
		return ErrTransportClosed
	}
	if ct.started {
		ct.mu.Unlock()
		return ErrTransportStarted
	}
	ct.started = true
	ct.ctx, ct.cancel = context.WithCancel(ctx)
	ct.mu.Unlock()

	for index := range ct.config.ActiveCount {
		peer := ct.peers[index]
		if peer == nil || peer.addr == "" {
			continue
		}
		ct.wg.Add(1)
		go ct.peerLoop(peer)
	}
	return nil
}

// Metrics returns a snapshot of client transport counters.
func (ct *ClientTransport) Metrics() MetricsSnapshot {
	return ct.metrics.snapshot()
}

// SendReplica enqueues a client request frame for replica to.
func (ct *ClientTransport) SendReplica(to protocol.ReplicaIndex, frame *replication.Message) {
	if frame == nil || uint8(to) >= ct.config.ActiveCount || ct.closedFlag.Load() {
		return
	}
	ct.sendMu.RLock()
	defer ct.sendMu.RUnlock()
	if ct.closedFlag.Load() {
		return
	}
	peer := ct.peers[to]
	if peer == nil || peer.addr == "" {
		return
	}
	if !frame.Retain() {
		return
	}
	select {
	case peer.queue <- frame:
	default:
		frame.Release()
		ct.metrics.sendDropped.Add(1)
	}
}

// SendClient is a no-op on client transports.
func (*ClientTransport) SendClient(protocol.ClientID, *replication.Message) {}

// BroadcastReplicas enqueues frame to every active replica.
func (ct *ClientTransport) BroadcastReplicas(frame *replication.Message) {
	if frame == nil || ct.closedFlag.Load() {
		return
	}
	for index := range ct.config.ActiveCount {
		ct.SendReplica(protocol.ReplicaIndex(index), frame)
	}
}

// Close closes all replica connections, stops workers, and releases queued frames.
func (ct *ClientTransport) Close(ctx context.Context) error {
	ct.mu.Lock()
	if ct.closed {
		ct.mu.Unlock()
		return nil
	}
	ct.closed = true
	ct.closedFlag.Store(true)
	cancel := ct.cancel
	conns := make([]net.Conn, 0, len(ct.conns))
	for conn := range ct.conns {
		conns = append(conns, conn)
	}
	ct.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	for _, conn := range conns {
		_ = conn.Close()
	}

	done := make(chan struct{})
	go func() {
		ct.wg.Wait()
		close(done)
	}()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}
	ct.sendMu.Lock()
	for index := range ct.config.ActiveCount {
		if peer := ct.peers[index]; peer != nil {
			drainFrameQueue(peer.queue)
		}
	}
	ct.sendMu.Unlock()
	return err
}

func (ct *ClientTransport) peerLoop(peer *clientPeer) {
	defer ct.wg.Done()
	backoff := ct.config.BackoffMin
	expected := PeerIdentity{Role: PeerRoleReplica, Member: peer.member, Replica: peer.index}

	for {
		var first *protocol.Frame
		select {
		case <-ct.ctx.Done():
			return
		case first = <-peer.queue:
		}
		if first == nil {
			continue
		}
		conn, err := ct.dialReplica(peer.addr, expected)
		if err != nil {
			first.Release()
			ct.metrics.sendDropped.Add(1)
			if !sleepWithContext(ct.ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, ct.config.BackoffMin, ct.config.BackoffMax)
			continue
		}
		backoff = ct.config.BackoffMin
		ct.runPeerConn(peer, conn, first)
		if ct.ctx.Err() != nil {
			return
		}
		ct.metrics.reconnects.Add(1)
		if !sleepWithContext(ct.ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, ct.config.BackoffMin, ct.config.BackoffMax)
	}
}

func (ct *ClientTransport) runPeerConn(peer *clientPeer, conn net.Conn, first *protocol.Frame) {
	ct.trackConn(conn)
	ct.metrics.activeConnections.Add(1)
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		ct.readLoop(peer.index, conn)
	}()

	defer func() {
		ct.untrackAndClose(conn)
		<-readDone
		ct.metrics.activeConnections.Add(-1)
	}()

	if err := writeFrame(conn, first, ct.config.WriteTimeout); err != nil {
		first.Release()
		ct.metrics.sendDropped.Add(1)
		return
	}
	first.Release()
	ct.metrics.framesSent.Add(1)

	for {
		select {
		case <-ct.ctx.Done():
			return
		case <-readDone:
			return
		case frame := <-peer.queue:
			if frame == nil {
				continue
			}
			if err := writeFrame(conn, frame, ct.config.WriteTimeout); err != nil {
				frame.Release()
				ct.metrics.sendDropped.Add(1)
				return
			}
			frame.Release()
			ct.metrics.framesSent.Add(1)
		}
	}
}

func (ct *ClientTransport) readLoop(from protocol.ReplicaIndex, conn net.Conn) {
	var headerBuf [protocol.HeaderSize]byte
	for {
		frame, _, err := readValidatedFrame(
			conn,
			&headerBuf,
			ct.pool,
			ct.config.Group,
			ct.config.MessageSizeMax,
			ct.config.ActiveCount,
			ct.config.ChecksumAlgorithm,
			&ct.metrics,
		)
		if err != nil {
			return
		}
		raw, err := frame.Bytes()
		if err != nil {
			frame.Release()
			return
		}
		ct.metrics.framesReceived.Add(1)
		ct.config.OnFrame(from, raw)
		frame.Release()
	}
}

func (ct *ClientTransport) dialReplica(addr string, expected PeerIdentity) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(ct.ctx, ct.config.DialTimeout)
	defer cancel()
	rawConn, err := ct.dial(dialCtx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := ct.auth.AuthenticateOutbound(dialCtx, rawConn, expected)
	if err != nil {
		ct.metrics.authFailures.Add(1)
		_ = rawConn.Close()
		return nil, err
	}
	return conn, nil
}

func (ct *ClientTransport) trackConn(conn net.Conn) {
	ct.mu.Lock()
	closed := ct.closed
	if !closed {
		ct.conns[conn] = struct{}{}
	}
	ct.mu.Unlock()
	if closed {
		_ = conn.Close()
	}
}

func (ct *ClientTransport) untrackAndClose(conn net.Conn) {
	ct.mu.Lock()
	delete(ct.conns, conn)
	ct.mu.Unlock()
	_ = conn.Close()
}
