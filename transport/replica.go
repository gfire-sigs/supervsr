package transport

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

// ReplicaConfig configures a socket-based ReplicaTransport.
type ReplicaConfig struct {
	Group             protocol.GroupID
	Membership        replication.Membership
	ListenAddr        string
	PeerAddrs         [replication.MembersMax]string
	MessageSizeMax    uint32
	ChecksumAlgorithm protocol.ChecksumAlgorithm
	FramePoolSize     uint32
	SendQueueSize     int
	MaxClients        int
	BackoffMin        time.Duration
	BackoffMax        time.Duration
	DialTimeout       time.Duration
	WriteTimeout      time.Duration
	Authenticator     Authenticator
	Dial              DialFunc
	OnFrame           func(*protocol.Frame) error
	Logger            *slog.Logger
}

type outboundPeer struct {
	index  protocol.ReplicaIndex
	member protocol.MemberID
	addr   string
	queue  chan *protocol.Frame
}

type clientRoute struct {
	id     protocol.ClientID
	conn   net.Conn
	queue  chan *protocol.Frame
	done   chan struct{}
	closed atomic.Bool
}

// ReplicaTransport implements replication.MessageBus over TCP (or an injected
// authenticated transport such as transport/mtls) and serves inbound peer and
// client connections.
type ReplicaTransport struct {
	config      ReplicaConfig
	localIndex  protocol.ReplicaIndex
	memberCount uint8
	auth        Authenticator
	dial        DialFunc
	pool        *protocol.FramePool
	metrics     transportMetrics

	peers [replication.MembersMax]*outboundPeer

	sendMu     sync.RWMutex
	closedFlag atomic.Bool

	mu       sync.Mutex
	listener net.Listener
	started  bool
	closed   bool
	clients  map[protocol.ClientID]*clientRoute
	conns    map[net.Conn]struct{}
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// NewReplicaTransport validates config and preallocates the inbound FramePool
// and per-peer send queues. Call Start to begin listening and dialing peers.
func NewReplicaTransport(config ReplicaConfig) (*ReplicaTransport, error) {
	if config.Group.IsZero() || config.OnFrame == nil {
		return nil, ErrInvalidConfig
	}
	if err := config.Membership.Validate(); err != nil {
		return nil, errors.Join(ErrInvalidConfig, err)
	}
	localIndex, ok := config.Membership.LocalIndex()
	if !ok {
		return nil, ErrInvalidConfig
	}
	if config.MessageSizeMax < protocol.HeaderSize || !config.ChecksumAlgorithm.Valid() {
		return nil, ErrInvalidConfig
	}
	if config.FramePoolSize == 0 {
		config.FramePoolSize = defaultFramePoolSize
	}
	if config.SendQueueSize <= 0 {
		config.SendQueueSize = defaultSendQueueSize
	}
	if config.MaxClients <= 0 {
		config.MaxClients = 256
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
			Group:      config.Group,
			Membership: config.Membership,
			Local: PeerIdentity{
				Role:    PeerRoleReplica,
				Member:  config.Membership.LocalMember,
				Replica: localIndex,
			},
			Timeout: config.DialTimeout,
		}
	}
	dial := config.Dial
	if dial == nil {
		dialer := &net.Dialer{Timeout: config.DialTimeout}
		dial = dialer.DialContext
	}
	memberCount := config.Membership.ActiveCount + config.Membership.StandbyCount
	t := &ReplicaTransport{
		config:      config,
		localIndex:  localIndex,
		memberCount: memberCount,
		auth:        auth,
		dial:        dial,
		pool:        pool,
		clients:     make(map[protocol.ClientID]*clientRoute),
		conns:       make(map[net.Conn]struct{}),
	}
	for index := range memberCount {
		idx := protocol.ReplicaIndex(index)
		if idx == localIndex {
			continue
		}
		t.peers[index] = &outboundPeer{
			index:  idx,
			member: config.Membership.Members[index],
			addr:   config.PeerAddrs[index],
			queue:  make(chan *protocol.Frame, config.SendQueueSize),
		}
	}
	return t, nil
}

// Start binds ListenAddr (or uses a caller-created listener via StartWithListener)
// and launches inbound accept and outbound peer dial/send goroutines.
func (t *ReplicaTransport) Start(ctx context.Context) error {
	if t.config.ListenAddr == "" {
		return ErrInvalidConfig
	}
	var lc net.ListenConfig
	listener, err := lc.Listen(ctx, "tcp", t.config.ListenAddr)
	if err != nil {
		return err
	}
	if err := t.StartWithListener(ctx, listener); err != nil {
		_ = listener.Close()
		return err
	}
	return nil
}

// StartWithListener starts serving on an already bound net.Listener.
func (t *ReplicaTransport) StartWithListener(ctx context.Context, listener net.Listener) error {
	if listener == nil {
		return ErrInvalidConfig
	}
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return ErrTransportClosed
	}
	if t.started {
		t.mu.Unlock()
		return ErrTransportStarted
	}
	t.listener = listener
	t.started = true
	t.ctx, t.cancel = context.WithCancel(ctx)
	t.mu.Unlock()

	t.wg.Add(1)
	go t.acceptLoop(listener)

	for index := range t.memberCount {
		peer := t.peers[index]
		if peer == nil || peer.addr == "" {
			continue
		}
		t.wg.Add(1)
		go t.peerSendLoop(peer)
	}
	return nil
}

// Addr returns the bound listener address, or nil if Start has not succeeded.
func (t *ReplicaTransport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listener == nil {
		return nil
	}
	return t.listener.Addr()
}

// Metrics returns a point-in-time snapshot of transport counters.
func (t *ReplicaTransport) Metrics() MetricsSnapshot {
	return t.metrics.snapshot()
}

// SendReplica enqueues frame for asynchronous delivery to replica to without
// blocking the caller or invoking replica logic reentrantly.
func (t *ReplicaTransport) SendReplica(to protocol.ReplicaIndex, frame *replication.Message) {
	if frame == nil || uint8(to) >= t.memberCount || to == t.localIndex || t.closedFlag.Load() {
		return
	}
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if t.closedFlag.Load() {
		return
	}
	peer := t.peers[to]
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
		t.metrics.sendDropped.Add(1)
	}
}

// BroadcastReplicas enqueues frame for asynchronous delivery to every other
// configured cluster member, acquiring one reference per destination.
func (t *ReplicaTransport) BroadcastReplicas(frame *replication.Message) {
	if frame == nil || t.closedFlag.Load() {
		return
	}
	for index := range t.memberCount {
		to := protocol.ReplicaIndex(index)
		if to == t.localIndex {
			continue
		}
		t.SendReplica(to, frame)
	}
}

// SendClient enqueues frame for asynchronous delivery over the active client
// connection bound to clientID. Unknown or congested routes drop without error.
func (t *ReplicaTransport) SendClient(to protocol.ClientID, frame *replication.Message) {
	if frame == nil || to.IsZero() || t.closedFlag.Load() {
		return
	}
	t.sendMu.RLock()
	defer t.sendMu.RUnlock()
	if t.closedFlag.Load() {
		return
	}
	t.mu.Lock()
	route := t.clients[to]
	t.mu.Unlock()
	if route == nil || route.closed.Load() {
		return
	}
	if !frame.Retain() {
		return
	}
	select {
	case route.queue <- frame:
	default:
		frame.Release()
		t.metrics.sendDropped.Add(1)
	}
}

// Close stops accepting new connections, closes active sockets, waits for
// background goroutines to exit (bounded by ctx), and releases queued frames.
func (t *ReplicaTransport) Close(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.closedFlag.Store(true)
	listener := t.listener
	cancel := t.cancel
	conns := make([]net.Conn, 0, len(t.conns))
	for conn := range t.conns {
		conns = append(conns, conn)
	}
	routes := make([]*clientRoute, 0, len(t.clients))
	for _, route := range t.clients {
		route.closed.Store(true)
		routes = append(routes, route)
	}
	clear(t.clients)
	t.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	var closeErr error
	if listener != nil {
		if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			closeErr = err
		}
	}
	for _, conn := range conns {
		_ = conn.Close()
	}
	for _, route := range routes {
		_ = route.conn.Close()
	}

	done := make(chan struct{})
	go func() {
		t.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		if closeErr == nil {
			closeErr = ctx.Err()
		}
	}

	// Ensure all concurrent SendReplica/SendClient calls have finished before draining.
	t.sendMu.Lock()
	for index := range t.memberCount {
		if peer := t.peers[index]; peer != nil {
			drainFrameQueue(peer.queue)
		}
	}
	for _, route := range routes {
		drainFrameQueue(route.queue)
	}
	t.sendMu.Unlock()
	return closeErr
}

func (t *ReplicaTransport) acceptLoop(listener net.Listener) {
	defer t.wg.Done()
	for {
		conn, err := listener.Accept()
		if err != nil {
			if t.isClosed() || errors.Is(err, net.ErrClosed) {
				return
			}
			continue
		}
		t.wg.Add(1)
		go t.serveConn(conn)
	}
}

func (t *ReplicaTransport) serveConn(rawConn net.Conn) {
	defer t.wg.Done()
	t.trackConn(rawConn)
	t.metrics.activeConnections.Add(1)
	defer t.metrics.activeConnections.Add(-1)

	conn, identity, err := t.auth.AuthenticateInbound(t.ctx, rawConn)
	if err != nil {
		t.metrics.authFailures.Add(1)
		t.untrackAndClose(rawConn)
		return
	}
	if conn != rawConn {
		t.replaceTrackedConn(rawConn, conn)
	}
	defer t.untrackAndClose(conn)

	if identity.Role == PeerRoleReplica {
		if uint8(identity.Replica) >= t.memberCount ||
			identity.Member != t.config.Membership.Members[identity.Replica] ||
			identity.Replica == t.localIndex {
			t.metrics.authFailures.Add(1)
			return
		}
	} else if identity.Role == PeerRoleClient {
		if identity.Client.IsZero() {
			t.metrics.authFailures.Add(1)
			return
		}
		route, ok := t.registerClientRoute(identity.Client, conn)
		if !ok {
			t.metrics.authFailures.Add(1)
			return
		}
		defer t.unregisterClientRoute(identity.Client, route)
		t.wg.Add(1)
		go t.clientSendLoop(route)
	} else {
		t.metrics.authFailures.Add(1)
		return
	}

	var headerBuf [protocol.HeaderSize]byte
	for {
		frame, header, err := readValidatedFrame(
			conn,
			&headerBuf,
			t.pool,
			t.config.Group,
			t.config.MessageSizeMax,
			t.memberCount,
			t.config.ChecksumAlgorithm,
			&t.metrics,
		)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) && t.config.Logger != nil {
				t.config.Logger.Debug("inbound transport connection closed", "error", err)
			}
			return
		}
		var bindErr error
		if identity.Role == PeerRoleReplica {
			bindErr = frame.BindReplica(identity.Member, identity.Replica)
		} else {
			if !clientFrameMatchesIdentity(&header, identity.Client) {
				frame.Release()
				t.metrics.authFailures.Add(1)
				return
			}
			bindErr = frame.BindClient()
		}
		if bindErr != nil {
			frame.Release()
			t.metrics.authFailures.Add(1)
			return
		}
		t.metrics.framesReceived.Add(1)
		if err := t.config.OnFrame(frame); err != nil {
			frame.Release()
		}
	}
}

func clientFrameMatchesIdentity(header *protocol.Header, authenticated protocol.ClientID) bool {
	var claimed protocol.ClientID
	switch header.Command {
	case protocol.CommandClientPing:
		copy(claimed[:], header.Fields[0:16])
	case protocol.CommandRequest:
		copy(claimed[:], header.Fields[32:48])
	default:
		return false
	}
	return claimed == authenticated
}

func (t *ReplicaTransport) peerSendLoop(peer *outboundPeer) {
	defer t.wg.Done()
	var conn net.Conn
	defer func() {
		if conn != nil {
			t.untrackAndClose(conn)
		}
	}()

	backoff := t.config.BackoffMin
	expected := PeerIdentity{Role: PeerRoleReplica, Member: peer.member, Replica: peer.index}

	for {
		var frame *protocol.Frame
		select {
		case <-t.ctx.Done():
			return
		case frame = <-peer.queue:
		}
		if frame == nil {
			continue
		}
		if conn == nil {
			dialed, err := t.dialPeer(peer.addr, expected)
			if err != nil {
				frame.Release()
				t.metrics.sendDropped.Add(1)
				if !sleepWithContext(t.ctx, backoff) {
					return
				}
				backoff = nextBackoff(backoff, t.config.BackoffMin, t.config.BackoffMax)
				continue
			}
			conn = dialed
			t.trackConn(conn)
			backoff = t.config.BackoffMin
		}
		if err := writeFrame(conn, frame, t.config.WriteTimeout); err != nil {
			frame.Release()
			t.metrics.sendDropped.Add(1)
			t.untrackAndClose(conn)
			conn = nil
			if t.ctx.Err() != nil {
				return
			}
			t.metrics.reconnects.Add(1)
			if !sleepWithContext(t.ctx, backoff) {
				return
			}
			backoff = nextBackoff(backoff, t.config.BackoffMin, t.config.BackoffMax)
			continue
		}
		frame.Release()
		t.metrics.framesSent.Add(1)
	}
}

func (t *ReplicaTransport) dialPeer(addr string, expected PeerIdentity) (net.Conn, error) {
	dialCtx, cancel := context.WithTimeout(t.ctx, t.config.DialTimeout)
	defer cancel()
	rawConn, err := t.dial(dialCtx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := t.auth.AuthenticateOutbound(dialCtx, rawConn, expected)
	if err != nil {
		t.metrics.authFailures.Add(1)
		_ = rawConn.Close()
		return nil, err
	}
	return conn, nil
}

func (t *ReplicaTransport) registerClientRoute(id protocol.ClientID, conn net.Conn) (*clientRoute, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return nil, false
	}
	if _, exists := t.clients[id]; !exists && len(t.clients) >= t.config.MaxClients {
		return nil, false
	}
	if prev := t.clients[id]; prev != nil {
		prev.closed.Store(true)
		_ = prev.conn.Close()
	}
	route := &clientRoute{
		id:    id,
		conn:  conn,
		queue: make(chan *protocol.Frame, t.config.SendQueueSize),
		done:  make(chan struct{}),
	}
	t.clients[id] = route
	return route, true
}

func (t *ReplicaTransport) unregisterClientRoute(id protocol.ClientID, route *clientRoute) {
	route.closed.Store(true)
	close(route.done)
	t.mu.Lock()
	if t.clients[id] == route {
		delete(t.clients, id)
	}
	t.mu.Unlock()
}

func (t *ReplicaTransport) clientSendLoop(route *clientRoute) {
	defer t.wg.Done()
	defer drainFrameQueue(route.queue)
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-route.done:
			return
		case frame := <-route.queue:
			if frame == nil {
				continue
			}
			if err := writeFrame(route.conn, frame, t.config.WriteTimeout); err != nil {
				frame.Release()
				t.metrics.sendDropped.Add(1)
				_ = route.conn.Close()
				return
			}
			frame.Release()
			t.metrics.framesSent.Add(1)
		}
	}
}

func (t *ReplicaTransport) trackConn(conn net.Conn) {
	t.mu.Lock()
	closed := t.closed
	if !closed {
		t.conns[conn] = struct{}{}
	}
	t.mu.Unlock()
	if closed {
		_ = conn.Close()
	}
}

func (t *ReplicaTransport) replaceTrackedConn(oldConn, newConn net.Conn) {
	t.mu.Lock()
	delete(t.conns, oldConn)
	closed := t.closed
	if !closed {
		t.conns[newConn] = struct{}{}
	}
	t.mu.Unlock()
	if closed {
		_ = newConn.Close()
	}
}

func (t *ReplicaTransport) untrackAndClose(conn net.Conn) {
	t.mu.Lock()
	delete(t.conns, conn)
	t.mu.Unlock()
	_ = conn.Close()
}

func (t *ReplicaTransport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func drainFrameQueue(queue chan *protocol.Frame) {
	for {
		select {
		case frame := <-queue:
			if frame != nil {
				frame.Release()
			}
		default:
			return
		}
	}
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
