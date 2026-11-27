// Package transport implements a socket-based TCP MessageBus and inbound frame
// server for VSR replicas and clients.
//
// TLS is intentionally excluded from this package so deployments on isolated
// trusted networks pay zero crypto/tls compile or binary-size overhead. For
// mutual TLS authentication over untrusted networks, supply an authenticator
// from package z.gfire.dev/supervsr/transport/mtls.
package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

const (
	defaultSendQueueSize = 128
	defaultFramePoolSize = 256
	defaultBackoffMin    = 25 * time.Millisecond
	defaultBackoffMax    = 2 * time.Second
	defaultDialTimeout   = 2 * time.Second
	defaultWriteTimeout  = 2 * time.Second
	defaultAuthTimeout   = 2 * time.Second

	plainPrefaceSize    = 40
	plainPrefaceVersion = 1
)

var plainPrefaceMagic = [4]byte{'S', 'V', 'S', 'R'}

var (
	ErrInvalidConfig    = errors.New("transport: invalid configuration")
	ErrAuthentication   = errors.New("transport: peer authentication failed")
	ErrHeaderRejected   = errors.New("transport: frame header rejected")
	ErrBodyRejected     = errors.New("transport: frame body checksum rejected")
	ErrTransportClosed  = errors.New("transport: closed")
	ErrTransportStarted = errors.New("transport: already started")
)

// PeerRole classifies an authenticated transport peer.
type PeerRole uint8

const (
	PeerRoleClient PeerRole = iota + 1
	PeerRoleReplica
)

// PeerIdentity binds an authenticated connection to a replica or client identity.
type PeerIdentity struct {
	Role    PeerRole
	Member  protocol.MemberID
	Replica protocol.ReplicaIndex
	Client  protocol.ClientID
}

// Authenticator authenticates inbound and outbound network connections and binds
// each connection to a verified PeerIdentity.
type Authenticator interface {
	AuthenticateInbound(ctx context.Context, conn net.Conn) (net.Conn, PeerIdentity, error)
	AuthenticateOutbound(ctx context.Context, conn net.Conn, expected PeerIdentity) (net.Conn, error)
}

// DialFunc dials a network address; defaults to net.Dialer.DialContext.
type DialFunc func(ctx context.Context, network, addr string) (net.Conn, error)

// MetricsSnapshot reports bounded-cardinality transport counters.
type MetricsSnapshot struct {
	HeadersRejected   uint64
	BodiesRejected    uint64
	AuthFailures      uint64
	SendDropped       uint64
	PoolExhausted     uint64
	Reconnects        uint64
	FramesSent        uint64
	FramesReceived    uint64
	ActiveConnections int64
}

type transportMetrics struct {
	headersRejected   atomic.Uint64
	bodiesRejected    atomic.Uint64
	authFailures      atomic.Uint64
	sendDropped       atomic.Uint64
	poolExhausted     atomic.Uint64
	reconnects        atomic.Uint64
	framesSent        atomic.Uint64
	framesReceived    atomic.Uint64
	activeConnections atomic.Int64
}

func (metrics *transportMetrics) snapshot() MetricsSnapshot {
	return MetricsSnapshot{
		HeadersRejected:   metrics.headersRejected.Load(),
		BodiesRejected:    metrics.bodiesRejected.Load(),
		AuthFailures:      metrics.authFailures.Load(),
		SendDropped:       metrics.sendDropped.Load(),
		PoolExhausted:     metrics.poolExhausted.Load(),
		Reconnects:        metrics.reconnects.Load(),
		FramesSent:        metrics.framesSent.Load(),
		FramesReceived:    metrics.framesReceived.Load(),
		ActiveConnections: metrics.activeConnections.Load(),
	}
}

// PlainAuthenticator performs a fixed 40-byte identity preface exchange over
// plaintext connections for isolated trusted networks. It verifies GroupID and
// configured Membership without importing crypto/tls.
type PlainAuthenticator struct {
	Group      protocol.GroupID
	Membership replication.Membership
	Local      PeerIdentity
	Timeout    time.Duration
}

func (auth *PlainAuthenticator) AuthenticateInbound(ctx context.Context, conn net.Conn) (net.Conn, PeerIdentity, error) {
	if err := applyAuthDeadline(ctx, conn, auth.Timeout); err != nil {
		return nil, PeerIdentity{}, err
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	var peerPreface [plainPrefaceSize]byte
	if _, err := io.ReadFull(conn, peerPreface[:]); err != nil {
		return nil, PeerIdentity{}, errors.Join(ErrAuthentication, err)
	}
	identity, err := auth.validatePeerPreface(peerPreface[:])
	if err != nil {
		return nil, PeerIdentity{}, err
	}
	var localPreface [plainPrefaceSize]byte
	encodePlainPreface(&localPreface, auth.Group, auth.Local)
	if err := writeAll(conn, localPreface[:]); err != nil {
		return nil, PeerIdentity{}, errors.Join(ErrAuthentication, err)
	}
	return conn, identity, nil
}

func (auth *PlainAuthenticator) AuthenticateOutbound(ctx context.Context, conn net.Conn, expected PeerIdentity) (net.Conn, error) {
	if err := applyAuthDeadline(ctx, conn, auth.Timeout); err != nil {
		return nil, err
	}
	defer func() { _ = conn.SetDeadline(time.Time{}) }()

	var localPreface [plainPrefaceSize]byte
	encodePlainPreface(&localPreface, auth.Group, auth.Local)
	if err := writeAll(conn, localPreface[:]); err != nil {
		return nil, errors.Join(ErrAuthentication, err)
	}
	var peerPreface [plainPrefaceSize]byte
	if _, err := io.ReadFull(conn, peerPreface[:]); err != nil {
		return nil, errors.Join(ErrAuthentication, err)
	}
	actual, err := auth.validateOutboundPeerPreface(peerPreface[:], expected)
	if err != nil {
		return nil, err
	}
	if actual.Role != expected.Role {
		return nil, ErrAuthentication
	}
	if expected.Role == PeerRoleReplica && (actual.Member != expected.Member || actual.Replica != expected.Replica) {
		return nil, ErrAuthentication
	}
	return conn, nil
}

func encodePlainPreface(dst *[plainPrefaceSize]byte, group protocol.GroupID, id PeerIdentity) {
	clear(dst[:])
	copy(dst[0:4], plainPrefaceMagic[:])
	dst[4] = plainPrefaceVersion
	dst[5] = byte(id.Role)
	copy(dst[8:24], group[:])
	switch id.Role {
	case PeerRoleReplica:
		copy(dst[24:40], id.Member[:])
	case PeerRoleClient:
		copy(dst[24:40], id.Client[:])
	}
}

func (auth *PlainAuthenticator) validatePeerPreface(preface []byte) (PeerIdentity, error) {
	if len(preface) != plainPrefaceSize {
		return PeerIdentity{}, ErrAuthentication
	}
	if [4]byte(preface[0:4]) != plainPrefaceMagic || preface[4] != plainPrefaceVersion || preface[6] != 0 || preface[7] != 0 {
		return PeerIdentity{}, ErrAuthentication
	}
	var group protocol.GroupID
	copy(group[:], preface[8:24])
	if group != auth.Group || group.IsZero() {
		return PeerIdentity{}, ErrAuthentication
	}
	switch PeerRole(preface[5]) {
	case PeerRoleReplica:
		var member protocol.MemberID
		copy(member[:], preface[24:40])
		if member.IsZero() {
			return PeerIdentity{}, ErrAuthentication
		}
		index, ok := lookupMemberIndex(auth.Membership, member)
		if !ok {
			return PeerIdentity{}, ErrAuthentication
		}
		if auth.Local.Role == PeerRoleReplica && member == auth.Local.Member {
			return PeerIdentity{}, ErrAuthentication
		}
		return PeerIdentity{Role: PeerRoleReplica, Member: member, Replica: index}, nil
	case PeerRoleClient:
		var client protocol.ClientID
		copy(client[:], preface[24:40])
		if client.IsZero() {
			return PeerIdentity{}, ErrAuthentication
		}
		return PeerIdentity{Role: PeerRoleClient, Client: client}, nil
	default:
		return PeerIdentity{}, ErrAuthentication
	}
}

func (auth *PlainAuthenticator) validateOutboundPeerPreface(preface []byte, expected PeerIdentity) (PeerIdentity, error) {
	if auth.Local.Role == PeerRoleReplica {
		return auth.validatePeerPreface(preface)
	}
	if len(preface) != plainPrefaceSize {
		return PeerIdentity{}, ErrAuthentication
	}
	if [4]byte(preface[0:4]) != plainPrefaceMagic || preface[4] != plainPrefaceVersion || preface[6] != 0 || preface[7] != 0 {
		return PeerIdentity{}, ErrAuthentication
	}
	var group protocol.GroupID
	copy(group[:], preface[8:24])
	if group != auth.Group || group.IsZero() {
		return PeerIdentity{}, ErrAuthentication
	}
	if PeerRole(preface[5]) != PeerRoleReplica {
		return PeerIdentity{}, ErrAuthentication
	}
	var member protocol.MemberID
	copy(member[:], preface[24:40])
	if member.IsZero() {
		return PeerIdentity{}, ErrAuthentication
	}
	// When the client knows the target MemberID, verify exact match; otherwise accept any non-zero member in Group.
	if !expected.Member.IsZero() && member != expected.Member {
		return PeerIdentity{}, ErrAuthentication
	}
	return PeerIdentity{Role: PeerRoleReplica, Member: member, Replica: expected.Replica}, nil
}

func lookupMemberIndex(membership replication.Membership, member protocol.MemberID) (protocol.ReplicaIndex, bool) {
	total := int(membership.ActiveCount) + int(membership.StandbyCount)
	for index := range total {
		if membership.Members[index] == member {
			return protocol.ReplicaIndex(index), true
		}
	}
	return 0, false
}

func applyAuthDeadline(ctx context.Context, conn net.Conn, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = defaultAuthTimeout
	}
	deadline := time.Now().Add(timeout)
	if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
		deadline = ctxDeadline
	}
	return conn.SetDeadline(deadline)
}

// ReadValidatedFrame enforces the memory-safe receive order:
//  1. read fixed 256-byte header into scratchHeader;
//  2. validate HeaderChecksum, GroupID, Size, ProtocolVersion, Epoch, Command, Author, and reservations;
//  3. acquire pooled frame buffer for Size bytes and read Size-256 body bytes;
//  4. verify BodyChecksum.
//
// Malformed headers are rejected with zero pool allocation and zero body read.
func readValidatedFrame(
	reader io.Reader,
	scratchHeader *[protocol.HeaderSize]byte,
	pool *protocol.FramePool,
	expectedGroup protocol.GroupID,
	messageSizeMax uint32,
	memberCount uint8,
	algorithm protocol.ChecksumAlgorithm,
	metrics *transportMetrics,
) (*protocol.Frame, protocol.Header, error) {
	if _, err := io.ReadFull(reader, scratchHeader[:]); err != nil {
		return nil, protocol.Header{}, err
	}
	header, reason := protocol.DecodeHeader(scratchHeader[:], expectedGroup, messageSizeMax, memberCount, algorithm)
	if reason != protocol.RejectNone {
		if metrics != nil {
			metrics.headersRejected.Add(1)
		}
		return nil, protocol.Header{}, ErrHeaderRejected
	}
	frame, err := pool.ReadEncoded(scratchHeader[:], header.Size, reader)
	if err != nil {
		if metrics != nil && errors.Is(err, protocol.ErrFramePoolEmpty) {
			metrics.poolExhausted.Add(1)
		}
		return nil, protocol.Header{}, err
	}
	raw, err := frame.Bytes()
	if err != nil {
		frame.Release()
		return nil, protocol.Header{}, err
	}
	if protocol.ValidateBody(&header, raw[protocol.HeaderSize:], algorithm) != protocol.RejectNone {
		frame.Release()
		if metrics != nil {
			metrics.bodiesRejected.Add(1)
		}
		return nil, protocol.Header{}, ErrBodyRejected
	}
	return frame, header, nil
}

func writeAll(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func writeFrame(conn net.Conn, frame *protocol.Frame, timeout time.Duration) error {
	raw, err := frame.Bytes()
	if err != nil {
		return err
	}
	if timeout > 0 {
		if err := conn.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}
	}
	return writeAll(conn, raw)
}

func nextBackoff(current, minimum, maximum time.Duration) time.Duration {
	if current < minimum {
		return minimum
	}
	next := current * 2
	if next > maximum || next <= 0 {
		return maximum
	}
	return next
}
