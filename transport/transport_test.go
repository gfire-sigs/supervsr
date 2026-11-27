package transport

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"go/parser"
	"go/token"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
)

func testClusterMembership(localIndex int) replication.Membership {
	var members [replication.MembersMax]protocol.MemberID
	for i := range 3 {
		members[i] = protocol.MemberID{byte(i + 1)}
	}
	return replication.Membership{
		ActiveCount: 3,
		Members:     members,
		LocalMember: members[localIndex],
	}
}

func makeSignedHeaderWithFields(group protocol.GroupID, size uint32, epoch protocol.Epoch, protoVersion uint16, command protocol.Command, author protocol.ReplicaIndex) [protocol.HeaderSize]byte {
	var raw [protocol.HeaderSize]byte
	copy(raw[80:96], group[:])
	binary.LittleEndian.PutUint32(raw[96:100], size)
	binary.LittleEndian.PutUint32(raw[100:104], uint32(epoch))
	binary.LittleEndian.PutUint16(raw[112:114], protoVersion)
	raw[114] = byte(command)
	raw[115] = byte(author)
	checksum, err := protocol.ChecksumBytes(protocol.ChecksumAEGIS128L, raw[protocol.HeaderChecksumFrom:])
	if err != nil {
		panic(err)
	}
	copy(raw[:16], checksum[:])
	return raw
}

func TestTransportExcludesCryptoTLS(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, spec := range file.Imports {
			path := strings.Trim(spec.Path.Value, `"`)
			if path == "crypto/tls" || path == "crypto/x509" {
				t.Fatalf("%s imports %q; package transport must stay TLS-free so callers can exclude crypto/tls", name, path)
			}
		}
	}
}

func TestTransportMalformedHeaderRejectedZeroAlloc(t *testing.T) {
	group := protocol.GroupID{11}
	pool, err := protocol.NewFramePool(4, 4096)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name   string
		header [protocol.HeaderSize]byte
	}{
		{
			name:   "oversized_size_with_valid_checksum",
			header: makeSignedHeaderWithFields(group, 0xFFFFFFFF, 0, protocol.ProtocolVersion, protocol.CommandExitView, 0),
		},
		{
			name:   "undersized_header",
			header: makeSignedHeaderWithFields(group, 128, 0, protocol.ProtocolVersion, protocol.CommandExitView, 0),
		},
		{
			name:   "wrong_group",
			header: makeSignedHeaderWithFields(protocol.GroupID{99}, protocol.HeaderSize, 0, protocol.ProtocolVersion, protocol.CommandExitView, 0),
		},
		{
			name:   "nonzero_epoch",
			header: makeSignedHeaderWithFields(group, protocol.HeaderSize, 1, protocol.ProtocolVersion, protocol.CommandExitView, 0),
		},
		{
			name:   "bad_protocol_version",
			header: makeSignedHeaderWithFields(group, protocol.HeaderSize, 0, 7, protocol.CommandExitView, 0),
		},
		{
			name: "bad_header_checksum",
			header: func() [protocol.HeaderSize]byte {
				h := makeSignedHeaderWithFields(group, protocol.HeaderSize, 0, protocol.ProtocolVersion, protocol.CommandExitView, 0)
				h[0] ^= 0xFF
				return h
			}(),
		},
	}

	// Trailing body bytes must never be read when header validation rejects the frame.
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			wire := make([]byte, protocol.HeaderSize+64)
			copy(wire[:protocol.HeaderSize], tc.header[:])
			reader := bytes.NewReader(wire)
			var scratch [protocol.HeaderSize]byte
			var metrics transportMetrics

			allocs := testing.AllocsPerRun(500, func() {
				reader.Reset(wire)
				frame, _, readErr := readValidatedFrame(reader, &scratch, pool, group, 4096, 3, protocol.ChecksumAEGIS128L, &metrics)
				if !errors.Is(readErr, ErrHeaderRejected) || frame != nil {
					panic("expected ErrHeaderRejected without frame allocation")
				}
				if reader.Len() != 64 {
					panic("body bytes were read despite rejected header")
				}
			})
			if allocs != 0 {
				t.Fatalf("allocs = %f, want 0", allocs)
			}
			if pool.Available() != pool.Capacity() {
				t.Fatalf("pool available = %d, want %d", pool.Available(), pool.Capacity())
			}
			if metrics.headersRejected.Load() == 0 {
				t.Fatalf("headersRejected = 0, want > 0")
			}
		})
	}

	// Live TCP server must reject a malicious Size=0xFFFFFFFF header without allocating or wedging.
	membership0 := testClusterMembership(0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan *protocol.Frame, 2)
	server, err := NewReplicaTransport(ReplicaConfig{
		Group:             group,
		Membership:        membership0,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		FramePoolSize:     4,
		OnFrame: func(frame *protocol.Frame) error {
			received <- frame
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.StartWithListener(t.Context(), ln); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Close(ctx)
	}()

	rawConn, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	clientAuth := &PlainAuthenticator{
		Group:      group,
		Membership: testClusterMembership(1),
		Local:      PeerIdentity{Role: PeerRoleReplica, Member: membership0.Members[1], Replica: 1},
		Timeout:    time.Second,
	}
	conn, err := clientAuth.AuthenticateOutbound(t.Context(), rawConn, PeerIdentity{
		Role:    PeerRoleReplica,
		Member:  membership0.Members[0],
		Replica: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	badHeader := makeSignedHeaderWithFields(group, 0xFFFFFFFF, 0, protocol.ProtocolVersion, protocol.CommandExitView, 1)
	if err := writeAll(conn, badHeader[:]); err != nil {
		t.Fatal(err)
	}
	// Server must close the connection immediately upon rejecting the header.
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var probe [1]byte
	if _, err := conn.Read(probe[:]); err == nil {
		t.Fatal("expected server to close connection after malformed header")
	}
	_ = conn.Close()

	waitCondition(t, time.Second, func() bool {
		return server.Metrics().HeadersRejected >= 1
	}, "HeadersRejected metric did not increment")
	if server.pool.Available() != server.pool.Capacity() {
		t.Fatalf("server pool available = %d, want %d", server.pool.Available(), server.pool.Capacity())
	}
}

func TestTransportIdentityImpersonationBlocked(t *testing.T) {
	group := protocol.GroupID{21}
	membership0 := testClusterMembership(0)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	received := make(chan *protocol.Frame, 4)
	server, err := NewReplicaTransport(ReplicaConfig{
		Group:             group,
		Membership:        membership0,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		OnFrame: func(frame *protocol.Frame) error {
			received <- frame
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.StartWithListener(t.Context(), ln); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = server.Close(ctx)
	}()

	// 1. Rogue member ID rejected at authentication.
	tryBadPreface := func(g protocol.GroupID, id PeerIdentity) {
		t.Helper()
		conn, dialErr := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		defer func() { _ = conn.Close() }()
		var preface [plainPrefaceSize]byte
		encodePlainPreface(&preface, g, id)
		if err := writeAll(conn, preface[:]); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(time.Second))
		var reply [plainPrefaceSize]byte
		if _, err := conn.Read(reply[:]); err == nil {
			t.Fatal("expected server to reject spoofed preface")
		}
	}

	tryBadPreface(group, PeerIdentity{Role: PeerRoleReplica, Member: protocol.MemberID{99}})
	tryBadPreface(protocol.GroupID{88}, PeerIdentity{Role: PeerRoleReplica, Member: membership0.Members[1]})
	tryBadPreface(group, PeerIdentity{Role: PeerRoleReplica, Member: membership0.Members[0]}) // self-impersonation

	waitCondition(t, time.Second, func() bool {
		return server.Metrics().AuthFailures >= 3
	}, "expected AuthFailures >= 3 for rejected prefaces")

	// 2. Client attempting to send a replica-only command or impersonate another ClientID.
	clientA := protocol.ClientID{10}
	clientB := protocol.ClientID{20}
	dialClient := func(id protocol.ClientID) net.Conn {
		t.Helper()
		raw, dialErr := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
		if dialErr != nil {
			t.Fatal(dialErr)
		}
		auth := &PlainAuthenticator{
			Group:   group,
			Local:   PeerIdentity{Role: PeerRoleClient, Client: id},
			Timeout: time.Second,
		}
		conn, authErr := auth.AuthenticateOutbound(t.Context(), raw, PeerIdentity{
			Role:    PeerRoleReplica,
			Member:  membership0.Members[0],
			Replica: 0,
		})
		if authErr != nil {
			t.Fatal(authErr)
		}
		return conn
	}

	// 2a. Client sending CommandExitView (replica command) must be rejected.
	c1 := dialClient(clientA)
	var exitRaw [protocol.HeaderSize]byte
	exitHeader := protocol.Header{Group: group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: 0}
	if err := protocol.SealFrame(exitRaw[:], &exitHeader, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(c1, exitRaw[:]); err != nil {
		t.Fatal(err)
	}
	_ = c1.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := c1.Read(b[:]); err == nil {
		t.Fatal("expected connection close on replica command from client")
	}
	_ = c1.Close()

	// 2b. ClientA sending CommandClientPing claiming ClientB must be rejected.
	c2 := dialClient(clientA)
	var spoofPing [protocol.HeaderSize]byte
	pingHeader := protocol.Header{Group: group, Release: 1, Protocol: protocol.ProtocolVersion, Command: protocol.CommandClientPing, Author: 0}
	copy(pingHeader.Fields[0:16], clientB[:])
	pingHeader.Fields[16] = 1
	if err := protocol.SealFrame(spoofPing[:], &pingHeader, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(c2, spoofPing[:]); err != nil {
		t.Fatal(err)
	}
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c2.Read(b[:]); err == nil {
		t.Fatal("expected connection close on spoofed client ID")
	}
	_ = c2.Close()

	waitCondition(t, time.Second, func() bool {
		return server.Metrics().AuthFailures >= 5
	}, "expected AuthFailures >= 5 after client spoofing attempts")
	if len(received) != 0 {
		t.Fatalf("received %d spoofed client frames, want 0", len(received))
	}

	// 3. Authenticated Replica 1 sending a frame claiming Author=0 binds to Replica 1 so protocol semantics reject impersonation.
	raw, err := net.DialTimeout("tcp", ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	replicaAuth := &PlainAuthenticator{
		Group:      group,
		Membership: testClusterMembership(1),
		Local:      PeerIdentity{Role: PeerRoleReplica, Member: membership0.Members[1], Replica: 1},
		Timeout:    time.Second,
	}
	conn, err := replicaAuth.AuthenticateOutbound(t.Context(), raw, PeerIdentity{
		Role:    PeerRoleReplica,
		Member:  membership0.Members[0],
		Replica: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	var forged [protocol.HeaderSize]byte
	forgedHeader := protocol.Header{Group: group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: 0}
	if err := protocol.SealFrame(forged[:], &forgedHeader, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	if err := writeAll(conn, forged[:]); err != nil {
		t.Fatal(err)
	}
	select {
	case frame := <-received:
		source, member, sender := frame.Source()
		rawBytes, bytesErr := frame.Bytes()
		if bytesErr != nil {
			t.Fatal(bytesErr)
		}
		hdr, body, decodeReason := protocol.DecodeFrame(rawBytes, group, 4096, 3, protocol.ChecksumAEGIS128L)
		frame.Release()
		if decodeReason != protocol.RejectNone {
			t.Fatalf("decodeReason = %v, want RejectNone", decodeReason)
		}
		if source != protocol.FrameSourceReplica || member != membership0.Members[1] || sender != 1 {
			t.Fatalf("bound source = (%v, %x, %d), want Replica 1", source, member, sender)
		}
		reason := protocol.ValidateSemantics(&hdr, body, protocol.ValidationContext{
			Authenticated:     true,
			ReplicaSource:     true,
			Sender:            sender,
			ActiveCount:       3,
			MemberCount:       3,
			CurrentRelease:    1,
			Group:             group,
			MessageSizeMax:    4096,
			ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		})
		if reason != protocol.RejectAuthor {
			t.Fatalf("ValidateSemantics reason = %v, want RejectAuthor", reason)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for bound replica frame")
	}
}

func TestTransportReconnectAfterPeerRestart(t *testing.T) {
	group := protocol.GroupID{31}
	ln0, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr1 := ln1.Addr().String()
	var addrs [replication.MembersMax]string
	addrs[0] = ln0.Addr().String()
	addrs[1] = addr1

	tr0, err := NewReplicaTransport(ReplicaConfig{
		Group:             group,
		Membership:        testClusterMembership(0),
		PeerAddrs:         addrs,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        40 * time.Millisecond,
		DialTimeout:       200 * time.Millisecond,
		WriteTimeout:      200 * time.Millisecond,
		OnFrame: func(frame *protocol.Frame) error {
			frame.Release()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr0.StartWithListener(t.Context(), ln0); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tr0.Close(ctx)
	}()

	startPeer1 := func(listener net.Listener) (*ReplicaTransport, chan *protocol.Frame) {
		ch := make(chan *protocol.Frame, 8)
		tr, trErr := NewReplicaTransport(ReplicaConfig{
			Group:             group,
			Membership:        testClusterMembership(1),
			PeerAddrs:         addrs,
			MessageSizeMax:    4096,
			ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
			OnFrame: func(frame *protocol.Frame) error {
				ch <- frame
				return nil
			},
		})
		if trErr != nil {
			t.Fatal(trErr)
		}
		if startErr := tr.StartWithListener(t.Context(), listener); startErr != nil {
			t.Fatal(startErr)
		}
		return tr, ch
	}

	tr1a, rx1a := startPeer1(ln1)
	pool, err := protocol.NewFramePool(8, 4096)
	if err != nil {
		t.Fatal(err)
	}
	sendExitView := func(view protocol.View) {
		f, acqErr := pool.Acquire(0)
		if acqErr != nil {
			t.Fatal(acqErr)
		}
		h := protocol.Header{Group: group, View: view, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: 0}
		if sealErr := f.Seal(&h, protocol.ChecksumAEGIS128L); sealErr != nil {
			t.Fatal(sealErr)
		}
		tr0.SendReplica(1, f)
		f.Release()
	}

	sendExitView(1)
	select {
	case f := <-rx1a:
		f.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("initial frame not delivered")
	}

	// Shut down peer 1 and trigger a broken connection on tr0.
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	if err := tr1a.Close(closeCtx); err != nil {
		cancelClose()
		t.Fatal(err)
	}
	cancelClose()

	// Send frames until tr0 observes the disconnect and increments Reconnects.
	waitCondition(t, 2*time.Second, func() bool {
		sendExitView(2)
		return tr0.Metrics().Reconnects >= 1
	}, "expected tr0 to observe disconnect and schedule reconnect")

	// Restart peer 1 on the exact same address and confirm delivery resumes.
	ln1b, err := net.Listen("tcp", addr1)
	if err != nil {
		t.Fatal(err)
	}
	tr1b, rx1b := startPeer1(ln1b)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tr1b.Close(ctx)
	}()

	waitCondition(t, 2*time.Second, func() bool {
		sendExitView(3)
		select {
		case f := <-rx1b:
			f.Release()
			return true
		default:
			return false
		}
	}, "expected tr0 to reconnect and deliver frames after peer restart")
}

var (
	errInjectedDisconnect = errors.New("injected one-way disconnect")
	errInjectedDial       = errors.New("injected dial failure")
)

type faultConn struct {
	net.Conn
	cut *atomic.Bool
}

func (c *faultConn) Write(p []byte) (int, error) {
	if c.cut.Load() {
		return 0, errInjectedDisconnect
	}
	return c.Conn.Write(p)
}

func TestTransportAsymmetricOneWayDisconnect(t *testing.T) {
	group := protocol.GroupID{41}
	ln0, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ln1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var addrs [replication.MembersMax]string
	addrs[0] = ln0.Addr().String()
	addrs[1] = ln1.Addr().String()

	var cut0to1 atomic.Bool
	rx0 := make(chan *protocol.Frame, 16)
	rx1 := make(chan *protocol.Frame, 16)

	tr0, err := NewReplicaTransport(ReplicaConfig{
		Group:             group,
		Membership:        testClusterMembership(0),
		PeerAddrs:         addrs,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		SendQueueSize:     4,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        25 * time.Millisecond,
		DialTimeout:       200 * time.Millisecond,
		WriteTimeout:      200 * time.Millisecond,
		Dial: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if cut0to1.Load() {
				return nil, errInjectedDial
			}
			var d net.Dialer
			c, dialErr := d.DialContext(ctx, network, addr)
			if dialErr != nil {
				return nil, dialErr
			}
			return &faultConn{Conn: c, cut: &cut0to1}, nil
		},
		OnFrame: func(frame *protocol.Frame) error {
			rx0 <- frame
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	tr1, err := NewReplicaTransport(ReplicaConfig{
		Group:             group,
		Membership:        testClusterMembership(1),
		PeerAddrs:         addrs,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		SendQueueSize:     4,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        25 * time.Millisecond,
		OnFrame: func(frame *protocol.Frame) error {
			rx1 <- frame
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := tr0.StartWithListener(t.Context(), ln0); err != nil {
		t.Fatal(err)
	}
	if err := tr1.StartWithListener(t.Context(), ln1); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tr0.Close(ctx)
		_ = tr1.Close(ctx)
	}()

	pool, err := protocol.NewFramePool(16, 4096)
	if err != nil {
		t.Fatal(err)
	}
	send := func(tr *ReplicaTransport, from, to protocol.ReplicaIndex) {
		f, acqErr := pool.Acquire(0)
		if acqErr != nil {
			t.Fatal(acqErr)
		}
		h := protocol.Header{Group: group, Protocol: protocol.ProtocolVersion, Command: protocol.CommandExitView, Author: from}
		if sealErr := f.Seal(&h, protocol.ChecksumAEGIS128L); sealErr != nil {
			t.Fatal(sealErr)
		}
		tr.SendReplica(to, f)
		f.Release()
	}

	// Establish healthy bidirectional delivery.
	send(tr0, 0, 1)
	send(tr1, 1, 0)
	select {
	case f := <-rx1:
		f.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("0->1 initial frame missing")
	}
	select {
	case f := <-rx0:
		f.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("1->0 initial frame missing")
	}

	// Cut 0 -> 1 only; 1 -> 0 must stay operational while 0 -> 1 drops cleanly without blocking.
	cut0to1.Store(true)
	for range 12 {
		send(tr0, 0, 1)
	}
	send(tr1, 1, 0)
	select {
	case f := <-rx0:
		f.Release()
	case <-time.After(2 * time.Second):
		t.Fatal("1->0 failed during asymmetric 0->1 disconnect")
	}
	waitCondition(t, 2*time.Second, func() bool {
		return tr0.Metrics().SendDropped > 0
	}, "expected tr0 SendDropped > 0 during one-way disconnect")

	// Heal 0 -> 1 and verify recovery and zero pool leak after close.
	cut0to1.Store(false)
	waitCondition(t, 2*time.Second, func() bool {
		send(tr0, 0, 1)
		select {
		case f := <-rx1:
			f.Release()
			return true
		default:
			return false
		}
	}, "expected 0->1 delivery to resume after healing one-way disconnect")

	closeCtx, cancelClose := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancelClose()
	_ = tr0.Close(closeCtx)
	_ = tr1.Close(closeCtx)
	if pool.Available() != pool.Capacity() {
		t.Fatalf("caller pool leaked frames: available=%d capacity=%d", pool.Available(), pool.Capacity())
	}
}

type memStorage struct {
	mu      sync.Mutex
	working []byte
	durable []byte
}

func (s *memStorage) ReadAt(dst []byte, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy(dst, s.working[offset:offset+uint64(len(dst))])
	return nil
}

func (s *memStorage) WriteAt(src []byte, offset uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy(s.working[offset:offset+uint64(len(src))], src)
	return nil
}

func (s *memStorage) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	copy(s.durable, s.working)
	return nil
}

func (s *memStorage) Resize(size uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if uint64(len(s.working)) == size {
		return nil
	}
	next := make([]byte, int(size))
	copy(next, s.working)
	s.working = next
	durable := make([]byte, int(size))
	copy(durable, s.durable)
	s.durable = durable
	return nil
}

func (s *memStorage) Size() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.working)), nil
}

func (s *memStorage) SyncParent() error { return nil }
func (s *memStorage) Close() error      { return nil }

type echoStateMachine struct {
	capacities replication.StateMachineCapacities
}

func (m *echoStateMachine) Capacities() replication.StateMachineCapacities { return m.capacities }
func (m *echoStateMachine) Validate(replication.ValidateInput) replication.ValidationResult {
	return replication.ValidationOK
}
func (m *echoStateMachine) PulseNeeded(uint64) bool { return false }
func (m *echoStateMachine) StartPrefetch(replication.PrefetchInput, *replication.SMCompletion) (replication.StartResult[replication.PrefetchToken], error) {
	return replication.Ready(replication.PrefetchToken(1)), nil
}
func (m *echoStateMachine) Commit(input replication.CommitInput, _ replication.PrefetchToken, reply []byte) (int, error) {
	n := copy(reply, "echo:")
	n += copy(reply[n:], input.Body)
	return n, nil
}
func (m *echoStateMachine) StartCompact(replication.CompactInput, *replication.SMCompletion) (replication.StartResult[replication.CompactResult], error) {
	return replication.Ready(replication.CompactResult{}), nil
}
func (m *echoStateMachine) StartCheckpoint(replication.CheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.CheckpointManifest], error) {
	return replication.Ready(replication.CheckpointManifest{}), nil
}
func (m *echoStateMachine) StartOpen(replication.OpenCheckpointInput, *replication.SMCompletion) (replication.StartResult[replication.OpenResult], error) {
	return replication.Ready(replication.OpenResult{}), nil
}
func (m *echoStateMachine) StartReset(*replication.SMCompletion) (replication.StartResult[replication.ResetResult], error) {
	return replication.Ready(replication.ResetResult{}), nil
}
func (m *echoStateMachine) Close() error { return nil }

type testClock struct {
	start time.Time
}

func (c testClock) Now() replication.TimeSample {
	now := time.Now()
	mono := now.Sub(c.start)
	if mono <= 0 {
		mono = 1
	}
	return replication.TimeSample{
		Wall:         uint64(now.UnixNano()),
		Monotonic:    uint64(mono),
		Synchronized: true,
	}
}

func TestTransportLoopbackClusterAndClient(t *testing.T) {
	group := protocol.GroupID{55}
	cluster := replication.DefaultClusterConfig()
	cluster.MessageSizeMax = 8192
	cluster.ApplicationBatchSizeMax = 1024
	cluster.ApplicationReplySizeMax = 1024
	cluster.JournalSlots = 256
	cluster.BlockSize = 4096
	cluster.ClientsMax = 4
	cluster.PipelineMax = 4
	cluster.ViewChangeHeadersSuffixMax = 5
	cluster.CompactionOps = 16

	process := replication.DefaultProcessConfig()
	process.Tick = 5 * time.Millisecond
	clock := testClock{start: time.Now().Add(-time.Second)}

	var listeners [3]net.Listener
	var addrs [replication.MembersMax]string
	for i := range 3 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		addrs[i] = ln.Addr().String()
	}

	runCtx, cancelRun := context.WithCancel(t.Context())
	defer cancelRun()

	var replicas [3]*replication.Replica
	var transports [3]*ReplicaTransport
	var runWG sync.WaitGroup

	for i := range 3 {
		membership := testClusterMembership(i)
		storage := &memStorage{}
		if err := replication.Format(t.Context(), replication.FormatConfig{
			Group:          group,
			Membership:     membership,
			Cluster:        cluster,
			CurrentRelease: 1,
		}, replication.FormatDependencies{Storage: storage}); err != nil {
			t.Fatal(err)
		}

		idx := i
		var tr *ReplicaTransport
		tr, err := NewReplicaTransport(ReplicaConfig{
			Group:             group,
			Membership:        membership,
			PeerAddrs:         addrs,
			MessageSizeMax:    uint32(cluster.MessageSizeMax),
			ChecksumAlgorithm: cluster.ChecksumAlgorithm,
			BackoffMin:        10 * time.Millisecond,
			BackoffMax:        50 * time.Millisecond,
			OnFrame: func(frame *protocol.Frame) error {
				if replicas[idx] == nil {
					return ErrTransportClosed
				}
				return replicas[idx].Submit(frame)
			},
		})
		if err != nil {
			t.Fatal(err)
		}

		sm := &echoStateMachine{
			capacities: replication.StateMachineCapacities{
				RequestBytes:  uint32(cluster.ApplicationBatchSizeMax),
				ReplyBytes:    uint32(cluster.ApplicationReplySizeMax),
				PrefetchMax:   4,
				CheckpointMax: 4,
			},
		}
		replica, err := replication.Open(t.Context(), replication.Config{
			Group:            group,
			Membership:       membership,
			Cluster:          cluster,
			Process:          process,
			CurrentRelease:   1,
			ClientReleaseMin: 1,
		}, replication.Dependencies{
			Storage:      storage,
			MessageBus:   tr,
			Clock:        clock,
			Entropy:      rand.Reader,
			StateMachine: sm,
		})
		if err != nil {
			t.Fatal(err)
		}
		replicas[i] = replica
		transports[i] = tr
		if err := tr.StartWithListener(runCtx, listeners[i]); err != nil {
			t.Fatal(err)
		}
		runWG.Add(1)
		go func(r *replication.Replica) {
			defer runWG.Done()
			_ = r.Run(runCtx)
		}(replica)
	}

	clientID := protocol.ClientID{77}
	var managed *replication.ManagedClient
	clientTr, err := NewClientTransport(ClientConfig{
		Group:             group,
		ClientID:          clientID,
		ActiveCount:       3,
		Members:           testClusterMembership(0).Members,
		PeerAddrs:         addrs,
		MessageSizeMax:    uint32(cluster.MessageSizeMax),
		ChecksumAlgorithm: cluster.ChecksumAlgorithm,
		BackoffMin:        10 * time.Millisecond,
		BackoffMax:        50 * time.Millisecond,
		OnFrame: func(from protocol.ReplicaIndex, raw []byte) {
			if managed != nil {
				managed.HandleFrame(from, raw)
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := clientTr.Start(runCtx); err != nil {
		t.Fatal(err)
	}
	managed, err = replication.NewManagedClient(replication.ClientConfig{
		Group:             group,
		ID:                clientID,
		Release:           1,
		ActiveCount:       3,
		MessageSizeMax:    uint32(cluster.MessageSizeMax),
		ChecksumAlgorithm: cluster.ChecksumAlgorithm,
		Process:           process,
	}, clientTr, clock, rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	opCtx, cancelOp := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelOp()
	if err := managed.Register(opCtx); err != nil {
		t.Fatalf("Register over TCP transport failed: %v", err)
	}
	reply, err := managed.Submit(opCtx, protocol.OperationApplicationMin, []byte("hello-vsr"))
	if err != nil {
		t.Fatalf("Submit over TCP transport failed: %v", err)
	}
	if string(reply.Body) != "echo:hello-vsr" {
		t.Fatalf("reply = %q, want %q", string(reply.Body), "echo:hello-vsr")
	}

	managed.Close()
	closeCtx, cancelClose := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelClose()
	if err := clientTr.Close(closeCtx); err != nil {
		t.Fatalf("client transport close: %v", err)
	}
	cancelRun()
	for i := range 3 {
		_ = replicas[i].Close(closeCtx)
		_ = transports[i].Close(closeCtx)
	}
	runWG.Wait()
}

func waitCondition(t *testing.T, timeout time.Duration, check func() bool, msg string) {
	t.Helper()
	if check() {
		return
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-deadline.C:
			t.Fatal(msg)
		case <-ticker.C:
			if check() {
				return
			}
		}
	}
}
