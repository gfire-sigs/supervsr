package mtls_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"sync"
	"testing"
	"time"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
	"z.gfire.dev/supervsr/transport"
	"z.gfire.dev/supervsr/transport/mtls"
)

type testPKI struct {
	caCert *x509.Certificate
	caKey  *ecdsa.PrivateKey
	pool   *x509.CertPool
}

func newTestPKI(t *testing.T, org string) *testPKI {
	t.Helper()
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{Organization: []string{org}, CommonName: org + "-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	caCert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	return &testPKI{caCert: caCert, caKey: caKey, pool: pool}
}

func (pki *testPKI) issueLeaf(t *testing.T, commonName string) tls.Certificate {
	t.Helper()
	leafKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: commonName},
		DNSNames:     []string{commonName, "localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, pki.caCert, &leafKey.PublicKey, pki.caKey)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: leafKey}
}

func testMembership(localIndex int) replication.Membership {
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

func TestMTLSReplicaAndClientHandshake(t *testing.T) {
	pki := newTestPKI(t, "cluster-a")
	group := protocol.GroupID{7}
	members := testMembership(0).Members
	clientID := protocol.ClientID{99}

	listeners := make([]net.Listener, 2)
	var peerAddrs [replication.MembersMax]string
	for i := range 2 {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[i] = ln
		peerAddrs[i] = ln.Addr().String()
	}

	received := make(chan *protocol.Frame, 4)
	var transports [2]*transport.ReplicaTransport
	for i := range 2 {
		membership := testMembership(i)
		cert := pki.issueLeaf(t, mtls.MemberCommonName(members[i]))
		auth, err := mtls.NewAuthenticator(mtls.Config{
			ServerTLS: &tls.Config{
				Certificates: []tls.Certificate{cert},
				ClientCAs:    pki.pool,
			},
			ClientTLS: &tls.Config{
				Certificates: []tls.Certificate{cert},
				RootCAs:      pki.pool,
				ServerName:   "localhost",
			},
			Membership:    membership,
			HasMembership: true,
			Local: transport.PeerIdentity{
				Role:    transport.PeerRoleReplica,
				Member:  members[i],
				Replica: protocol.ReplicaIndex(i),
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		idx := i
		tr, err := transport.NewReplicaTransport(transport.ReplicaConfig{
			Group:             group,
			Membership:        membership,
			PeerAddrs:         peerAddrs,
			MessageSizeMax:    4096,
			ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
			Authenticator:     auth,
			OnFrame: func(frame *protocol.Frame) error {
				if idx == 1 {
					received <- frame
					return nil
				}
				frame.Release()
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		if err := tr.StartWithListener(t.Context(), listeners[i]); err != nil {
			t.Fatal(err)
		}
		transports[i] = tr
		defer func(target *transport.ReplicaTransport) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			_ = target.Close(ctx)
		}(tr)
	}

	// Replica 0 -> Replica 1 frame authenticated and bound to Member 0 / ReplicaIndex 0.
	pool, err := protocol.NewFramePool(8, 4096)
	if err != nil {
		t.Fatal(err)
	}
	frame, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	header := protocol.Header{
		Group:    group,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandExitView,
		Author:   0,
	}
	if err := frame.Seal(&header, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	transports[0].SendReplica(1, frame)
	frame.Release()

	select {
	case got := <-received:
		source, member, sender := got.Source()
		got.Release()
		if source != protocol.FrameSourceReplica || member != members[0] || sender != 0 {
			t.Fatalf("replica source = (%v, %x, %d), want (replica, %x, 0)", source, member, sender, members[0])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mTLS replica frame")
	}

	// Authenticated client -> Replica 1 frame bound as FrameSourceClient.
	clientCert := pki.issueLeaf(t, mtls.ClientCommonName(clientID))
	clientAuth, err := mtls.NewAuthenticator(mtls.Config{
		ClientTLS: &tls.Config{
			Certificates: []tls.Certificate{clientCert},
			RootCAs:      pki.pool,
			ServerName:   "localhost",
		},
		Local: transport.PeerIdentity{
			Role:   transport.PeerRoleClient,
			Client: clientID,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	clientTr, err := transport.NewClientTransport(transport.ClientConfig{
		Group:             group,
		ClientID:          clientID,
		ActiveCount:       3,
		Members:           members,
		PeerAddrs:         peerAddrs,
		MessageSizeMax:    4096,
		ChecksumAlgorithm: protocol.ChecksumAEGIS128L,
		Authenticator:     clientAuth,
		OnFrame:           func(protocol.ReplicaIndex, []byte) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := clientTr.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = clientTr.Close(ctx)
	}()

	ping, err := pool.Acquire(0)
	if err != nil {
		t.Fatal(err)
	}
	pingHeader := protocol.Header{
		Group:    group,
		Release:  1,
		Protocol: protocol.ProtocolVersion,
		Command:  protocol.CommandClientPing,
		Author:   0,
	}
	copy(pingHeader.Fields[0:16], clientID[:])
	pingHeader.Fields[16] = 1
	if err := ping.Seal(&pingHeader, protocol.ChecksumAEGIS128L); err != nil {
		t.Fatal(err)
	}
	clientTr.SendReplica(1, ping)
	ping.Release()

	select {
	case got := <-received:
		source, _, _ := got.Source()
		got.Release()
		if source != protocol.FrameSourceClient {
			t.Fatalf("client source = %v, want FrameSourceClient", source)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for mTLS client frame")
	}
}

func TestMTLSBlocksImpersonationAndUntrustedCerts(t *testing.T) {
	trustedPKI := newTestPKI(t, "trusted")
	roguePKI := newTestPKI(t, "rogue")
	membership0 := testMembership(0)
	members := membership0.Members

	serverCert0 := trustedPKI.issueLeaf(t, mtls.MemberCommonName(members[0]))
	serverAuth, err := mtls.NewAuthenticator(mtls.Config{
		ServerTLS: &tls.Config{
			Certificates: []tls.Certificate{serverCert0},
			ClientCAs:    trustedPKI.pool,
		},
		Membership:    membership0,
		HasMembership: true,
		Local: transport.PeerIdentity{
			Role:    transport.PeerRoleReplica,
			Member:  members[0],
			Replica: 0,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	handshake := func(clientAuth transport.Authenticator, expected transport.PeerIdentity) (error, error) {
		serverConn, clientConn := net.Pipe()
		defer func() {
			_ = serverConn.Close()
			_ = clientConn.Close()
		}()
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()
		var serverErr, clientErr error
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _, serverErr = serverAuth.AuthenticateInbound(ctx, serverConn)
			_ = serverConn.Close()
		}()
		go func() {
			defer wg.Done()
			_, clientErr = clientAuth.AuthenticateOutbound(ctx, clientConn, expected)
			_ = clientConn.Close()
		}()
		wg.Wait()
		return serverErr, clientErr
	}

	// 1. Untrusted CA certificate must fail server authentication.
	rogueCert := roguePKI.issueLeaf(t, mtls.MemberCommonName(members[1]))
	rogueAuth, err := mtls.NewAuthenticator(mtls.Config{
		ClientTLS: &tls.Config{
			Certificates: []tls.Certificate{rogueCert},
			RootCAs:      trustedPKI.pool,
			ServerName:   "localhost",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if serverErr, _ := handshake(rogueAuth, transport.PeerIdentity{Role: transport.PeerRoleReplica, Member: members[0], Replica: 0}); serverErr == nil {
		t.Fatal("expected untrusted CA certificate to be rejected")
	}

	// 2. Self-impersonation (Member 0 connecting to Member 0) must fail inbound verification.
	selfAuth, err := mtls.NewAuthenticator(mtls.Config{
		ClientTLS: &tls.Config{
			Certificates: []tls.Certificate{serverCert0},
			RootCAs:      trustedPKI.pool,
			ServerName:   "localhost",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if serverErr, _ := handshake(selfAuth, transport.PeerIdentity{Role: transport.PeerRoleReplica, Member: members[0], Replica: 0}); serverErr == nil {
		t.Fatal("expected self-impersonation to be rejected")
	}

	// 3. Outbound peer mismatch (dialing Member 0 while expecting Member 2) must fail outbound verification.
	member1Cert := trustedPKI.issueLeaf(t, mtls.MemberCommonName(members[1]))
	member1Auth, err := mtls.NewAuthenticator(mtls.Config{
		ClientTLS: &tls.Config{
			Certificates: []tls.Certificate{member1Cert},
			RootCAs:      trustedPKI.pool,
			ServerName:   "localhost",
		},
		Membership:    testMembership(1),
		HasMembership: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, clientErr := handshake(member1Auth, transport.PeerIdentity{Role: transport.PeerRoleReplica, Member: members[2], Replica: 2}); clientErr == nil {
		t.Fatal("expected outbound identity mismatch to be rejected")
	}

	// 4. Missing client certificate must fail RequireAndVerifyClientCert.
	noClientCertAuth, err := mtls.NewAuthenticator(mtls.Config{
		ClientTLS: &tls.Config{
			RootCAs:    trustedPKI.pool,
			ServerName: "localhost",
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if serverErr, _ := handshake(noClientCertAuth, transport.PeerIdentity{Role: transport.PeerRoleReplica, Member: members[0], Replica: 0}); serverErr == nil {
		t.Fatal("expected missing client certificate to be rejected")
	}
}
