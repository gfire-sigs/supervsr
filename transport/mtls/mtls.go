// Package mtls provides an optional mutual-TLS Authenticator for package
// transport. Importing only z.gfire.dev/supervsr/transport excludes crypto/tls
// and crypto/x509 from the compiled binary.
package mtls

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"errors"
	"net"
	"strings"

	"z.gfire.dev/supervsr/replication"
	"z.gfire.dev/supervsr/replication/protocol"
	"z.gfire.dev/supervsr/transport"
)

const (
	memberPrefix = "supervsr-member-"
	clientPrefix = "supervsr-client-"
)

// CertIdentityFunc extracts a PeerIdentity from a verified peer leaf certificate.
type CertIdentityFunc func(cert *x509.Certificate) (transport.PeerIdentity, error)

// Config configures mutual TLS authentication and certificate-to-identity binding.
type Config struct {
	ServerTLS     *tls.Config
	ClientTLS     *tls.Config
	Membership    replication.Membership
	HasMembership bool
	Local         transport.PeerIdentity
	IdentifyCert  CertIdentityFunc
}

// Authenticator implements transport.Authenticator using TLS 1.3 mutual
// certificate authentication.
type Authenticator struct {
	serverTLS     *tls.Config
	clientTLS     *tls.Config
	membership    replication.Membership
	hasMembership bool
	local         transport.PeerIdentity
	identify      CertIdentityFunc
}

// NewAuthenticator validates config and returns an mTLS Authenticator.
// ServerTLS and ClientTLS are cloned with TLS 1.3 minimum version and strict
// client certificate verification enforced.
func NewAuthenticator(config Config) (*Authenticator, error) {
	if config.ServerTLS == nil && config.ClientTLS == nil {
		return nil, transport.ErrInvalidConfig
	}
	if config.HasMembership {
		if err := config.Membership.Validate(); err != nil {
			return nil, errors.Join(transport.ErrInvalidConfig, err)
		}
	}
	var serverTLS *tls.Config
	if config.ServerTLS != nil {
		serverTLS = config.ServerTLS.Clone()
		serverTLS.MinVersion = tls.VersionTLS13
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	}
	var clientTLS *tls.Config
	if config.ClientTLS != nil {
		clientTLS = config.ClientTLS.Clone()
		clientTLS.MinVersion = tls.VersionTLS13
	}
	auth := &Authenticator{
		serverTLS:     serverTLS,
		clientTLS:     clientTLS,
		membership:    config.Membership,
		hasMembership: config.HasMembership,
		local:         config.Local,
		identify:      config.IdentifyCert,
	}
	if auth.identify == nil {
		auth.identify = auth.defaultIdentifyCert
	}
	return auth, nil
}

// MemberCommonName returns the default certificate CommonName/DNSName encoding
// recognized by Authenticator for a cluster MemberID.
func MemberCommonName(member protocol.MemberID) string {
	return memberPrefix + hex.EncodeToString(member[:])
}

// ClientCommonName returns the default certificate CommonName/DNSName encoding
// recognized by Authenticator for a ClientID.
func ClientCommonName(client protocol.ClientID) string {
	return clientPrefix + hex.EncodeToString(client[:])
}

// AuthenticateInbound performs a server-side mTLS handshake and resolves the
// verified client certificate to a configured PeerIdentity.
func (auth *Authenticator) AuthenticateInbound(ctx context.Context, conn net.Conn) (net.Conn, transport.PeerIdentity, error) {
	if auth.serverTLS == nil {
		return nil, transport.PeerIdentity{}, transport.ErrAuthentication
	}
	tlsConn := tls.Server(conn, auth.serverTLS)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, transport.PeerIdentity{}, errors.Join(transport.ErrAuthentication, err)
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, transport.PeerIdentity{}, transport.ErrAuthentication
	}
	identity, err := auth.identify(state.PeerCertificates[0])
	if err != nil {
		return nil, transport.PeerIdentity{}, errors.Join(transport.ErrAuthentication, err)
	}
	if identity.Role == transport.PeerRoleReplica {
		if !auth.hasMembership {
			return nil, transport.PeerIdentity{}, transport.ErrAuthentication
		}
		index, ok := findMember(auth.membership, identity.Member)
		if !ok {
			return nil, transport.PeerIdentity{}, transport.ErrAuthentication
		}
		if auth.local.Role == transport.PeerRoleReplica && identity.Member == auth.local.Member {
			return nil, transport.PeerIdentity{}, transport.ErrAuthentication
		}
		identity.Replica = index
	}
	return tlsConn, identity, nil
}

// AuthenticateOutbound performs a client-side mTLS handshake and verifies that
// the peer certificate matches expected.
func (auth *Authenticator) AuthenticateOutbound(ctx context.Context, conn net.Conn, expected transport.PeerIdentity) (net.Conn, error) {
	if auth.clientTLS == nil {
		return nil, transport.ErrAuthentication
	}
	tlsConn := tls.Client(conn, auth.clientTLS)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return nil, errors.Join(transport.ErrAuthentication, err)
	}
	state := tlsConn.ConnectionState()
	if len(state.PeerCertificates) == 0 {
		return nil, transport.ErrAuthentication
	}
	actual, err := auth.identify(state.PeerCertificates[0])
	if err != nil {
		return nil, errors.Join(transport.ErrAuthentication, err)
	}
	if actual.Role != expected.Role {
		return nil, transport.ErrAuthentication
	}
	if expected.Role == transport.PeerRoleReplica {
		if !expected.Member.IsZero() && actual.Member != expected.Member {
			return nil, transport.ErrAuthentication
		}
		if auth.hasMembership {
			index, ok := findMember(auth.membership, actual.Member)
			if !ok || index != expected.Replica {
				return nil, transport.ErrAuthentication
			}
		}
	}
	return tlsConn, nil
}

func (auth *Authenticator) defaultIdentifyCert(cert *x509.Certificate) (transport.PeerIdentity, error) {
	for _, name := range cert.DNSNames {
		if id, ok := auth.identifyName(name); ok {
			return id, nil
		}
	}
	if id, ok := auth.identifyName(cert.Subject.CommonName); ok {
		return id, nil
	}
	return transport.PeerIdentity{}, transport.ErrAuthentication
}

func (auth *Authenticator) identifyName(name string) (transport.PeerIdentity, bool) {
	if hexID, ok := strings.CutPrefix(name, memberPrefix); ok {
		var member protocol.MemberID
		if !decodeHexID(member[:], hexID) || member.IsZero() {
			return transport.PeerIdentity{}, false
		}
		id := transport.PeerIdentity{Role: transport.PeerRoleReplica, Member: member}
		if auth.hasMembership {
			if index, found := findMember(auth.membership, member); found {
				id.Replica = index
			}
		}
		return id, true
	}
	if hexID, ok := strings.CutPrefix(name, clientPrefix); ok {
		var client protocol.ClientID
		if !decodeHexID(client[:], hexID) || client.IsZero() {
			return transport.PeerIdentity{}, false
		}
		return transport.PeerIdentity{Role: transport.PeerRoleClient, Client: client}, true
	}
	return transport.PeerIdentity{}, false
}

func decodeHexID(dst []byte, encoded string) bool {
	if len(encoded) != hex.EncodedLen(len(dst)) {
		return false
	}
	n, err := hex.Decode(dst, []byte(encoded))
	return err == nil && n == len(dst)
}

func findMember(membership replication.Membership, member protocol.MemberID) (protocol.ReplicaIndex, bool) {
	total := int(membership.ActiveCount) + int(membership.StandbyCount)
	for index := range total {
		if membership.Members[index] == member {
			return protocol.ReplicaIndex(index), true
		}
	}
	return 0, false
}
