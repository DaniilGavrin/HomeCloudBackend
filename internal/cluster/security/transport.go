package security

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"
)

const (
	raftALPN       = "homecloud-raft/1"
	membershipALPN = "homecloud-membership/1"
)

// Listener is a pinned-public-key, mutual-TLS stream used by Raft.
// There is no fallback to cleartext TCP. Only previously approved peers
// can connect. A separate trusted owner flow must approve pairing first.
type Listener struct {
	protocol     string
	base         net.Listener
	trust        *TrustStore
	cert         tls.Certificate
	certMu       sync.Mutex
	certProvider func() (tls.Certificate, error)
	mu           sync.Mutex
	active       map[*securedConn]bool
	cancelled    func()
	closed       bool
}

type securedConn struct {
	net.Conn
	owner  *Listener
	peerID string
	once   sync.Once
}

func (c *securedConn) Close() error {
	var err error
	c.once.Do(func() { err = c.Conn.Close(); c.owner.mu.Lock(); delete(c.owner.active, c); c.owner.mu.Unlock() })
	return err
}

func NewRaftListener(bind string, cert tls.Certificate, trust *TrustStore) (*Listener, error) {
	return newPinnedListener(bind, cert, trust, raftALPN)
}

// NewMembershipListener isolates privileged cluster-management RPCs from Raft
// streams with their own TLS ALPN and listening socket. No plain TCP fallback.
func NewMembershipListener(bind string, cert tls.Certificate, trust *TrustStore) (*Listener, error) {
	return newPinnedListener(bind, cert, trust, membershipALPN)
}

func newPinnedListener(bind string, cert tls.Certificate, trust *TrustStore, protocol string) (*Listener, error) {
	if trust == nil {
		return nil, errors.New("secure raft requires a trust store")
	}
	host, _, err := net.SplitHostPort(bind)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() || ip.IsMulticast() {
		return nil, errors.New("secure Raft bind must specify an existing non-wildcard IP")
	}
	if len(cert.Certificate) == 0 || cert.PrivateKey == nil {
		return nil, errors.New("secure Raft requires a local identity TLS certificate")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok || NodeID(pub) != trust.localID {
		return nil, ErrKeyMismatch
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	s := &Listener{base: ln, trust: trust, cert: cert, protocol: protocol, active: make(map[*securedConn]bool)}
	s.cancelled = trust.OnRevoke(s.disconnectPeer)
	return s, nil
}

// SetCertificateProvider allows a long-running service to rotate its short-lived
// self-signed certificate using the SAME permanent Ed25519 node identity.
// Called once before the listener is used; not a network or WS operation.
func (s *Listener) SetCertificateProvider(fn func() (tls.Certificate, error)) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	s.certProvider = fn
}
func (s *Listener) certificate() (*tls.Certificate, error) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	leaf := s.cert.Leaf
	if leaf == nil {
		var err error
		leaf, err = x509.ParseCertificate(s.cert.Certificate[0])
		if err != nil {
			return nil, err
		}
	}
	if time.Until(leaf.NotAfter) <= 30*time.Minute {
		if s.certProvider == nil {
			return nil, errors.New("cluster certificate expiring; no rotation provider")
		}
		cert, err := s.certProvider()
		if err != nil {
			return nil, err
		}
		next, err := x509.ParseCertificate(cert.Certificate[0])
		if err != nil {
			return nil, err
		}
		pub, ok := next.PublicKey.(ed25519.PublicKey)
		if !ok || NodeID(pub) != s.trust.localID {
			return nil, ErrKeyMismatch
		}
		if time.Until(next.NotAfter) <= 30*time.Minute {
			return nil, errors.New("new certificate expires too soon")
		}
		s.cert = cert
		s.cert.Leaf = next
	}
	copyCert := s.cert
	return &copyCert, nil
}
func (s *Listener) disconnectPeer(id string) {
	s.mu.Lock()
	toClose := make([]*securedConn, 0)
	for conn := range s.active {
		if conn.peerID == id {
			toClose = append(toClose, conn)
		}
	}
	s.mu.Unlock()
	for _, conn := range toClose {
		_ = conn.Close()
	}
}
func (s *Listener) serverTLS() *tls.Config {
	return &tls.Config{GetCertificate: func(_ *tls.ClientHelloInfo) (*tls.Certificate, error) { return s.certificate() }, MinVersion: tls.VersionTLS13, ClientAuth: tls.RequireAnyClientCert,
		SessionTicketsDisabled: true, NextProtos: []string{s.protocol}, VerifyConnection: func(cs tls.ConnectionState) error {
			_, err := s.verifyTLS(cs, "")
			return err
		}}
}
func (s *Listener) clientTLS(expected string) *tls.Config {
	// InsecureSkipVerify is safe here ONLY because VerifyConnection always
	// enforces self-signed cert integrity, validity and exact Ed25519 pin.
	return &tls.Config{GetClientCertificate: func(_ *tls.CertificateRequestInfo) (*tls.Certificate, error) { return s.certificate() }, MinVersion: tls.VersionTLS13,
		InsecureSkipVerify: true, SessionTicketsDisabled: true, NextProtos: []string{s.protocol}, VerifyConnection: func(cs tls.ConnectionState) error {
			_, err := s.verifyTLS(cs, expected)
			return err
		}}
}
func (s *Listener) verifyTLS(cs tls.ConnectionState, expected string) (string, error) {
	if cs.Version < tls.VersionTLS13 || cs.NegotiatedProtocol != s.protocol || len(cs.PeerCertificates) != 1 {
		return "", errors.New("invalid Raft TLS protocol or certificate chain")
	}
	cert := cs.PeerCertificates[0]
	now := time.Now()
	if now.Before(cert.NotBefore) || now.After(cert.NotAfter) || cert.IsCA {
		return "", errors.New("invalid Raft peer certificate validity")
	}
	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return "", errors.New("peer certificate cannot sign")
	}
	requiredUsage := x509.ExtKeyUsageClientAuth
	if expected != "" {
		requiredUsage = x509.ExtKeyUsageServerAuth
	}
	allowed := false
	for _, usage := range cert.ExtKeyUsage {
		if usage == requiredUsage {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", errors.New("peer certificate lacks required TLS usage")
	}

	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
		return "", fmt.Errorf("peer certificate signature: %w", err)
	}
	pub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", ErrKeyMismatch
	}
	id := NodeID(pub)
	if cert.Subject.CommonName != id || (expected != "" && expected != id) {
		return "", ErrKeyMismatch
	}
	if err := s.trust.Verify(id, pub); err != nil {
		return "", err
	}
	return id, nil
}

// Accept verifies the TLS handshake before returning the Raft stream.
// Failed/untrusted handshakes cannot enter the Raft decoder.
func (s *Listener) Accept() (net.Conn, error) {
	for {
		raw, err := s.base.Accept()
		if err != nil {
			return nil, err
		}
		_ = raw.SetDeadline(time.Now().Add(5 * time.Second))
		conn := tls.Server(raw, s.serverTLS())
		if err := conn.Handshake(); err != nil {
			_ = raw.Close()
			continue
		}
		_ = conn.SetDeadline(time.Time{})
		id, err := s.verifyTLS(conn.ConnectionState(), "")
		if err != nil {
			_ = conn.Close()
			continue
		}
		wrapped := &securedConn{Conn: conn, owner: s, peerID: id}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			return nil, net.ErrClosed
		}
		// Lock before rechecking trust to close the revoke/registration race.
		leafPub := conn.ConnectionState().PeerCertificates[0].PublicKey.(ed25519.PublicKey)
		if e := s.trust.Verify(id, leafPub); e != nil {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.active[wrapped] = true
		s.mu.Unlock()
		return wrapped, nil
	}
}

// Dial only accepts an address already pinned to the expected Node ID.
func (s *Listener) Dial(address string, timeout time.Duration) (net.Conn, error) {
	peer, err := s.trust.PeerForAddress(address)
	if err != nil {
		return nil, err
	}
	return s.dialPinned(address, peer.NodeID, timeout)
}

// DialMembership derives an isolated control port (Raft port+1) from a
// PINNED Raft endpoint. It cannot dial arbitrary hosts or switch to cleartext.
func (s *Listener) DialMembership(raftAddress string, timeout time.Duration) (net.Conn, error) {
	if s.protocol != membershipALPN {
		return nil, errors.New("not a membership TLS listener")
	}
	peer, err := s.trust.PeerForAddress(raftAddress)
	if err != nil {
		return nil, err
	}
	host, portText, err := net.SplitHostPort(raftAddress)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port <= 0 || port >= 65535 {
		return nil, errors.New("invalid membership port offset")
	}
	return s.dialPinned(net.JoinHostPort(host, strconv.Itoa(port+1)), peer.NodeID, timeout)
}

func (s *Listener) dialPinned(address, expectedNodeID string, timeout time.Duration) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: timeout}
	c, err := tls.DialWithDialer(dialer, "tcp", address, s.clientTLS(expectedNodeID))
	if err != nil {
		return nil, err
	}
	wrapped := &securedConn{Conn: c, owner: s, peerID: expectedNodeID}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		_ = c.Close()
		return nil, net.ErrClosed
	}
	leafPub := c.ConnectionState().PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if e := s.trust.Verify(expectedNodeID, leafPub); e != nil {
		s.mu.Unlock()
		_ = c.Close()
		return nil, e
	}
	s.active[wrapped] = true
	s.mu.Unlock()
	return wrapped, nil
}
func (s *Listener) Addr() net.Addr { return s.base.Addr() }
func (s *Listener) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	conns := make([]*securedConn, 0, len(s.active))
	for c := range s.active {
		conns = append(conns, c)
	}
	s.mu.Unlock()
	s.cancelled()
	err := s.base.Close()
	for _, c := range conns {
		_ = c.Close()
	}
	return err
}

// AuthenticatedPeerID obtains the identity actually verified during the TLS
// handshake, never from any untrusted JSON field in the RPC payload.
func AuthenticatedPeerID(conn net.Conn) (string, error) {
	peer, ok := conn.(*securedConn)
	if !ok || peer.peerID == "" {
		return "", ErrUntrusted
	}
	// Ensure a peer revoked between handshake and dispatch cannot execute RPC.
	tlsConn, ok := peer.Conn.(*tls.Conn)
	if !ok || len(tlsConn.ConnectionState().PeerCertificates) != 1 {
		return "", ErrUntrusted
	}
	pub, ok := tlsConn.ConnectionState().PeerCertificates[0].PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", ErrKeyMismatch
	}
	if err := peer.owner.trust.Verify(peer.peerID, pub); err != nil {
		return "", err
	}
	return peer.peerID, nil
}
