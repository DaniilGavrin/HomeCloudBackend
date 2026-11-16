package security

// Automatic first-use trust for explicitly configured private LAN/Tailscale networks.
// NOTE: TOFU does NOT authenticate the remote peer on the very first connection.
// Do not enable the listener on the public Internet or via unauthenticated WS.

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

const firstUseALPN = "homecloud-enroll/1"
const enrollmentTimeout = 6 * time.Second
const maxEnrollMessage = 2048

// PrivateNetworks accepts explicit, limited CIDRs ONLY inside RFC1918,
// loopback, CGNAT/Tailscale 100.64/10, ULA or link-local IPv6 ranges.
// Passing 0.0.0.0/0 or public IP ranges fails closed.
type PrivateNetworks struct{ prefixes []netip.Prefix }

func NewPrivateNetworks(cidrs ...string) (PrivateNetworks, error) {
	permitted := []netip.Prefix{}
	for _, s := range []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "127.0.0.0/8", "fc00::/7", "fe80::/10", "::1/128"} {
		p, _ := netip.ParsePrefix(s)
		permitted = append(permitted, p)
	}
	if len(cidrs) == 0 {
		return PrivateNetworks{}, errors.New("explicit permitted private CIDRs are required")
	}
	var n PrivateNetworks
	for _, s := range cidrs {
		p, err := netip.ParsePrefix(s)
		if err != nil {
			return PrivateNetworks{}, err
		}
		p = p.Masked()
		ok := false
		for _, base := range permitted {
			if p.Addr().Is4() == base.Addr().Is4() && p.Bits() >= base.Bits() && base.Contains(p.Addr()) {
				ok = true
				break
			}
		}
		if !ok {
			return PrivateNetworks{}, fmt.Errorf("public or overly broad CIDR forbidden: %s", p)
		}
		n.prefixes = append(n.prefixes, p)
	}
	return n, nil
}
func (n PrivateNetworks) allowsIP(ip net.IP) bool {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return false
	}
	addr = addr.Unmap()
	for _, p := range n.prefixes {
		if p.Contains(addr) {
			return true
		}
	}
	return false
}
func (n PrivateNetworks) checkEndpoint(addr string) (string, error) {
	normal, err := normalizedAddress(addr)
	if err != nil {
		return "", err
	}
	host, _, _ := net.SplitHostPort(normal)
	ip := net.ParseIP(host)
	// KAN-8 intentionally supports literal addresses only; DNS policy and
	// changing multi-route endpoints belong to KAN-10.
	if ip == nil || !n.allowsIP(ip) {
		return "", errors.New("enrollment endpoint is outside allowed private networks")
	}
	return normal, nil
}
func endpointIP(addr string) (net.IP, error) {
	h, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ip := net.ParseIP(h)
	if ip == nil {
		return nil, errors.New("IP endpoint required")
	}
	return ip, nil
}

// FirstUseAddress is the dedicated one-time enrollment port: Raft port+2.
// The Raft and privileged membership sockets NEVER accept unpinned peers.
func FirstUseAddress(raftEndpoint string) (string, error) {
	host, port, err := net.SplitHostPort(raftEndpoint)
	if err != nil {
		return "", err
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65533 {
		return "", errors.New("raft port cannot derive private enrollment port")
	}
	return net.JoinHostPort(host, strconv.Itoa(n+2)), nil
}
func certificateIdentity(cert tls.Certificate, localID string) error {
	if len(cert.Certificate) != 1 || cert.PrivateKey == nil {
		return errors.New("identity certificate required")
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		return err
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok || NodeID(pub) != localID || leaf.Subject.CommonName != localID {
		return ErrKeyMismatch
	}
	return nil
}
func checkFirstUsePeer(cs tls.ConnectionState, asServer bool) (string, ed25519.PublicKey, error) {
	if cs.Version != tls.VersionTLS13 || cs.NegotiatedProtocol != firstUseALPN || len(cs.PeerCertificates) != 1 {
		return "", nil, errors.New("wrong enrollment TLS protocol or certificate chain")
	}
	leaf := cs.PeerCertificates[0]
	now := time.Now()
	if leaf.IsCA || now.Before(leaf.NotBefore) || now.After(leaf.NotAfter) || leaf.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		return "", nil, ErrKeyMismatch
	}
	wanted := x509.ExtKeyUsageServerAuth
	if asServer {
		wanted = x509.ExtKeyUsageClientAuth
	}
	found := false
	for _, v := range leaf.ExtKeyUsage {
		if v == wanted {
			found = true
		}
	}
	if !found {
		return "", nil, ErrKeyMismatch
	}
	if err := leaf.CheckSignature(leaf.SignatureAlgorithm, leaf.RawTBSCertificate, leaf.Signature); err != nil {
		return "", nil, err
	}
	pub, ok := leaf.PublicKey.(ed25519.PublicKey)
	if !ok {
		return "", nil, ErrKeyMismatch
	}
	id := NodeID(pub)
	if leaf.Subject.CommonName != id {
		return "", nil, ErrKeyMismatch
	}
	return id, pub, nil
}
func inspectFirstUse(cs tls.ConnectionState, trust *TrustStore, server bool) (string, ed25519.PublicKey, error) {
	id, pub, err := checkFirstUsePeer(cs, server)
	if err != nil {
		return "", nil, err
	}
	if id == trust.localID || trust.IsRevoked(id) {
		return "", nil, ErrUntrusted
	}
	if _, ok := trust.Peer(id); ok {
		if err := trust.Verify(id, pub); err != nil {
			return "", nil, err
		}
	}
	return id, pub, nil
}

type enrollMessage struct {
	Type      string `json:"type"`
	Raft      string `json:"raft"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id,omitempty"`
}

func readEnroll(conn net.Conn) (enrollMessage, error) {
	var v enrollMessage
	// Each message is one newline-delimited JSON object with a hard size cap.
	var b [1]byte
	buf := make([]byte, 0, 256)
	for len(buf) < maxEnrollMessage {
		_, err := io.ReadFull(conn, b[:])
		if err != nil {
			return v, err
		}
		if b[0] == '\n' {
			break
		}
		buf = append(buf, b[0])
	}
	if len(buf) == maxEnrollMessage {
		return v, errors.New("enrollment message too large")
	}
	if err := json.Unmarshal(buf, &v); err != nil {
		return v, err
	}
	if len(v.Type) > 32 || len(v.Raft) > 512 || len(v.NodeID) > 64 || len(v.ClusterID) > 128 {
		return v, errors.New("enrollment fields too large")
	}
	return v, nil
}
func writeEnroll(conn net.Conn, v enrollMessage) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	if len(b)+1 > maxEnrollMessage {
		return errors.New("enrollment message too large")
	}
	b = append(b, '\n')
	_, err = conn.Write(b)
	return err
}

// FirstUseServer is an explicit opt-in private-network listener. It does not
// start from serve automatically and never processes user/Storage/IoT traffic.
type FirstUseServer struct {
	listener     net.Listener
	cert         tls.Certificate
	trust        *TrustStore
	networks     PrivateNetworks
	raft         string
	mu           sync.Mutex
	closed       bool
	certMu       sync.Mutex
	certProvider func() (tls.Certificate, error)
	sem          chan struct{}
	peers        sync.WaitGroup
	joinHandler  func(context.Context, Peer, string) error
}

func OpenFirstUseServer(raftAddress string, cert tls.Certificate, trust *TrustStore, networks PrivateNetworks) (*FirstUseServer, error) {
	if trust == nil || len(networks.prefixes) == 0 {
		return nil, errors.New("trust and private CIDR policy required")
	}
	raft, err := networks.checkEndpoint(raftAddress)
	if err != nil {
		return nil, err
	}
	if err = certificateIdentity(cert, trust.localID); err != nil {
		return nil, err
	}
	bind, err := FirstUseAddress(raft)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", bind)
	if err != nil {
		return nil, err
	}
	return &FirstUseServer{listener: ln, cert: cert, trust: trust, networks: networks, raft: raft, sem: make(chan struct{}, 16)}, nil
}

// SetCertificateProvider refreshes the short-lived TLS certificate from the
// SAME permanent identity. Set once before calling Serve; the private key
// never leaves the node. A failed rotation rejects the handshake.
func (s *FirstUseServer) SetCertificateProvider(fn func() (tls.Certificate, error)) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	s.certProvider = fn
}
func (s *FirstUseServer) certificate() (*tls.Certificate, error) {
	s.certMu.Lock()
	defer s.certMu.Unlock()
	leaf, err := x509.ParseCertificate(s.cert.Certificate[0])
	if err != nil {
		return nil, err
	}
	if time.Until(leaf.NotAfter) <= 30*time.Minute {
		if s.certProvider == nil {
			return nil, errors.New("enrollment certificate expiring; no rotation provider")
		}
		next, err := s.certProvider()
		if err != nil {
			return nil, err
		}
		if err = certificateIdentity(next, s.trust.localID); err != nil {
			return nil, err
		}
		parsed, err := x509.ParseCertificate(next.Certificate[0])
		if err != nil {
			return nil, err
		}
		if time.Until(parsed.NotAfter) <= 30*time.Minute {
			return nil, errors.New("rotated certificate expires too soon")
		}
		s.cert = next
	}
	copyCert := s.cert
	return &copyCert, nil
}

// SetJoinHandler lets the local runtime prepare an unbootstrapped candidate
// when a cluster leader explicitly offers a cluster ID. No user-visible code
// or receiving-side UI approval is required; PRIVATE NETWORK TOFU applies.
// Set once before Serve. The handler must reject conflicting persisted clusters.
func (s *FirstUseServer) SetJoinHandler(fn func(context.Context, Peer, string) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.joinHandler = fn
}
func (s *FirstUseServer) Addr() net.Addr { return s.listener.Addr() }
func (s *FirstUseServer) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return s.listener.Close()
}
func (s *FirstUseServer) Serve(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.Close()
		case <-done:
		}
	}()
	defer close(done)
	for {
		c, err := s.listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		ip, err := endpointIP(c.RemoteAddr().String())
		if err != nil || !s.networks.allowsIP(ip) {
			_ = c.Close()
			continue
		}
		select {
		case s.sem <- struct{}{}:
			s.peers.Add(1)
			go func() { defer s.peers.Done(); defer func() { <-s.sem }(); s.serveOne(c) }()
		default:
			_ = c.Close()
		}
	}
}
func (s *FirstUseServer) serveOne(raw net.Conn) {
	defer raw.Close()
	_ = raw.SetDeadline(time.Now().Add(enrollmentTimeout))
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) { return s.certificate() }, ClientAuth: tls.RequireAnyClientCert, SessionTicketsDisabled: true, NextProtos: []string{firstUseALPN}, VerifyConnection: func(cs tls.ConnectionState) error { _, _, err := inspectFirstUse(cs, s.trust, true); return err }}
	conn := tls.Server(raw, cfg)
	if err := conn.Handshake(); err != nil {
		return
	}
	id, pub, err := inspectFirstUse(conn.ConnectionState(), s.trust, true)
	if err != nil {
		return
	}
	req, err := readEnroll(conn)
	if err != nil || req.Type != "hello" || req.NodeID != id {
		return
	}
	raft, err := s.networks.checkEndpoint(req.Raft)
	if err != nil {
		return
	}
	remoteIP, err := endpointIP(conn.RemoteAddr().String())
	if err != nil {
		return
	}
	claimedIP, err := endpointIP(raft)
	if err != nil || !claimedIP.Equal(remoteIP) {
		return
	}
	if err = writeEnroll(conn, enrollMessage{Type: "hello", NodeID: s.trust.localID, Raft: s.raft}); err != nil {
		return
	}
	ack, err := readEnroll(conn)
	if err != nil || ack.Type != "ack" || ack.NodeID != id {
		return
	}
	peer := Peer{NodeID: id, PublicKey: base64.StdEncoding.EncodeToString(pub), Addresses: []string{raft}}
	if req.ClusterID != "" {
		s.mu.Lock()
		handler := s.joinHandler
		s.mu.Unlock()
		if handler == nil {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = handler(ctx, peer, req.ClusterID)
		cancel()
		if err != nil {
			return
		}
	}
	if err = s.trust.Approve(peer); err != nil {
		return
	}
	_ = writeEnroll(conn, enrollMessage{Type: "committed", NodeID: s.trust.localID})
}

// TrustFirstUse is called only after an explicit authenticated LOCAL UI action.
// It pins both peers with their real TLS Ed25519 identity, then later normal
// Raft/membership sockets use strict mTLS pinning without first-use fallback.
func TrustFirstUse(ctx context.Context, localRaft, targetRaft string, cert tls.Certificate, trust *TrustStore, networks PrivateNetworks) (Peer, error) {
	return TrustFirstUseWithCluster(ctx, localRaft, targetRaft, "", cert, trust, networks)
}

// TrustFirstUseWithCluster additionally makes an explicit cluster invitation
// over the first-use TLS channel. The receiving Go prepares its Raft candidate
// automatically; a trusted Raft leader then commits the actual admission.
func TrustFirstUseWithCluster(ctx context.Context, localRaft, targetRaft, clusterID string, cert tls.Certificate, trust *TrustStore, networks PrivateNetworks) (Peer, error) {
	if trust == nil || len(networks.prefixes) == 0 {
		return Peer{}, errors.New("trust and private CIDR policy required")
	}
	local, err := networks.checkEndpoint(localRaft)
	if err != nil {
		return Peer{}, err
	}
	remote, err := networks.checkEndpoint(targetRaft)
	if err != nil {
		return Peer{}, err
	}
	if err = certificateIdentity(cert, trust.localID); err != nil {
		return Peer{}, err
	}
	boot, err := FirstUseAddress(remote)
	if err != nil {
		return Peer{}, err
	}
	expected, _ := trust.PeerForAddress(remote)
	cfg := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert}, InsecureSkipVerify: true, SessionTicketsDisabled: true, NextProtos: []string{firstUseALPN}, VerifyConnection: func(cs tls.ConnectionState) error {
		id, _, err := inspectFirstUse(cs, trust, false)
		if err != nil {
			return err
		}
		if expected.NodeID != "" && id != expected.NodeID {
			return ErrKeyMismatch
		}
		return nil
	}}
	dialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: enrollmentTimeout}, Config: cfg}
	dctx, cancel := context.WithTimeout(ctx, enrollmentTimeout)
	defer cancel()
	c, err := dialer.DialContext(dctx, "tcp", boot)
	if err != nil {
		return Peer{}, err
	}
	defer c.Close()
	_ = c.SetDeadline(time.Now().Add(enrollmentTimeout))
	tc, ok := c.(*tls.Conn)
	if !ok {
		return Peer{}, errors.New("TLS connection expected")
	}
	id, pub, err := inspectFirstUse(tc.ConnectionState(), trust, false)
	if err != nil {
		return Peer{}, err
	}
	localIP, err := endpointIP(c.LocalAddr().String())
	if err != nil {
		return Peer{}, err
	}
	advertisedIP, err := endpointIP(local)
	if err != nil || !localIP.Equal(advertisedIP) {
		return Peer{}, errors.New("local advertised Raft endpoint does not match outbound interface IP")
	}
	if err := writeEnroll(c, enrollMessage{Type: "hello", NodeID: trust.localID, Raft: local, ClusterID: clusterID}); err != nil {
		return Peer{}, err
	}
	response, err := readEnroll(c)
	if err != nil {
		return Peer{}, err
	}
	if response.Type != "hello" || response.NodeID != id || response.Raft != remote {
		return Peer{}, ErrKeyMismatch
	}
	if err = writeEnroll(c, enrollMessage{Type: "ack", NodeID: trust.localID}); err != nil {
		return Peer{}, err
	}
	confirmed, err := readEnroll(c)
	if err != nil {
		return Peer{}, err
	}
	if confirmed.Type != "committed" || confirmed.NodeID != id {
		return Peer{}, errors.New("enrollment not committed by peer")
	}
	peer := Peer{NodeID: id, PublicKey: base64.StdEncoding.EncodeToString(pub), Addresses: []string{remote}}
	if err = trust.Approve(peer); err != nil {
		return Peer{}, err
	}
	return peer, nil
}

// ValidatePrivateRaftEndpoint rejects wildcard, publicly routable and
// unapproved local addresses. The CIDR policy must be explicit.
func ValidatePrivateRaftEndpoint(addr string, networks PrivateNetworks) error {
	_, err := networks.checkEndpoint(addr)
	return err
}
