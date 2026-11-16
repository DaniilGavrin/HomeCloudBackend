package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func testKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}
func testPeer(t *testing.T, addr string) (Peer, ed25519.PrivateKey) {
	t.Helper()
	pub, priv := testKey(t)
	return Peer{NodeID: NodeID(pub), PublicKey: base64.StdEncoding.EncodeToString(pub), Addresses: []string{addr}}, priv
}
func testTrust(t *testing.T, local string) *TrustStore {
	t.Helper()
	s, err := OpenTrustStore(filepath.Join(t.TempDir(), "trust"), local)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func TestTrustApprovalPersistenceAndRevocation(t *testing.T) {
	local, _ := testKey(t)
	dir := filepath.Join(t.TempDir(), "trust")
	s, err := OpenTrustStore(dir, NodeID(local))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := testPeer(t, "127.0.0.1:19001")
	if _, err := s.PeerForAddress(p.Addresses[0]); !errors.Is(err, ErrUntrusted) {
		t.Fatal(err)
	}
	if err := s.Approve(p); err != nil {
		t.Fatal(err)
	}
	if err := s.Approve(p); err != nil {
		t.Fatal(err)
	}
	s2, err := OpenTrustStore(dir, NodeID(local))
	if err != nil {
		t.Fatal(err)
	}
	if out, err := s2.PeerForAddress(p.Addresses[0]); err != nil || out.NodeID != p.NodeID {
		t.Fatalf("persist: %+v %v", out, err)
	}
	if err := s2.Revoke(p.NodeID); err != nil {
		t.Fatal(err)
	}
	s3, err := OpenTrustStore(dir, NodeID(local))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s3.Peer(p.NodeID); ok {
		t.Fatal("revoked peer resurrected")
	}
}
func TestTrustRejectsImpersonationAndEndpointCollision(t *testing.T) {
	local, _ := testKey(t)
	s := testTrust(t, NodeID(local))
	p, _ := testPeer(t, "127.0.0.1:19002")
	if err := s.Approve(p); err != nil {
		t.Fatal(err)
	}
	p2, _ := testPeer(t, "127.0.0.1:19002")
	if err := s.Approve(p2); err == nil {
		t.Fatal("duplicate endpoint approved")
	}
	p2.NodeID = p.NodeID
	if err := s.Approve(p2); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("impersonation: %v", err)
	}
	if err := s.Verify(p.NodeID, []byte("invalid")); !errors.Is(err, ErrKeyMismatch) {
		t.Fatalf("key mismatch: %v", err)
	}
}
func TestPairingNeedsProofAndOwnerConfirmation(t *testing.T) {
	local, _ := testKey(t)
	s := testTrust(t, NodeID(local))
	mgr := NewPairingManager(s)
	p, priv := testPeer(t, "127.0.0.1:19003")
	challenge, err := mgr.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.Approve(challenge.ID, true); err == nil {
		t.Fatal("approval without proof")
	}
	challenge, err = mgr.Begin()
	if err != nil {
		t.Fatal(err)
	}
	nonce, err := base64.StdEncoding.DecodeString(challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	invalid := ed25519.Sign(priv, []byte("wrong challenge"))
	if err := mgr.VerifyProof(challenge.ID, p, invalid); err == nil {
		t.Fatal("invalid proof accepted")
	}
	proof := ed25519.Sign(priv, append([]byte(proofDomain), nonce...))
	if err := mgr.VerifyProof(challenge.ID, p, proof); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Peer(p.NodeID); ok {
		t.Fatal("verified but unapproved peer granted")
	}
	if err := mgr.Approve(challenge.ID, false); err == nil {
		t.Fatal("declined pairing accepted")
	}
	if _, ok := s.Peer(p.NodeID); ok {
		t.Fatal("declined peer granted")
	}
	c, err := mgr.Begin()
	if err != nil {
		t.Fatal(err)
	}
	n, _ := base64.StdEncoding.DecodeString(c.Nonce)
	if err := mgr.VerifyProof(c.ID, p, ed25519.Sign(priv, append([]byte(proofDomain), n...))); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Approve(c.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Peer(p.NodeID); !ok {
		t.Fatal("approved peer not trusted")
	}
	if err := mgr.Approve(c.ID, true); err == nil {
		t.Fatal("challenge replay accepted")
	}
}
func mkCert(t *testing.T, p Peer, key ed25519.PrivateKey) tls.Certificate {
	t.Helper()
	now := time.Now()
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(5), Subject: pkix.Name{CommonName: p.NodeID}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, key.Public(), key)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}
}
func TestTLSMutualPinningAndRevokeActiveStream(t *testing.T) {
	pubA, privA := testKey(t)
	pubB, privB := testKey(t)
	a := Peer{NodeID: NodeID(pubA), PublicKey: base64.StdEncoding.EncodeToString(pubA)}
	b := Peer{NodeID: NodeID(pubB), PublicKey: base64.StdEncoding.EncodeToString(pubB)}
	sa := testTrust(t, a.NodeID)
	sb := testTrust(t, b.NodeID)
	// Reserve the listening address, then allow the client to dial that exact endpoint.
	addrListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := addrListener.Addr().String()
	_ = addrListener.Close()
	a.Addresses = []string{addr}
	b.Addresses = []string{"127.0.0.1:19005"}
	if err := sa.Approve(b); err != nil {
		t.Fatal(err)
	}
	if err := sb.Approve(a); err != nil {
		t.Fatal(err)
	}
	server, err := NewRaftListener(addr, mkCert(t, a, privA), sa)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	// client needs only a local listener to own the dialing stream (no Accept needed).
	client, err := NewRaftListener("127.0.0.1:0", mkCert(t, b, privB), sb)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	got := make(chan net.Conn, 1)
	go func() {
		accepted, e := server.Accept()
		if e == nil {
			got <- accepted
		}
	}()
	outgoing, err := client.Dial(addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer outgoing.Close()
	var incoming net.Conn
	select {
	case incoming = <-got:
	case <-time.After(3 * time.Second):
		t.Fatal("mutual TLS peer never accepted")
	}
	defer incoming.Close()
	if _, err := outgoing.Write([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 5)
	if _, err := incoming.Read(data); err != nil {
		t.Fatal(err)
	}
	if string(data) != "hello" {
		t.Fatalf("bad payload %q", data)
	}
	if err := sa.Revoke(b.NodeID); err != nil {
		t.Fatal(err)
	}
	_ = incoming.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := incoming.Read(data); err == nil {
		t.Fatal("revoked connection remained readable")
	}
}
func TestRemotePeerMustBeApproved(t *testing.T) {
	pubA, privA := testKey(t)
	pubB, privB := testKey(t)
	a := Peer{NodeID: NodeID(pubA), PublicKey: base64.StdEncoding.EncodeToString(pubA)}
	b := Peer{NodeID: NodeID(pubB), PublicKey: base64.StdEncoding.EncodeToString(pubB)}
	sa := testTrust(t, a.NodeID)
	sb := testTrust(t, b.NodeID)
	addrListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := addrListener.Addr().String()
	_ = addrListener.Close()
	a.Addresses = []string{addr}
	b.Addresses = []string{"127.0.0.1:19006"}
	if err := sb.Approve(a); err != nil {
		t.Fatal(err)
	}
	server, err := NewRaftListener(addr, mkCert(t, a, privA), sa)
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	client, err := NewRaftListener("127.0.0.1:0", mkCert(t, b, privB), sb)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c, e := server.Accept()
		if e == nil {
			_ = c.Close()
		}
	}()
	c, err := client.Dial(addr, 2*time.Second)
	if err == nil {
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, err = c.Read(make([]byte, 1))
	}
	if err == nil {
		t.Fatal("unauthorized mutual TLS peer connected")
	}
}
func TestExpiredPairingCannotApprove(t *testing.T) {
	local, _ := testKey(t)
	s := testTrust(t, NodeID(local))
	mgr := NewPairingManager(s)
	current := time.Now()
	mgr.now = func() time.Time { return current }
	p, priv := testPeer(t, "127.0.0.1:19007")
	c, err := mgr.Begin()
	if err != nil {
		t.Fatal(err)
	}
	nonce, _ := base64.StdEncoding.DecodeString(c.Nonce)
	current = current.Add(3 * time.Minute)
	if err := mgr.VerifyProof(c.ID, p, ed25519.Sign(priv, append([]byte(proofDomain), nonce...))); err == nil {
		t.Fatal("expired pairing proof accepted")
	}
	if _, ok := s.Peer(p.NodeID); ok {
		t.Fatal("expired peer unexpectedly trusted")
	}
}
func TestNoTrustMeansOutboundRejectedBeforeDial(t *testing.T) {
	pub, priv := testKey(t)
	p := Peer{NodeID: NodeID(pub), PublicKey: base64.StdEncoding.EncodeToString(pub)}
	s := testTrust(t, p.NodeID)
	layer, err := NewRaftListener("127.0.0.1:0", mkCert(t, p, priv), s)
	if err != nil {
		t.Fatal(err)
	}
	defer layer.Close()
	if _, err := layer.Dial("127.0.0.1:19101", time.Second); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("untrusted remote dial: %v", err)
	}
}
