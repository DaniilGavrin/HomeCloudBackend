package security

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/identity"
)

type testEnrollmentNode struct {
	id       identity.Identity
	store    *TrustStore
	cert     tls.Certificate
	trustDir string
	raft     string
}

func newTestEnrollmentNode(t *testing.T) *testEnrollmentNode {
	t.Helper()
	dir := t.TempDir()
	s := identity.NewStore(filepath.Join(dir, "identity"))
	id, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := s.RaftTLSCertificate()
	if err != nil {
		t.Fatal(err)
	}
	trustDir := filepath.Join(dir, "trust")
	store, err := OpenTrustStore(trustDir, id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	return &testEnrollmentNode{id: id, cert: cert, store: store, trustDir: trustDir, raft: freeRaftPort(t)}
}
func freeRaftPort(t *testing.T) string {
	t.Helper()
	for i := 0; i < 25; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		_ = ln.Close()
		if port >= 65533 {
			continue
		}
		check, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port+2))
		if err != nil {
			continue
		}
		_ = check.Close()
		return fmt.Sprintf("127.0.0.1:%d", port)
	}
	t.Fatal("no free Raft/TOFU port pair")
	return ""
}
func startFirstUse(t *testing.T, n *testEnrollmentNode, allowed PrivateNetworks) *FirstUseServer {
	t.Helper()
	server, err := OpenFirstUseServer(n.raft, n.cert, n.store, allowed)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = server.Close()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("enrollment server: %v", err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("server did not exit")
		}
	})
	return server
}
func testNetworks(t *testing.T) PrivateNetworks {
	t.Helper()
	p, err := NewPrivateNetworks("127.0.0.0/8")
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestAutomaticFirstUseMutualTLSAndDurablePins(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	startFirstUse(t, b, policy)
	peer, err := TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy)
	if err != nil {
		t.Fatal(err)
	}
	if peer.NodeID != b.id.NodeID {
		t.Fatalf("peer identity mismatch %q", peer.NodeID)
	}
	if p, ok := a.store.Peer(b.id.NodeID); !ok || p.Addresses[0] != b.raft {
		t.Fatal("initiator missing peer pin")
	}
	if p, ok := b.store.Peer(a.id.NodeID); !ok || p.Addresses[0] != a.raft {
		t.Fatal("receiver missing peer pin")
	}
	// Both normal peer stores survive restart and preserve the original pins.
	for _, n := range []*testEnrollmentNode{a, b} {
		reloaded, err := OpenTrustStore(n.trustDir, n.id.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		n.store = reloaded
	}
	raw, err := base64.StdEncoding.DecodeString(b.id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.store.Verify(b.id.NodeID, ed25519.PublicKey(raw)); err != nil {
		t.Fatalf("durable pin rejected: %v", err)
	}
	if _, err := TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy); err != nil {
		t.Fatalf("repeated first-use should be idempotent: %v", err)
	}
}

func TestFirstUseReenrollmentRevocationSurvivesRestart(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	startFirstUse(t, b, policy)
	if _, err := TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy); err != nil {
		t.Fatal(err)
	}
	if err := a.store.Revoke(b.id.NodeID); err != nil {
		t.Fatal(err)
	}
	restarted, err := OpenTrustStore(a.trustDir, a.id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	a.store = restarted
	if !a.store.IsRevoked(b.id.NodeID) {
		t.Fatal("revocation not durable")
	}
	if _, err := TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy); err == nil {
		t.Fatal("revoked peer was reaccepted")
	}
	if err := a.store.Approve(Peer{NodeID: b.id.NodeID, PublicKey: b.id.PublicKey, Addresses: []string{b.raft}}); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("revoke bypassed: %v", err)
	}
	if err := a.store.ReauthorizeRevoked(b.id.NodeID); err != nil {
		t.Fatal(err)
	}
	if _, err := TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy); err != nil {
		t.Fatalf("explicit reauthorization failed: %v", err)
	}
}
func TestOfflineRemovedNodeTombstoneBeforeFirstContact(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	if err := a.store.Revoke(b.id.NodeID); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("expected unknown peer marker: %v", err)
	}
	restart, err := OpenTrustStore(a.trustDir, a.id.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !restart.IsRevoked(b.id.NodeID) {
		t.Fatal("offline revocation lost")
	}
	if err := restart.Approve(Peer{NodeID: b.id.NodeID, PublicKey: b.id.PublicKey, Addresses: []string{b.raft}}); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("resurrected removed voter: %v", err)
	}
}
func TestFirstUseRejectPublicAndWildcard(t *testing.T) {
	for _, cidr := range []string{"0.0.0.0/0", "8.8.8.0/24", "100.0.0.0/8", "::/0", "2001:db8::/32"} {
		if _, err := NewPrivateNetworks(cidr); err == nil {
			t.Errorf("accepted unsafe CIDR %s", cidr)
		}
	}
	n := newTestEnrollmentNode(t)
	networks := testNetworks(t)
	if _, err := OpenFirstUseServer("0.0.0.0:12342", n.cert, n.store, networks); err == nil {
		t.Fatal("wildcard bind accepted")
	}
	if _, err := OpenFirstUseServer("8.8.8.8:12342", n.cert, n.store, networks); err == nil {
		t.Fatal("public bind accepted")
	}
	if _, err := TrustFirstUse(context.Background(), n.raft, "8.8.8.8:10000", n.cert, n.store, networks); err == nil {
		t.Fatal("public dial accepted")
	}
	p, err := NewPrivateNetworks("100.64.0.0/10", "192.168.0.0/16", "fd7a:115c:a1e0::/48")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = p.checkEndpoint("100.82.22.45:18833"); err != nil {
		t.Fatal(err)
	}
	if _, err = p.checkEndpoint("[fd7a:115c:a1e0::ab]:18833"); err != nil {
		t.Fatal(err)
	}
}
func TestFirstUseRejectAdvertisedSourceMismatch(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	startFirstUse(t, b, policy)
	_, port, err := net.SplitHostPort(a.raft)
	if err != nil {
		t.Fatal(err)
	}
	_, err = TrustFirstUse(context.Background(), net.JoinHostPort("127.0.0.2", port), b.raft, a.cert, a.store, policy)
	if err == nil {
		t.Fatal("source identity claim accepted")
	}
	if _, ok := b.store.Peer(a.id.NodeID); ok {
		t.Fatal("receiver pinned unverified source")
	}
}
func TestTrustStoreDoesNotAcceptSymlinks(t *testing.T) {
	a := newTestEnrollmentNode(t)
	trustFilePath := filepath.Join(a.trustDir, "trusted-peers.json")
	if err := os.Symlink(filepath.Join(t.TempDir(), "other.json"), trustFilePath); err == nil {
		if _, err := OpenTrustStore(a.trustDir, a.id.NodeID); err == nil {
			t.Fatal("symlink trust DB accepted")
		}
	}
}

func TestFirstUseRejectsKeyReplacementAtSameAddress(t *testing.T) {
	a, b, c := newTestEnrollmentNode(t), newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	bServer, err := OpenFirstUseServer(b.raft, b.cert, b.store, policy)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = bServer.Serve(ctx); close(done) }()
	if _, err = TrustFirstUse(context.Background(), a.raft, b.raft, a.cert, a.store, policy); err != nil {
		t.Fatal(err)
	}
	cancel()
	_ = bServer.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("first server did not exit")
	}
	// Simulate a different identity taking over the same LAN/Tailscale IP:port.
	c.raft = b.raft
	startFirstUse(t, c, policy)
	if _, err = TrustFirstUse(context.Background(), a.raft, c.raft, a.cert, a.store, policy); err == nil {
		t.Fatal("pinned key replacement was accepted")
	}
	if p, ok := a.store.Peer(b.id.NodeID); !ok || p.Addresses[0] != b.raft {
		t.Fatal("original pin replaced")
	}
	if _, ok := a.store.Peer(c.id.NodeID); ok {
		t.Fatal("attacker identity became trusted")
	}
}
