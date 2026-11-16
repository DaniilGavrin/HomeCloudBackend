package security

import (
	"context"
	"errors"
	"testing"
)

func TestFirstUseInvitesWithoutReceivingSideCode(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	server, err := OpenFirstUseServer(b.raft, b.cert, b.store, policy)
	if err != nil {
		t.Fatal(err)
	}
	invited := make(chan struct {
		cluster string
		peer    Peer
	}, 1)
	server.SetJoinHandler(func(_ context.Context, peer Peer, cluster string) error {
		invited <- struct {
			cluster string
			peer    Peer
		}{cluster, peer}
		return nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(ctx) }()
	defer func() { cancel(); server.Close(); <-done }()
	p, err := TrustFirstUseWithCluster(context.Background(), a.raft, b.raft, "cluster-try", a.cert, a.store, policy)
	if err != nil {
		t.Fatal(err)
	}
	if p.NodeID != b.id.NodeID {
		t.Fatal("TLS identity not matched")
	}
	select {
	case got := <-invited:
		if got.cluster != "cluster-try" || got.peer.NodeID != a.id.NodeID {
			t.Fatalf("wrong offer: %+v", got)
		}
	default:
		t.Fatal("invite not delivered")
	}
	if _, ok := a.store.Peer(b.id.NodeID); !ok {
		t.Fatal("initiator not pinned")
	}
	if _, ok := b.store.Peer(a.id.NodeID); !ok {
		t.Fatal("recipient not pinned")
	}
}
func TestRejectedInvitationCannotPinOnRecipient(t *testing.T) {
	a, b := newTestEnrollmentNode(t), newTestEnrollmentNode(t)
	policy := testNetworks(t)
	server, err := OpenFirstUseServer(b.raft, b.cert, b.store, policy)
	if err != nil {
		t.Fatal(err)
	}
	server.SetJoinHandler(func(context.Context, Peer, string) error { return errors.New("already belongs to another cluster") })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = server.Serve(ctx); close(done) }()
	defer func() { cancel(); server.Close(); <-done }()
	if _, err := TrustFirstUseWithCluster(context.Background(), a.raft, b.raft, "cluster-foreign", a.cert, a.store, policy); err == nil {
		t.Fatal("refused cluster was enrolled")
	}
	if _, ok := b.store.Peer(a.id.NodeID); ok {
		t.Fatal("recipient pinned refused invite")
	}
	if _, ok := a.store.Peer(b.id.NodeID); ok {
		t.Fatal("initiator pinned refused invite")
	}
}
