package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"path/filepath"
	"testing"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
)

type reconcileDir struct{ registry.Snapshot }

func (d reconcileDir) ReadSnapshot(_ context.Context, _ bool) (registry.Snapshot, error) {
	return d.Snapshot, nil
}
func TestReconcileRemovedTrustAfterOfflineRestart(t *testing.T) {
	a, _, _ := ed25519.GenerateKey(rand.Reader)
	b, _, _ := ed25519.GenerateKey(rand.Reader)
	c, _, _ := ed25519.GenerateKey(rand.Reader)
	id := security.NodeID(a)
	store, err := security.OpenTrustStore(filepath.Join(t.TempDir(), "trust"), id)
	if err != nil {
		t.Fatal(err)
	}
	removed := security.Peer{NodeID: security.NodeID(b), PublicKey: base64.StdEncoding.EncodeToString(b), Addresses: []string{"127.0.0.1:19001"}}
	active := security.Peer{NodeID: security.NodeID(c), PublicKey: base64.StdEncoding.EncodeToString(c), Addresses: []string{"127.0.0.1:19002"}}
	if err := store.Approve(removed); err != nil {
		t.Fatal(err)
	}
	if err := store.Approve(active); err != nil {
		t.Fatal(err)
	}
	d := reconcileDir{registry.Snapshot{ClusterID: "demo", Revision: 20, Nodes: []registry.NodeRecord{{NodeID: removed.NodeID, Removed: true}, {NodeID: active.NodeID, Membership: "active"}}}}
	if err := ReconcileRemoved(context.Background(), d, store); err != nil {
		t.Fatal(err)
	}
	if err := ReconcileRemoved(context.Background(), d, store); err != nil {
		t.Fatal(err)
	}
	if _, ok := store.Peer(removed.NodeID); ok {
		t.Fatal("removed identity was resurrected")
	}
	if _, ok := store.Peer(active.NodeID); !ok {
		t.Fatal("active trust entry lost")
	}
}
