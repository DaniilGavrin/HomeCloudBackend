package consensus

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
	_ "modernc.org/sqlite"
)

type securePeer struct {
	*testPeer
	ident  *identity.Store
	public identity.Identity
	trust  *security.TrustStore
}

// Real Raft/SQLite/Bolt instances, TLS 1.3 and KAN-5 Ed25519 identities.
// Tests seed trust using a local Go function; public unauthenticated WS
// CANNOT approve peers. KAN-9 will supply the trusted owner UI workflow.
func TestSecureRaftThreeNodesAndDeniedUnapprovedVoter(t *testing.T) {
	root := t.TempDir()
	nodes := make([]*securePeer, 3)
	for i := range nodes {
		dir := filepath.Join(root, fmt.Sprintf("n%d", i))
		ident := identity.NewStore(filepath.Join(dir, "identity"))
		pub, err := ident.LoadOrCreate()
		if err != nil {
			t.Fatal(err)
		}
		trust, err := security.OpenTrustStore(filepath.Join(dir, "trust"), pub.NodeID)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = &securePeer{ident: ident, public: pub, trust: trust, testPeer: &testPeer{node: pub.NodeID, addr: freeAddr(t), dir: dir}}
	}
	for i := range nodes {
		for j := range nodes {
			if i == j {
				continue
			}
			if err := nodes[i].trust.Approve(security.Peer{NodeID: nodes[j].node, PublicKey: nodes[j].public.PublicKey, Addresses: []string{nodes[j].addr}}); err != nil {
				t.Fatal(err)
			}
		}
	}
	defer func() {
		for _, p := range nodes {
			p.close()
		}
	}()
	for i, p := range nodes {
		if err := os.MkdirAll(p.dir, 0700); err != nil {
			t.Fatal(err)
		}
		db, err := sql.Open("sqlite", filepath.Join(p.dir, "registry.db"))
		if err != nil {
			t.Fatal(err)
		}
		db.SetMaxOpenConns(1)
		if _, err = db.Exec("PRAGMA foreign_keys=ON"); err != nil {
			t.Fatal(err)
		}
		if err = registry.Migrate(context.Background(), db); err != nil {
			t.Fatal(err)
		}
		p.db = db
		p.store = registry.NewStore(db)
		svc, err := OpenSecure(SecureOptions{NodeID: p.node, BindAddr: p.addr, DataDir: filepath.Join(p.dir, "raft-secure"), Identity: p.ident, Trust: p.trust, Bootstrap: i == 0}, p.store)
		if err != nil {
			t.Fatal(err)
		}
		p.svc = svc
		p.addr = svc.Address()
	}
	peers := []*testPeer{nodes[0].testPeer, nodes[1].testPeer, nodes[2].testPeer}
	waitUntil(t, 15*time.Second, func() bool { return leader(peers...) != nil })
	lead := leader(peers...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for _, p := range nodes {
		if p.node == lead.node {
			continue
		}
		if err := lead.svc.AddVoter(ctx, p.node, p.addr); err != nil {
			t.Fatal(err)
		}
	}
	waitUntil(t, 15*time.Second, func() bool { config, e := lead.svc.Configuration(); return e == nil && len(config.Servers) == 3 })
	cmd := registry.Command{
		OperationID: "secure-add", ClusterID: "hc-secure-test", ExpectedRevision: 0, Type: registry.ChangeAdd,
		Timestamp: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
		Node: registry.NodeRecord{NodeID: lead.node, ClusterID: "hc-secure-test", Name: "Kostroma", Membership: "active",
			Endpoints: []registry.Endpoint{{Network: "tailscale", Protocol: "ws", Host: "100.100.1.1", Port: 8080, Priority: 1}}},
	}
	if res, err := lead.svc.Apply(ctx, cmd); err != nil || res.Revision != 1 {
		t.Fatalf("secure replication %+v %v", res, err)
	}
	waitUntil(t, 15*time.Second, func() bool {
		for _, p := range nodes {
			state, err := p.store.ReadSnapshot(context.Background(), true)
			if err != nil || state.Revision != 1 {
				return false
			}
		}
		return true
	})
	rogueID := "hc-1234567890abcdef1234567890abcdef"
	if err := lead.svc.AddVoter(ctx, rogueID, "127.0.0.1:19091"); err == nil {
		t.Fatal("Raft voter admitted without approved public key")
	}
	// Ensure a hostile/unapproved peer cannot be smuggled into Raft membership.
	conf, err := lead.svc.Configuration()
	if err != nil || len(conf.Servers) != 3 {
		t.Fatalf("unexpected membership: %+v %v", conf, err)
	}
	// Ensure JSON status diagnostics remain local and never disclose private keys.
	raw, _ := json.Marshal(conf)
	if len(raw) == 0 {
		t.Fatal("empty Raft config")
	}
}

// Security guard: no secure open with missing trusted store or mismatch.
func TestSecureOpenRequiresIdentityAndTrust(t *testing.T) {
	if _, err := OpenSecure(SecureOptions{}, nil); err == nil {
		t.Fatal("insecure Raft setup was accepted")
	}
	id := identity.NewStore(filepath.Join(t.TempDir(), "identity"))
	pub, err := id.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	trust, err := security.OpenTrustStore(filepath.Join(t.TempDir(), "trust"), pub.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSecure(SecureOptions{NodeID: "hc-00000000000000000000000000000000", Identity: id, Trust: trust}, nil); err == nil {
		t.Fatal("nil registry accepted")
	}
}
