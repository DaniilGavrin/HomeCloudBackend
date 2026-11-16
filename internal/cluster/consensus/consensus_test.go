package consensus

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"github.com/hashicorp/raft"
	_ "modernc.org/sqlite"
)

type testPeer struct {
	db    *sql.DB
	store *registry.Store
	svc   *Service
	node  string
	addr  string
	dir   string
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}
func startPeer(t *testing.T, nodeID, addr, dir string, bootstrap bool) *testPeer {
	t.Helper()
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", filepath.Join(dir, "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	if err = registry.Migrate(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	store := registry.NewStore(db)
	svc, err := Open(Options{NodeID: nodeID, BindAddr: addr, DataDir: filepath.Join(dir, "raft"), Bootstrap: bootstrap}, store)
	if err != nil {
		t.Fatalf("open %s: %v", nodeID, err)
	}
	return &testPeer{db: db, store: store, svc: svc, node: nodeID, addr: svc.Address(), dir: dir}
}
func (p *testPeer) close() {
	if p.svc != nil {
		_ = p.svc.Close()
		p.svc = nil
	}
	if p.db != nil {
		_ = p.db.Close()
		p.db = nil
	}
}
func waitUntil(t *testing.T, d time.Duration, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("timeout waiting for Raft condition")
}
func leader(peers ...*testPeer) *testPeer {
	for _, p := range peers {
		if p.svc != nil && p.svc.RaftState() == raft.Leader {
			return p
		}
	}
	return nil
}

// Three independent real Raft TCP transports and durable SQLite projections.
// This deliberately runs over loopback only until KAN-8 mTLS/pairing is ready.
func TestThreeNodesElectionReplicationAndFailover(t *testing.T) {
	ids := []string{"hc-" + strings.Repeat("a", 32), "hc-" + strings.Repeat("b", 32), "hc-" + strings.Repeat("c", 32)}
	root := t.TempDir()
	peers := make([]*testPeer, 3)
	for i := 0; i < 3; i++ {
		peers[i] = startPeer(t, ids[i], freeAddr(t), filepath.Join(root, fmt.Sprintf("node%d", i)), i == 0)
	}
	defer func() {
		for _, p := range peers {
			p.close()
		}
	}()
	waitUntil(t, 10*time.Second, func() bool { return leader(peers...) != nil })
	first := leader(peers...)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	voters := 1
	for _, p := range peers {
		if p != first {
			if err := first.svc.AddVoter(ctx, p.node, p.addr); err != nil {
				t.Fatal(err)
			}
			voters++
			expected := voters
			waitUntil(t, 15*time.Second, func() bool {
				conf, e := first.svc.Configuration()
				if e != nil || len(conf.Servers) != expected {
					return false
				}
				for _, member := range conf.Servers {
					if member.Suffrage != raft.Voter {
						return false
					}
				}
				return true
			})
		}
	}
	waitUntil(t, 15*time.Second, func() bool { conf, err := first.svc.Configuration(); return err == nil && len(conf.Servers) == 3 })
	cmd := registry.Command{
		OperationID: "add-kostroma", ClusterID: "hc-homecluster", ExpectedRevision: 0, Type: registry.ChangeAdd, Timestamp: time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC),
		Node: registry.NodeRecord{NodeID: ids[0], ClusterID: "hc-homecluster", Name: "Кострома", Membership: "active", Endpoints: []registry.Endpoint{{Network: "tailscale", Protocol: "ws", Host: "100.64.1.1", Port: 8080, Priority: 100}}},
	}
	if r, err := first.svc.Apply(ctx, cmd); err != nil || r.Revision != 1 {
		t.Fatalf("apply: %+v %v", r, err)
	}
	waitUntil(t, 15*time.Second, func() bool {
		for _, p := range peers {
			snap, err := p.store.ReadSnapshot(context.Background(), true)
			if err != nil || snap.Revision != 1 || len(snap.Nodes) != 1 {
				return false
			}
		}
		return true
	})
	if err := first.svc.Snapshot(); err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	first.close()
	alive := make([]*testPeer, 0, 2)
	for _, p := range peers {
		if p != first {
			alive = append(alive, p)
		}
	}
	waitUntil(t, 15*time.Second, func() bool { return leader(alive...) != nil })
	replacement := leader(alive...)
	cmd.OperationID = "rename-kostroma"
	cmd.Type = registry.ChangeUpdate
	cmd.ExpectedRevision = 1
	cmd.Timestamp = cmd.Timestamp.Add(time.Minute)
	cmd.Node.Name = "Кострома — Max"
	if r, err := replacement.svc.Apply(ctx, cmd); err != nil || r.Revision != 2 {
		t.Fatalf("failover write: %+v %v", r, err)
	}
	waitUntil(t, 12*time.Second, func() bool {
		for _, p := range alive {
			snap, e := p.store.ReadSnapshot(context.Background(), true)
			if e != nil || snap.Revision != 2 {
				return false
			}
		}
		return true
	})
	// Reopen the ORIGINAL leader with its old Bolt state and SQLite, never re-bootstrap.
	revived := startPeer(t, first.node, first.addr, first.dir, false)
	for i, p := range peers {
		if p == first {
			peers[i] = revived
		}
	}
	waitUntil(t, 20*time.Second, func() bool {
		snap, e := revived.store.ReadSnapshot(context.Background(), true)
		return e == nil && snap.Revision == 2 && len(snap.Nodes) == 1 && snap.Nodes[0].Name == "Кострома — Max"
	})
	if got := leader(peers...); got == nil {
		t.Fatal("no leader after rejoin")
	}
}

func TestNoQuorumRejectsNewRegistryWrites(t *testing.T) {
	ids := []string{"hc-" + strings.Repeat("d", 32), "hc-" + strings.Repeat("e", 32), "hc-" + strings.Repeat("f", 32)}
	root := t.TempDir()
	ps := make([]*testPeer, 3)
	for i := range ps {
		ps[i] = startPeer(t, ids[i], freeAddr(t), filepath.Join(root, fmt.Sprintf("no-quorum-%d", i)), i == 0)
	}
	defer func() {
		for _, p := range ps {
			p.close()
		}
	}()
	waitUntil(t, 10*time.Second, func() bool { return leader(ps...) != nil })
	current := leader(ps...)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	voters := 1
	for _, p := range ps {
		if p != current {
			if err := current.svc.AddVoter(ctx, p.node, p.addr); err != nil {
				t.Fatal(err)
			}
			voters++
			expected := voters
			waitUntil(t, 15*time.Second, func() bool {
				conf, e := current.svc.Configuration()
				if e != nil || len(conf.Servers) != expected {
					return false
				}
				for _, member := range conf.Servers {
					if member.Suffrage != raft.Voter {
						return false
					}
				}
				return true
			})
		}
	}
	waitUntil(t, 15*time.Second, func() bool { conf, e := current.svc.Configuration(); return e == nil && len(conf.Servers) == 3 })
	for _, p := range ps {
		if p != current {
			p.close()
		}
	}
	cmd := registry.Command{OperationID: "quorum-write", ClusterID: "test-cluster", ExpectedRevision: 0, Type: registry.ChangeAdd, Timestamp: time.Now().UTC(), Node: registry.NodeRecord{NodeID: current.node, ClusterID: "test-cluster", Name: "Isolated", Membership: "active", Endpoints: []registry.Endpoint{{Network: "lan", Protocol: "ws", Host: "127.0.0.1", Port: 8080}}}}
	limited, stop := context.WithTimeout(context.Background(), 6*time.Second)
	defer stop()
	if _, err := current.svc.Apply(limited, cmd); err == nil {
		t.Fatal("a minority must not commit writes")
	}
	snap, err := current.store.ReadSnapshot(context.Background(), true)
	if err != nil || snap.Revision != 0 {
		t.Fatalf("minority modified registry: %+v %v", snap, err)
	}
}
