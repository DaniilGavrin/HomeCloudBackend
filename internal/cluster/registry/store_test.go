package registry

import (
	"context"
	"database/sql"
	"errors"
	_ "modernc.org/sqlite"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testDB(t *testing.T) (*sql.DB, context.Context) {
	t.Helper()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "registry.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err = db.Exec(`PRAGMA foreign_keys=ON`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	ctx := context.Background()
	if err = Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, db); err != nil {
		t.Fatalf("migration must be idempotent: %v", err)
	}
	if err = VerifySchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	return db, ctx
}

func TestRegistryCRUDAndTombstones(t *testing.T) {
	db, ctx := testDB(t)
	s := NewStore(db)
	snap, err := s.ReadSnapshot(ctx, false)
	if err != nil || snap.Revision != 0 || len(snap.Nodes) != 0 {
		t.Fatalf("empty: %+v %v", snap, err)
	}
	a := testCommand("add-kostroma", ChangeAdd, 0, testNode(testNodeID1))
	r, err := s.ApplyCommitted(ctx, a)
	if err != nil || r.Revision != 1 || r.Replayed {
		t.Fatalf("add %+v %v", r, err)
	}
	duplicate, err := s.ApplyCommitted(ctx, a)
	if err != nil || duplicate.Revision != 1 || !duplicate.Replayed {
		t.Fatalf("idempotency: %+v %v", duplicate, err)
	}
	bad := a
	bad.Node.Name = "evil"
	if _, err = s.ApplyCommitted(ctx, bad); !errors.Is(err, ErrOperationIDReuse) {
		t.Fatalf("reuse: %v", err)
	}
	b := testCommand("add-yaroslavl", ChangeAdd, 1, testNode(testNodeID2))
	b.Node.Name = "Ярославль"
	b.Node.HomeID = "home-yaroslavl"
	b.Node.Endpoints = []Endpoint{{Network: "tailscale", Protocol: "wss", Host: "yaroslavl.ts.net", Port: 443, Priority: 100}}
	if _, err = s.ApplyCommitted(ctx, b); err != nil {
		t.Fatal(err)
	}
	change := testCommand("rename-kostroma", ChangeUpdate, 2, testNode(testNodeID1))
	change.Node.Name = "Кострома — Max"
	change.Node.Endpoints = []Endpoint{{Network: "tailscale", Protocol: "wss", Host: "kostroma.ts.net", Port: 443, Priority: 100}}
	if _, err = s.ApplyCommitted(ctx, change); err != nil {
		t.Fatal(err)
	}
	snap, err = s.ReadSnapshot(ctx, false)
	if err != nil || snap.Revision != 3 || len(snap.Nodes) != 2 || snap.Nodes[0].Name != "Кострома — Max" || len(snap.Nodes[0].Endpoints) != 1 {
		t.Fatalf("snapshot: %+v %v", snap, err)
	}
	stale := testCommand("stale-update", ChangeUpdate, 1, testNode(testNodeID2))
	if _, err = s.ApplyCommitted(ctx, stale); !errors.Is(err, ErrRevisionConflict) {
		t.Fatalf("stale: %v", err)
	}
	remove := testCommand("remove-yaroslavl", ChangeRemove, 3, NodeRecord{})
	remove.NodeID = testNodeID2
	if _, err = s.ApplyCommitted(ctx, remove); err != nil {
		t.Fatal(err)
	}
	snap, err = s.ReadSnapshot(ctx, false)
	if err != nil || snap.Revision != 4 || len(snap.Nodes) != 1 {
		t.Fatalf("filtered: %+v %v", snap, err)
	}
	snap, err = s.ReadSnapshot(ctx, true)
	if err != nil || len(snap.Nodes) != 2 || !snap.Nodes[1].Removed || snap.Nodes[1].Membership != "removed" || len(snap.Nodes[1].Endpoints) != 0 {
		t.Fatalf("tombstone: %+v %v", snap, err)
	}
	resurrect := testCommand("resurrect", ChangeAdd, 4, testNode(testNodeID2))
	if _, err = s.ApplyCommitted(ctx, resurrect); !errors.Is(err, ErrNodeRemoved) {
		t.Fatalf("resurrection: %v", err)
	}
	final, err := s.ReadSnapshot(ctx, true)
	if err != nil || final.Revision != 4 {
		t.Fatalf("rollback: %+v %v", final, err)
	}
}

func TestClusterMismatchAndAtomicRollback(t *testing.T) {
	db, ctx := testDB(t)
	s := NewStore(db)
	c := testCommand("first-add", ChangeAdd, 0, testNode(testNodeID1))
	if _, err := s.ApplyCommitted(ctx, c); err != nil {
		t.Fatal(err)
	}
	wrong := testCommand("other-cluster", ChangeAdd, 1, testNode(testNodeID2))
	wrong.ClusterID = "another-cluster"
	wrong.Node.ClusterID = wrong.ClusterID
	if _, err := s.ApplyCommitted(ctx, wrong); !errors.Is(err, ErrClusterMismatch) {
		t.Fatal(err)
	}
	notFound := testCommand("missing-update", ChangeUpdate, 1, testNode(testNodeID2))
	if _, err := s.ApplyCommitted(ctx, notFound); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	snap, err := s.ReadSnapshot(ctx, false)
	if err != nil || snap.Revision != 1 || len(snap.Nodes) != 1 {
		t.Fatalf("rollback failed %+v %v", snap, err)
	}
}

func TestTwoConcurrentChangesOneWins(t *testing.T) {
	db, ctx := testDB(t)
	s := NewStore(db)
	if _, err := s.ApplyCommitted(ctx, testCommand("first-add", ChangeAdd, 0, testNode(testNodeID1))); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	successes, conflicts := make(chan bool, 2), make(chan bool, 2)
	for _, label := range []string{"name-a", "name-b"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			cmd := testCommand("edit-"+name, ChangeUpdate, 1, testNode(testNodeID1))
			cmd.Node.Name = name
			_, err := s.ApplyCommitted(ctx, cmd)
			if err == nil {
				successes <- true
			} else if errors.Is(err, ErrRevisionConflict) {
				conflicts <- true
			} else {
				t.Errorf("unexpected %v", err)
			}
		}(label)
	}
	wg.Wait()
	if len(successes) != 1 || len(conflicts) != 1 {
		t.Fatalf("expected one write and one conflict: successes=%d conflicts=%d", len(successes), len(conflicts))
	}
}

func TestRestartPersistedAndVersionGuard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if err = Migrate(ctx, db); err != nil {
		t.Fatal(err)
	}
	s := NewStore(db)
	a := testCommand("first-add", ChangeAdd, 0, testNode(testNodeID1))
	a.Timestamp = time.Date(2026, 10, 28, 12, 30, 0, 0, time.UTC)
	if _, err = s.ApplyCommitted(ctx, a); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopened.SetMaxOpenConns(1)
	if err = VerifySchema(ctx, reopened); err != nil {
		t.Fatal(err)
	}
	snap, err := NewStore(reopened).ReadSnapshot(ctx, false)
	if err != nil || snap.ClusterID != testClusterID || snap.Revision != 1 || len(snap.Nodes) != 1 {
		t.Fatalf("restart %+v %v", snap, err)
	}
	if _, err = reopened.Exec(`INSERT INTO hc_cluster_registry_migrations(version,applied_at) VALUES(99,'future')`); err != nil {
		t.Fatal(err)
	}
	if err = Migrate(ctx, reopened); err == nil {
		t.Fatal("expected forward version guard")
	}
}
