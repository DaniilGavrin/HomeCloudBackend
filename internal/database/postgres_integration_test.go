package database

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/telemetry"
)

func TestPostgreSQLSchemaAndParameterBinding(t *testing.T) {
	dsn := os.Getenv("HOMECLOUD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("HOMECLOUD_TEST_POSTGRES_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	db, err := sql.Open("postgres-rebind", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(5)
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	var rebound string
	if err := db.QueryRowContext(ctx, "SELECT ?::text || '?'::text", "bound").Scan(&rebound); err != nil {
		t.Fatalf("query with rebound positional parameter: %v", err)
	}
	if rebound != "bound?" {
		t.Fatalf("parameter binding result = %q, want %q", rebound, "bound?")
	}

	if err := Init(ctx, db); err != nil {
		t.Fatalf("initialize PostgreSQL schema: %v", err)
	}
	if err := Verify(ctx, db); err != nil {
		t.Fatalf("verify PostgreSQL schema: %v", err)
	}

	command := registry.Command{
		OperationID: "operation-001",
		ClusterID: "cluster-001",
		ExpectedRevision: 0,
		Type: registry.ChangeAdd,
		Timestamp: time.Now().UTC(),
		Node: registry.NodeRecord{
			NodeID: "hc-00000000000000000000000000000001",
			ClusterID: "cluster-001",
			Name: "PostgreSQL integration node",
			Membership: "active",
			Endpoints: []registry.Endpoint{{
				Network: "lan", Protocol: "ws", Host: "192.168.1.20", Port: 8080, Priority: 10,
			}},
		},
	}
	store := registry.NewStore(db)
	applied, err := store.ApplyCommitted(ctx, command)
	if err != nil || applied.Revision != 1 {
		t.Fatalf("apply registry command: result=%+v err=%v", applied, err)
	}
	snapshot, err := store.ReadSnapshot(ctx, false)
	if err != nil || snapshot.Revision != 1 || len(snapshot.Nodes) != 1 {
		t.Fatalf("read registry snapshot: snapshot=%+v err=%v", snapshot, err)
	}

	now := time.Now().UTC()
	history, err := telemetry.NewSQLHistoryWithRetention(db, 8, time.Hour, 24*time.Hour, 14*24*time.Hour)
	if err != nil {
		t.Fatalf("create PostgreSQL telemetry history: %v", err)
	}
	if err := history.Append(ctx, []telemetry.Sample{{
		NodeID: "hc-integration-node", Name: "host.cpu.usage", Value: 42.5, Unit: "percent", Timestamp: now,
	}}); err != nil {
		t.Fatalf("append PostgreSQL telemetry sample: %v", err)
	}
	if err := history.Close(); err != nil {
		t.Fatalf("close PostgreSQL telemetry history: %v", err)
	}
	points, err := history.Query(ctx, telemetry.Query{
		NodeID: "hc-integration-node", Metric: "host.cpu.usage",
		From: now.Add(-time.Second), To: now.Add(2*time.Second),
		Resolution: time.Second, Limit: 10,
	})
	if err != nil || len(points) != 1 || points[0].Value != 42.5 {
		t.Fatalf("query PostgreSQL telemetry history: points=%+v err=%v", points, err)
	}
}
