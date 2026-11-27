package database

import (
	"context"
		"os"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/telemetry"
	"HomeCloudBackend/wsconfig"
)

func TestPostgreSQLSchemaAndParameterBinding(t *testing.T) {
	dsn := os.Getenv("HOMECLOUD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("HOMECLOUD_TEST_POSTGRES_DSN is not set")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL test DSN: %v", err)
	}
	port := 5432
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
		if err != nil {
			t.Fatalf("parse PostgreSQL test port: %v", err)
		}
	}
	username := ""
	password := ""
	if parsed.User != nil {
		username = parsed.User.Username()
		password, _ = parsed.User.Password()
	}
	mode := wsconfig.DBPostgreSQLRemote
	cfg := &wsconfig.Config{}
	cfg.TypeStart.DBByte = &mode
	cfg.DBServer.Host = parsed.Hostname()
	cfg.DBServer.Port = port
	cfg.DBServer.Database = strings.TrimPrefix(parsed.Path, "/")
	cfg.DBServer.Username = username
	cfg.DBServer.Password = password
	cfg.DBServer.ConnectionTimeout = 10
	cfg.DBServer.MaxOpenConnections = 5
	cfg.DBServer.MaxIdleConnections = 2
	db, err := Open(ctx, cfg, t.TempDir())
	if err != nil {
		t.Fatalf("open PostgreSQL through database.Open: %v", err)
	}
	defer db.Close()

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
	// Wait until the sample's ten-second bucket is complete so the real
	// background compactor executes PostgreSQL INSERT .. SELECT .. ON CONFLICT.
	nextBucket := now.Truncate(10 * time.Second).Add(10 * time.Second)
	time.Sleep(time.Until(nextBucket.Add(1200 * time.Millisecond)))
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
	rollups, err := history.Query(ctx, telemetry.Query{
		NodeID: "hc-integration-node", Metric: "host.cpu.usage",
		From: now.Add(-10*time.Second), To: now.Add(20*time.Second),
		Resolution: 10*time.Second, Limit: 10,
	})
	if err != nil || len(rollups) != 1 || rollups[0].Value != 42.5 || rollups[0].Count != 1 {
		t.Fatalf("query PostgreSQL telemetry rollup: points=%+v err=%v", rollups, err)
	}
}
