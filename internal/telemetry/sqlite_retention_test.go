package telemetry

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteHistoryPrunesExpiredRawSamples(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if err := InitSQLiteSchema(ctx, db); err != nil {
		t.Fatal(err)
	}
	history, err := NewSQLiteHistoryWithRetention(db, 8, time.Second, time.Second, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()

	now := time.UnixMilli(100000).UTC()
	expired := now.Add(-5 * time.Second)
	if err := history.writeBatch(ctx, []Sample{{
		NodeID: "node-a", Name: "cpu.usage_percent",
		Value: 10, Timestamp: expired,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := history.prune(ctx, now); err != nil {
		t.Fatal(err)
	}
	points, err := history.Query(ctx, Query{
		NodeID: "node-a", Metric: "cpu.usage_percent",
		From: expired.Add(-time.Second), To: expired.Add(time.Second),
		Resolution: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 0 {
		t.Fatalf("expected expired samples to be pruned, got %#v", points)
	}
}
