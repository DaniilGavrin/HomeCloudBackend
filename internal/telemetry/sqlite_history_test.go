package telemetry

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteHistoryAppendAndQuery(t *testing.T) {
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
	history, err := NewSQLiteHistory(db, 8)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := history.Append(ctx, []Sample{{
		NodeID: "node-a", Name: "memory.used_percent",
		Labels: map[string]string{"kind": "ram"},
		Value: 42.5, Unit: "percent", Timestamp: now,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := history.Close(); err != nil {
		t.Fatal(err)
	}
	points, err := history.Query(ctx, Query{
		NodeID: "node-a", Metric: "memory.used_percent",
		From: now.Add(-time.Second), To: now.Add(time.Second),
		Resolution: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("expected one point, got %d", len(points))
	}
	if points[0].Value != 42.5 || points[0].Labels["kind"] != "ram" {
		t.Fatalf("unexpected point: %#v", points[0])
	}
}

func TestSQLiteHistoryRejectsUnsupportedResolution(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	if err := InitSQLiteSchema(context.Background(), db); err != nil {
		t.Fatal(err)
	}
	history, err := NewSQLiteHistory(db, 8)
	if err != nil {
		t.Fatal(err)
	}
	defer history.Close()
	_, err = history.Query(context.Background(), Query{
		NodeID: "node-a", Metric: "cpu.usage",
		From: time.Now().Add(-time.Hour), To: time.Now(),
		Resolution: 5 * time.Second,
	})
	if err != ErrInvalidQuery {
		t.Fatalf("expected ErrInvalidQuery, got %v", err)
	}
}
