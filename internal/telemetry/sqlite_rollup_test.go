package telemetry

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteHistoryRollsUpAndRetainsAggregates(t *testing.T) {
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
	defer history.Close()

	// The compactor closes the 110s bucket when called at 120s.
	base := time.UnixMilli(111000).UTC()
	samples := []Sample{
		{NodeID: "node-a", Name: "cpu.usage_percent", Value: 10, Timestamp: base},
		{NodeID: "node-a", Name: "cpu.usage_percent", Value: 20, Timestamp: base.Add(time.Second)},
	}
	if err := history.writeBatch(ctx, samples); err != nil {
		t.Fatal(err)
	}
	if err := history.compact(ctx, time.UnixMilli(120000).UTC()); err != nil {
		t.Fatal(err)
	}
	points, err := history.Query(ctx, Query{
		NodeID: "node-a", Metric: "cpu.usage_percent",
		From: time.UnixMilli(110000).UTC(), To: time.UnixMilli(120000).UTC(),
		Resolution: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(points) != 1 {
		t.Fatalf("expected one 10s aggregate, got %d", len(points))
	}
	point := points[0]
	if point.Value != 15 || point.Min != 10 || point.Max != 20 || point.Count != 2 {
		t.Fatalf("unexpected 10s aggregate: %#v", point)
	}

	// At the minute boundary, the same 10s aggregate is promoted to 1m history.
	if err := history.compact(ctx, time.UnixMilli(180000).UTC()); err != nil {
		t.Fatal(err)
	}
	minute, err := history.Query(ctx, Query{
		NodeID: "node-a", Metric: "cpu.usage_percent",
		From: time.UnixMilli(120000).UTC(), To: time.UnixMilli(180000).UTC(),
		Resolution: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(minute) != 1 || minute[0].Value != 15 || minute[0].Count != 2 {
		t.Fatalf("unexpected 1m aggregate: %#v", minute)
	}
}
