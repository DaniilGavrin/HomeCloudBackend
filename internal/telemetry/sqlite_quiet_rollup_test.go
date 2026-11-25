package telemetry

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

func TestSQLiteHistoryBuildsMinuteRollupAfterQuietInterval(t *testing.T) {
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil { t.Fatal(err) }
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	if err := InitSQLiteSchema(ctx, db); err != nil { t.Fatal(err) }
	history, err := NewSQLiteHistory(db, 8)
	if err != nil { t.Fatal(err) }
	defer history.Close()

	samples := []Sample{
		{NodeID:"node-a", Name:"cpu.usage_percent", Value:30, Timestamp:time.UnixMilli(121000).UTC()},
		{NodeID:"node-a", Name:"cpu.usage_percent", Value:50, Timestamp:time.UnixMilli(122000).UTC()},
	}
	if err := history.writeBatch(ctx, samples); err != nil { t.Fatal(err) }
	if err := history.compact(ctx, time.UnixMilli(130000).UTC()); err != nil { t.Fatal(err) }
	// No more samples arrive during the remainder of this minute.
	if err := history.compact(ctx, time.UnixMilli(180000).UTC()); err != nil { t.Fatal(err) }

	points, err := history.Query(ctx, Query{
		NodeID:"node-a", Metric:"cpu.usage_percent",
		From:time.UnixMilli(120000).UTC(), To:time.UnixMilli(180000).UTC(),
		Resolution:time.Minute,
	})
	if err != nil { t.Fatal(err) }
	if len(points) != 1 || points[0].Value != 40 || points[0].Count != 2 {
		t.Fatalf("expected minute rollup despite quiet interval, got %#v", points)
	}
}
