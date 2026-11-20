package database

import (
	"context"
	"database/sql"
	"testing"
)

func TestInitAndVerifyDoNotRequireOrDropLegacyTelemetryTables(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	// Simulate an installation that still has data from the removed Monitoring
	// implementation. Initialization must neither require nor delete that data.
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE hc_metric_samples (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO hc_metric_samples(id, value) VALUES (1, 'preserve-me');
	`); err != nil {
		t.Fatal(err)
	}

	if err := Init(ctx, db); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}
	if err := Verify(ctx, db); err != nil {
		t.Fatalf("Verify() must not require telemetry schema: %v", err)
	}

	var value string
	if err := db.QueryRowContext(ctx,
		"SELECT value FROM hc_metric_samples WHERE id = 1",
	).Scan(&value); err != nil {
		t.Fatalf("legacy telemetry data was removed: %v", err)
	}
	if value != "preserve-me" {
		t.Fatalf("legacy telemetry data changed: got %q", value)
	}
}

func TestVerifyAcceptsFreshCurrentSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)

	if err := Init(ctx, db); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}
	if err := Verify(ctx, db); err != nil {
		t.Fatalf("Verify() failed for initialized schema: %v", err)
	}
}
