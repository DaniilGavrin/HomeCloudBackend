package database

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"
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
}
