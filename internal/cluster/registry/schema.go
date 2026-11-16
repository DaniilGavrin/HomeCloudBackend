package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// Registry schema is independently versioned. Explicitly migrate only during
// `init-db` or when the existing db_server.auto_migrate setting is enabled.
// No DROP/DELETE is performed on existing HomeCloud tables or data.
const CurrentSchemaVersion = 1

var schemaV1 = []string{
	`CREATE TABLE hc_cluster_registry_meta (
        singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
        cluster_id TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 0 CHECK(revision >= 0),
        updated_at TEXT NOT NULL
    )`,
	`CREATE TABLE hc_cluster_registry_nodes (
        node_id TEXT PRIMARY KEY,
        cluster_id TEXT NOT NULL,
        home_id TEXT NOT NULL DEFAULT '',
        display_name TEXT NOT NULL,
        membership TEXT NOT NULL CHECK (membership IN ('pending','active','removed')),
        revision INTEGER NOT NULL CHECK(revision > 0),
        removed INTEGER NOT NULL DEFAULT 0 CHECK(removed IN (0,1)),
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )`,
	`CREATE INDEX idx_hc_cluster_nodes_active ON hc_cluster_registry_nodes(removed, display_name, node_id)`,
	`CREATE TABLE hc_cluster_registry_endpoints (
        node_id TEXT NOT NULL REFERENCES hc_cluster_registry_nodes(node_id) ON DELETE CASCADE,
        network TEXT NOT NULL,
        protocol TEXT NOT NULL,
        host TEXT NOT NULL,
        port INTEGER NOT NULL CHECK(port BETWEEN 1 AND 65535),
        priority INTEGER NOT NULL CHECK(priority BETWEEN 0 AND 1000),
        PRIMARY KEY(node_id, network, protocol, host, port)
    )`,
	`CREATE TABLE hc_cluster_registry_applied (
        operation_id TEXT PRIMARY KEY,
        command_hash TEXT NOT NULL,
        revision INTEGER NOT NULL UNIQUE,
        applied_at TEXT NOT NULL
    )`,
}

func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("registry migration begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS hc_cluster_registry_migrations (
        version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
    )`); err != nil {
		return fmt.Errorf("registry migration metadata: %w", err)
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM hc_cluster_registry_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > CurrentSchemaVersion {
		return fmt.Errorf("registry schema version %d is newer than supported %d", version, CurrentSchemaVersion)
	}
	if version == 0 {
		for _, stmt := range schemaV1 {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("registry schema v1: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_migrations(version,applied_at) VALUES(?,?)`, 1, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func VerifySchema(ctx context.Context, db *sql.DB) error {
	var version int
	err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM hc_cluster_registry_migrations`).Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrSchemaMissing
		}
		return fmt.Errorf("%w: %v", ErrSchemaMissing, err)
	}
	if version != CurrentSchemaVersion {
		return fmt.Errorf("registry schema version %d; expected %d", version, CurrentSchemaVersion)
	}
	return nil
}
