// Package physicaldevices owns canonical physical-device records. It is
// intentionally separate from internal/devices, which stores WS clients.
package physicaldevices

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const CurrentSchemaVersion = 1

var schemaV1 = []string{
	`CREATE TABLE hc_physical_devices (
		device_id TEXT PRIMARY KEY,
		device_type_id TEXT NOT NULL,
		device_type_version INTEGER NOT NULL CHECK(device_type_version > 0),
		metadata_json TEXT NOT NULL DEFAULT '{}',
		capability_revision BIGINT NOT NULL DEFAULT 0 CHECK(capability_revision >= 0),
		status TEXT NOT NULL DEFAULT 'unsupported' CHECK(status IN ('active','unsupported','removed')),
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		last_seen_at TEXT
	)`,
	`CREATE INDEX idx_hc_physical_devices_type ON hc_physical_devices(device_type_id,status)`,
	`CREATE TABLE hc_device_identity_mappings (
		mapping_id TEXT PRIMARY KEY,
		integration_id TEXT NOT NULL,
		native_device_id TEXT NOT NULL,
		identity_quality TEXT NOT NULL CHECK(identity_quality IN ('stable','session_only','weak')),
		device_id TEXT NOT NULL REFERENCES hc_physical_devices(device_id) ON DELETE RESTRICT,
		created_at TEXT NOT NULL,
		last_seen_at TEXT NOT NULL
	)`,
	`CREATE INDEX idx_hc_device_identity_lookup ON hc_device_identity_mappings(integration_id,native_device_id)`,
	`CREATE UNIQUE INDEX idx_hc_device_identity_stable ON hc_device_identity_mappings(integration_id,native_device_id) WHERE identity_quality='stable'`,
	`CREATE INDEX idx_hc_device_identity_device ON hc_device_identity_mappings(device_id)`,
	`CREATE TABLE hc_physical_device_capabilities (
		device_id TEXT NOT NULL REFERENCES hc_physical_devices(device_id) ON DELETE CASCADE,
		capability_id TEXT NOT NULL,
		version INTEGER NOT NULL CHECK(version > 0),
		declaration_json TEXT NOT NULL,
		accepted INTEGER NOT NULL DEFAULT 0 CHECK(accepted IN (0,1)),
		updated_at TEXT NOT NULL,
		PRIMARY KEY(device_id,capability_id)
	)`,
	`CREATE TABLE hc_physical_device_entities (
		device_id TEXT NOT NULL REFERENCES hc_physical_devices(device_id) ON DELETE CASCADE,
		entity_id TEXT NOT NULL,
		capability_id TEXT NOT NULL,
		value_type TEXT NOT NULL,
		unit TEXT NOT NULL DEFAULT '',
		declaration_json TEXT NOT NULL,
		PRIMARY KEY(device_id,entity_id),
		FOREIGN KEY(device_id,capability_id) REFERENCES hc_physical_device_capabilities(device_id,capability_id) ON DELETE CASCADE
	)`,
	`CREATE TABLE hc_physical_device_state (
		device_id TEXT NOT NULL REFERENCES hc_physical_devices(device_id) ON DELETE CASCADE,
		entity_id TEXT NOT NULL,
		value_json TEXT NOT NULL,
		measured_at TEXT NOT NULL,
		received_at TEXT NOT NULL,
		capability_revision INTEGER NOT NULL,
		PRIMARY KEY(device_id,entity_id),
		FOREIGN KEY(device_id,entity_id) REFERENCES hc_physical_device_entities(device_id,entity_id) ON DELETE CASCADE
	)`,
}

func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("physical device migration begin: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS hc_physical_device_migrations (
		version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL
	)`); err != nil {
		return fmt.Errorf("physical device migration metadata: %w", err)
	}
	var version int
	if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM hc_physical_device_migrations`).Scan(&version); err != nil {
		return err
	}
	if version > CurrentSchemaVersion {
		return fmt.Errorf("physical device schema version %d is newer than supported %d", version, CurrentSchemaVersion)
	}
	if version == 0 {
		for _, stmt := range schemaV1 {
			if _, err := tx.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("physical device schema v1: %w", err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO hc_physical_device_migrations(version,applied_at) VALUES(?,?)`,
			CurrentSchemaVersion, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func VerifySchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version),0) FROM hc_physical_device_migrations`).Scan(&version); err != nil {
		return errors.New("physical device schema missing: run go run . init-db")
	}
	if version != CurrentSchemaVersion {
		return fmt.Errorf("physical device schema version %d; expected %d", version, CurrentSchemaVersion)
	}
	return nil
}
