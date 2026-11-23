package physicaldevices

import (
	"context"
	"database/sql"
	"testing"

	_ "modernc.org/sqlite"
)

func TestMigrateCreatesPhysicalRegistryAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:physicaldevices-schema-test?mode=memory&cache=shared")
	if err != nil { t.Fatal(err) }
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil { t.Fatal(err) }
	if err := Migrate(ctx, db); err != nil { t.Fatalf("first migration: %v", err) }
	if err := Migrate(ctx, db); err != nil { t.Fatalf("second migration: %v", err) }
	if err := VerifySchema(ctx, db); err != nil { t.Fatalf("verify schema: %v", err) }

	for _, table := range []string{
		"hc_physical_devices", "hc_device_identity_mappings",
		"hc_physical_device_capabilities", "hc_physical_device_entities",
		"hc_physical_device_state",
	} {
		var name string
		if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name); err != nil {
			t.Errorf("table %s missing: %v", table, err)
		}
	}
}

func TestIdentityMappingUniquenessOnlyForStableIdentity(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:physicaldevices-identity-test?mode=memory&cache=shared")
	if err != nil { t.Fatal(err) }
	defer db.Close()
	db.SetMaxOpenConns(1)
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys=ON"); err != nil { t.Fatal(err) }
	if err := Migrate(ctx, db); err != nil { t.Fatal(err) }

	for _, id := range []string{"device-a", "device-b", "device-c"} {
		if _, err := db.ExecContext(ctx, `INSERT INTO hc_physical_devices
			(device_id,device_type_id,device_type_version,created_at,updated_at)
			VALUES(?,?,1,'now','now')`, id, "smart_plug"); err != nil { t.Fatal(err) }
	}
	insert := `INSERT INTO hc_device_identity_mappings
		(mapping_id,integration_id,native_device_id,identity_quality,device_id,created_at,last_seen_at)
		VALUES(?,?,?,?,?,'now','now')`
	if _, err := db.ExecContext(ctx, insert, "map-a", "wifi.local", "same-native-id", "stable", "device-a"); err != nil {
		t.Fatalf("first stable mapping: %v", err)
	}
	if _, err := db.ExecContext(ctx, insert, "map-b", "wifi.local", "same-native-id", "stable", "device-b"); err == nil {
		t.Fatal("duplicate stable identity was accepted")
	}
	if _, err := db.ExecContext(ctx, insert, "map-c", "ble", "temporary-id", "weak", "device-b"); err != nil {
		t.Fatalf("first weak mapping: %v", err)
	}
	if _, err := db.ExecContext(ctx, insert, "map-d", "ble", "temporary-id", "weak", "device-c"); err != nil {
		t.Fatalf("reused weak identity must not force a merge: %v", err)
	}
}
