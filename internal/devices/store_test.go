package devices

import (
	"context"
	"errors"
	"testing"

	_ "modernc.org/sqlite"
	"database/sql"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil { t.Fatal(err) }
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE hc_devices (
		device_id TEXT PRIMARY KEY, client_type TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
		first_seen_at INTEGER NOT NULL, last_seen_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
	)`)
	if err != nil { t.Fatal(err) }
	t.Cleanup(func(){ _ = db.Close() })
	return New(db)
}

func TestRegisterPreservesCustomNameAndListUsesLiveState(t *testing.T) {
	ctx := context.Background()
	store := testStore(t)
	if err := store.Register(ctx, "storage-1", "storage_service"); err != nil { t.Fatal(err) }
	if _, err := store.Rename(ctx, "storage-1", "Сервер хранения"); err != nil { t.Fatal(err) }
	if err := store.Register(ctx, "storage-1", "storage_service"); err != nil { t.Fatal(err) }
	items, err := store.List(ctx, func(id string) bool { return id == "storage-1" })
	if err != nil { t.Fatal(err) }
	if len(items) != 1 || items[0].Name != "Сервер хранения" || !items[0].Online {
		t.Fatalf("unexpected devices: %+v", items)
	}
}

func TestRenameValidationAndNotFound(t *testing.T) {
	store := testStore(t)
	ctx := context.Background()
	if _, err := store.Rename(ctx, "missing", "Name"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
	if err := store.Register(ctx, "device-1", "desktop"); err != nil { t.Fatal(err) }
	for _, name := range []string{"", "   ", "bad\nname"} {
		if _, err := store.Rename(ctx, "device-1", name); err == nil {
			t.Fatalf("expected invalid name error for %q", name)
		}
	}
	if _, err := store.Rename(ctx, "device-1", "Рабочий компьютер"); err != nil { t.Fatal(err) }
}
