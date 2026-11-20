package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/wsconfig"
	_ "modernc.org/sqlite"
)

const schema = `
CREATE TABLE IF NOT EXISTS hc_storage_volumes (
 id TEXT PRIMARY KEY,
 name TEXT NOT NULL,
 path TEXT NOT NULL UNIQUE,
 priority INTEGER NOT NULL DEFAULT 0,
 enabled INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS hc_storage_files (
 id TEXT PRIMARY KEY,
 storage_id TEXT NOT NULL REFERENCES hc_storage_volumes(id),
 name TEXT NOT NULL,
 mime_type TEXT NOT NULL,
 size_bytes INTEGER NOT NULL,
 sha256 TEXT NOT NULL,
 created_at TEXT NOT NULL,
 uploaded_by TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hc_storage_files_storage ON hc_storage_files(storage_id,created_at);
CREATE TABLE IF NOT EXISTS hc_storage_index (
 id TEXT PRIMARY KEY,
 storage_id TEXT NOT NULL REFERENCES hc_storage_volumes(id),
 relative_path TEXT NOT NULL,
 name TEXT NOT NULL,
 mime_type TEXT NOT NULL,
 size_bytes INTEGER NOT NULL,
 modified_at TEXT NOT NULL,
 indexed_at TEXT NOT NULL,
 seen_run TEXT NOT NULL,
 available INTEGER NOT NULL DEFAULT 1,
 UNIQUE(storage_id, relative_path)
);
CREATE INDEX IF NOT EXISTS idx_hc_storage_index_storage ON hc_storage_index(storage_id, indexed_at);
CREATE TABLE IF NOT EXISTS hc_devices (
    device_id TEXT PRIMARY KEY,
    client_type TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_hc_devices_last_seen ON hc_devices(last_seen_at);
CREATE TABLE IF NOT EXISTS audit_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_time TEXT NOT NULL,
    subject TEXT NOT NULL,
    action TEXT NOT NULL,
    result TEXT NOT NULL
);
`

func Open(ctx context.Context, cfg *wsconfig.Config, root string) (*sql.DB, error) {
	if cfg.TypeStart.DBByte == nil || *cfg.TypeStart.DBByte != wsconfig.DBSQLite {
		return nil, errors.New("только SQLite поддерживается в этой сборке")
	}
	filename := cfg.DBServer.FilePath
	if !filepath.IsAbs(filename) {
		filename = filepath.Join(root, filename)
	}
	filename, err := filepath.Abs(filename)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(filename), 0700); err != nil {
		return nil, err
	}
	// SQLite path must be local; do not place WAL database on NFS/SMB.
	db, err := sql.Open("sqlite", filename)
	if err != nil {
		return nil, err
	}
	// Explicitly one SQLite connection: consistent PRAGMA settings and predictable locking.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0) // PRAGMA state is per connection; keep the single SQLite connection.
	db.SetConnMaxIdleTime(0)
	pragmas := []string{
		fmt.Sprintf("PRAGMA busy_timeout=%d", cfg.DBServer.SQLite.BusyTimeoutMs),
		"PRAGMA journal_mode=" + cfg.DBServer.SQLite.JournalMode,
	}
	if cfg.DBServer.SQLite.ForeignKeys {
		pragmas = append(pragmas, "PRAGMA foreign_keys=ON")
	}
	for _, q := range pragmas {
		if _, err := db.ExecContext(ctx, q); err != nil {
			db.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func Init(ctx context.Context, db *sql.DB) error {
	// Statements are deliberately explicit; no automatic schema changes on normal boot.
	for _, stmt := range strings.Split(schema, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("инициализация БД: %w", err)
		}
	}
	if err := telemetry.InitSQLiteSchema(ctx, db); err != nil {
		return err
	}
	return registry.Migrate(ctx, db)
}

func Verify(ctx context.Context, db *sql.DB) error {
	var name string
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='hc_storage_volumes'").Scan(&name); err != nil {
		return errors.New("схема БД не готова: сначала выполните go run . init-db")
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='audit_events'").Scan(&name); err != nil {
		return errors.New("таблица audit_events отсутствует: сначала go run . init-db")
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='hc_devices'").Scan(&name); err != nil {
		return errors.New("таблица hc_devices отсутствует: сначала выполните go run . init-db")
	}
	if err := db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name='hc_metric_samples'").Scan(&name); err != nil {
		return errors.New("таблица hc_metric_samples отсутствует: сначала выполните go run . init-db для миграции истории метрик")
	}
	return nil
}

func Audit(ctx context.Context, db *sql.DB, sub, action, result string) {
	// Audit must never contain credentials or payloads.
	_, _ = db.ExecContext(ctx, "INSERT INTO audit_events(event_time,subject,action,result) VALUES (?,?,?,?)",
		time.Now().UTC().Format(time.RFC3339Nano), sub, action, result)
}
