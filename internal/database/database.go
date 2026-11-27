package database

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"net"
	"net/url"
	"strings"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/physicaldevices"
	"HomeCloudBackend/internal/telemetry"
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
 size_bytes BIGINT NOT NULL,
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
 size_bytes BIGINT NOT NULL,
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
    first_seen_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
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
	if cfg == nil || cfg.TypeStart.DBByte == nil {
		return nil, errors.New("тип базы данных не задан")
	}
	switch *cfg.TypeStart.DBByte {
	case wsconfig.DBSQLite:
		return openSQLite(ctx, cfg, root)
	case wsconfig.DBPostgreSQLLocal, wsconfig.DBPostgreSQLRemote:
		return openPostgreSQL(ctx, cfg)
	default:
		return nil, fmt.Errorf("неподдерживаемый db_byte 0x%02X", *cfg.TypeStart.DBByte)
	}
}

func openSQLite(ctx context.Context, cfg *wsconfig.Config, root string) (*sql.DB, error) {
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
	db.SetConnMaxLifetime(0)
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
			_ = db.Close()
			return nil, fmt.Errorf("%s: %w", q, err)
		}
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func openPostgreSQL(ctx context.Context, cfg *wsconfig.Config) (*sql.DB, error) {
	d := cfg.DBServer
	host := d.Host
	if host == "" {
		host = "127.0.0.1"
	}
	port := d.Port
	if port == 0 {
		port = 5432
	}
	sslMode := "disable"
	if d.TLSEnabled {
		sslMode = "verify-full"
	}
	u := &url.URL{
		Scheme: "postgres",
		User: url.UserPassword(d.Username, d.Password),
		Host: net.JoinHostPort(host, fmt.Sprint(port)),
		Path: "/" + d.Database,
	}
	query := url.Values{}
	query.Set("sslmode", sslMode)
	query.Set("connect_timeout", fmt.Sprint(d.ConnectionTimeout))
	u.RawQuery = query.Encode()

	db, err := sql.Open("postgres-rebind", u.String())
	if err != nil {
		return nil, fmt.Errorf("открытие PostgreSQL driver: %w", err)
	}
	maxOpen := d.MaxOpenConnections
	if maxOpen <= 0 {
		maxOpen = 10
	}
	maxIdle := d.MaxIdleConnections
	if maxIdle < 0 {
		maxIdle = 0
	}
	if maxIdle > maxOpen {
		maxIdle = maxOpen
	}
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	if d.ConnectionMaxLifetimeSeconds > 0 {
		db.SetConnMaxLifetime(time.Duration(d.ConnectionMaxLifetimeSeconds) * time.Second)
	}
	if d.ConnectionMaxIdleSeconds > 0 {
		db.SetConnMaxIdleTime(time.Duration(d.ConnectionMaxIdleSeconds) * time.Second)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("проверка подключения PostgreSQL: %w", err)
	}
	return db, nil
}

func isPostgreSQL(db *sql.DB) bool {
	_, ok := db.Driver().(*rebindDriver)
	return ok
}
func Init(ctx context.Context, db *sql.DB) error {
	// Statements are deliberately explicit; no automatic schema changes on normal boot.
	ddl := schema
	if isPostgreSQL(db) {
		ddl = strings.Replace(ddl, "id INTEGER PRIMARY KEY AUTOINCREMENT", "id BIGSERIAL PRIMARY KEY", 1)
	}
	for _, stmt := range strings.Split(ddl, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("инициализация БД: %w", err)
		}
	}
	if err := registry.Migrate(ctx, db); err != nil {
		return err
	}
	if err := physicaldevices.Migrate(ctx, db); err != nil {
		return err
	}
	return telemetry.InitSchema(ctx, db)
}

func Verify(ctx context.Context, db *sql.DB) error {
	var name string
	tableExists := func(table string) error {
		if isPostgreSQL(db) {
			return db.QueryRowContext(ctx, "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_name=?", table).Scan(&name)
		}
		return db.QueryRowContext(ctx, "SELECT name FROM sqlite_master WHERE type='table' AND name=?", table).Scan(&name)
	}
	if err := tableExists("hc_storage_volumes"); err != nil {
		return errors.New("схема БД не готова: сначала выполните go run . init-db")
	}
	if err := tableExists("audit_events"); err != nil {
		return errors.New("таблица audit_events отсутствует: сначала go run . init-db")
	}
	if err := tableExists("hc_devices"); err != nil {
		return errors.New("таблица hc_devices отсутствует: сначала выполните go run . init-db")
	}
	if err := registry.VerifySchema(ctx, db); err != nil {
		return err
	}
	if err := physicaldevices.VerifySchema(ctx, db); err != nil {
		return err
	}
	if err := tableExists("hc_telemetry_samples"); err != nil {
		return errors.New("таблица hc_telemetry_samples отсутствует: сначала выполните go run . init-db для миграции истории метрик")
	}
	return nil
}

func Audit(ctx context.Context, db *sql.DB, sub, action, result string) {
	// Audit must never contain credentials or payloads.
	_, _ = db.ExecContext(ctx, "INSERT INTO audit_events(event_time,subject,action,result) VALUES (?,?,?,?)",
		time.Now().UTC().Format(time.RFC3339Nano), sub, action, result)
}
