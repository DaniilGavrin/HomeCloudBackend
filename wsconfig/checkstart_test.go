package wsconfig

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func validConfig(t *testing.T) *Config {
	t.Helper()
	start, db := uint8(StartStrict), uint8(DBSQLite)
	return &Config{
		ConfigVersion: 1,
		Home:          HomeConfig{ID: "test-home", Name: "Test", Locale: "ru-RU", Timezone: "Europe/Moscow", Units: "metric"},
		Server:        ServerConfig{Host: "127.0.0.1", Port: 8080, WebSocketPath: "/ws", ReadHeaderTimeoutSeconds: 5, ShutdownTimeoutSeconds: 20, TLS: ServerTLSConfig{Mode: "disabled"}},
		WebSocket:     WebSocketConfig{ProtocolVersion: 1, Encoding: "json", MaxConnections: 100, MaxConnectionsPerIP: 10, MaxMessageBytes: 1024, MaxMessagesPerSecond: 20, MaxOutboundQueueMessages: 10, HelloTimeoutSeconds: 10, WriteTimeoutSeconds: 5, PingIntervalSeconds: 30, PongTimeoutSeconds: 5},
		Security: SecurityConfig{
			Pairing:         SecurityPairingConfig{PairingWindowSeconds: 120, MaxAttempts: 5},
			CriticalActions: SecurityCriticalActionsConfig{ConfirmationTimeoutSeconds: 30},
		},
		DBServer:   DBServerConfig{FilePath: "./data/test.db", ConnectionTimeout: 10, MaxOpenConnections: 10, MaxIdleConnections: 5, SQLite: DBServerSQLiteConfig{JournalMode: "WAL", BusyTimeoutMs: 5000, ForeignKeys: true}},
		MessageBus: MessageBusConfig{SlowConsumerPolicy: "disconnect", RequireMessageID: true, RequireActionNamespace: true, CommandAckTimeoutSeconds: 10, MaxPendingCommandsPerClient: 32},
		Devices:    DevicesConfig{HeartbeatIntervalSeconds: 30, OfflineAfterSeconds: 90},
		Screens:    ScreensConfig{DefaultContentTimeoutSeconds: 300},
		Media:      MediaConfig{ControlOnly: true},
		Monitoring: MonitoringConfig{SampleIntervalSeconds: 30},
		Audit:      AuditConfig{RetentionDays: 90},
		Logging:    LoggingConfig{Format: "json", Output: "stdout", Level: "info"},
		TypeStart:  TypeStartConfig{StartByte: &start, DBByte: &db},
	}
}
func TestStrictValid(t *testing.T) {
	t.Run("Legacy / TestStrictValid", func(t *testing.T) {
		c := validConfig(t)
		if err := CheckStart(c, t.TempDir()); err != nil {
			t.Fatal(err)
		}

	})
}
func TestRejectEmptyDBByte(t *testing.T) {
	t.Run("Legacy / TestRejectEmptyDBByte", func(t *testing.T) {
		c := validConfig(t)
		c.TypeStart.DBByte = nil
		if err := CheckStart(c, t.TempDir()); err == nil {
			t.Fatal("missing db_byte must fail")
		}

	})
}
func TestRejectUnsafeBind(t *testing.T) {
	t.Run("Legacy / TestRejectUnsafeBind", func(t *testing.T) {
		c := validConfig(t)
		c.Server.Host = "0.0.0.0"
		if err := CheckStart(c, t.TempDir()); err == nil {
			t.Fatal("non-TLS public bind must fail")
		}

	})
}
func TestRejectInvalidWSPath(t *testing.T) {
	t.Run("Legacy / TestRejectInvalidWSPath", func(t *testing.T) {
		c := validConfig(t)
		c.Server.WebSocketPath = ""
		if err := CheckStart(c, t.TempDir()); err == nil {
			t.Fatal("empty ws path must fail")
		}

	})
}
func TestCreateTemplate(t *testing.T) {
	t.Run("Legacy / TestCreateTemplate", func(t *testing.T) {
		dir := t.TempDir()
		_, err := LoadAt(dir)
		if !errors.Is(err, ErrCreated) {
			t.Fatalf("expected ErrCreated, got %v", err)
		}
		content, err := os.ReadFile(filepath.Join(dir, "config", "config.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(strings.ReplaceAll(string(content), "\r\n", "\n"), "  start_byte:\n") {
			t.Fatal("template must require mode setup")
		}
		if !strings.Contains(strings.ReplaceAll(string(content), "\r\n", "\n"), "  db_byte:\n") {
			t.Fatal("template must require database setup")
		}

	})
}

func TestNoAuthPrivateInterfaceMode(t *testing.T) {
	c := validConfig(t)
	c.Server.Host = "auto_private"
	if err := CheckStart(c, t.TempDir()); err != nil {
		t.Fatalf("auto_private without authentication rejected: %v", err)
	}
	c.Server.Host = "8.8.8.8"
	if err := CheckStart(c, t.TempDir()); err == nil {
		t.Fatal("public IP must not be accepted")
	}
}

// The optional permissions_storage field defaults to the only supported
// implementation, while unsupported backends must still be rejected.
func TestPermissionsStorageOptionalAndValidated(t *testing.T) {
	config := validConfig(t)
	if err := CheckStart(config, t.TempDir()); err != nil {
		t.Fatalf("optional permissions_storage rejected: %v", err)
	}
	if got := config.Security.Authorization.PermissionsStorage; got != "database" {
		t.Fatalf("default storage backend = %q, want database", got)
	}
	config.Security.Authorization.PermissionsStorage = "filesystem"
	if err := CheckStart(config, t.TempDir()); err == nil {
		t.Fatal("unsupported permissions backend accepted")
	}
}

func TestMonitoringBoundsApplyInFastAndStrictModes(t *testing.T) {
	for _, mode := range []uint8{StartFast, StartStrict} {
		t.Run(fmt.Sprintf("mode_%02x", mode), func(t *testing.T) {
			cfg := validConfig(t)
			cfg.TypeStart.StartByte = &mode
			cfg.Monitoring.SampleIntervalSeconds = 61
			if err := CheckStart(cfg, t.TempDir()); err == nil {
				t.Fatal("sample interval above 60 seconds was accepted")
			}
			cfg = validConfig(t)
			cfg.TypeStart.StartByte = &mode
			cfg.Monitoring.MetricsRetentionDays = 15
			if err := CheckStart(cfg, t.TempDir()); err == nil {
				t.Fatal("retention above 14 days was accepted")
			}
		})
	}
}

func TestPostgreSQLModesDoNotRequireSQLiteSettings(t *testing.T) {
	for _, mode := range []uint8{DBPostgreSQLLocal, DBPostgreSQLRemote} {
		t.Run(fmt.Sprintf("db_byte_%02x", mode), func(t *testing.T) {
			cfg := validConfig(t)
			cfg.TypeStart.DBByte = &mode
			cfg.DBServer.FilePath = ""
			cfg.DBServer.Database = "homecloud"
			cfg.DBServer.Username = "homecloud"
			if mode == DBPostgreSQLLocal {
				cfg.DBServer.Host = "127.0.0.1"
			} else {
				cfg.DBServer.Host = "db.example.internal"
			}
			cfg.DBServer.SQLite.JournalMode = "not-a-sqlite-mode"
			cfg.DBServer.SQLite.BusyTimeoutMs = -1
			cfg.DBServer.SQLite.ForeignKeys = false
			cfg.DBServer.TLSEnabled = true

			if err := CheckStart(cfg, t.TempDir()); err != nil {
				t.Fatalf("PostgreSQL mode must not be rejected by SQLite-only settings: %v", err)
			}
		})
	}
}

func TestUnknownDatabaseByteRejected(t *testing.T) {
	cfg := validConfig(t)
	unknown := uint8(0x03)
	cfg.TypeStart.DBByte = &unknown
	if err := CheckStart(cfg, t.TempDir()); err == nil {
		t.Fatal("unknown database byte must be rejected")
	}
}

func TestPostgreSQLLocalRejectsRemoteHost(t *testing.T) {
	cfg := validConfig(t)
	mode := uint8(DBPostgreSQLLocal)
	cfg.TypeStart.DBByte = &mode
	cfg.DBServer.Database = "homecloud"
	cfg.DBServer.Username = "homecloud"
	cfg.DBServer.Host = "192.168.1.20"
	if err := CheckStart(cfg, t.TempDir()); err == nil {
		t.Fatal("PostgreSQL Local must reject a non-loopback host")
	}
}

func TestPostgreSQLRemoteRequiresConnectionTarget(t *testing.T) {
	cfg := validConfig(t)
	mode := uint8(DBPostgreSQLRemote)
	cfg.TypeStart.DBByte = &mode
	cfg.DBServer.Database = "homecloud"
	cfg.DBServer.Username = "homecloud"
	if err := CheckStart(cfg, t.TempDir()); err == nil {
		t.Fatal("PostgreSQL Remote must require a host")
	}
	cfg.DBServer.Host = "db.example.internal"
	if err := CheckStart(cfg, t.TempDir()); err != nil {
		t.Fatalf("valid remote PostgreSQL settings rejected: %v", err)
	}
}
