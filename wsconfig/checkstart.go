package wsconfig

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var wsPathPattern = regexp.MustCompile(`^/[A-Za-z0-9/_-]+$`)

func isLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func privateBindHost(host string) bool {
	if host == "auto_private" || isLoopback(host) {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.IsUnspecified() {
		return false
	}
	_, tailscale, _ := net.ParseCIDR("100.64.0.0/10")
	_, tailscaleV6, _ := net.ParseCIDR("fd7a:115c:a1e0::/48")
	return ip.IsPrivate() || tailscale.Contains(ip) || tailscaleV6.Contains(ip)
}

func goodPath(s string) bool {
	return wsPathPattern.MatchString(s) && s != "/" && !strings.Contains(s, "//")
}

func CheckStart(c *Config, root string) error {
	if c.TypeStart.StartByte == nil {
		return errors.New("type_start.start_byte обязателен")
	}
	if c.TypeStart.DBByte == nil {
		return errors.New("type_start.db_byte обязателен, 0x00 означает SQLite")
	}
	switch *c.TypeStart.DBByte {
	case DBSQLite, DBPostgreSQLLocal, DBPostgreSQLRemote:
		// Database connection providers are initialized separately.
	default:
		return fmt.Errorf("неизвестный db_byte 0x%02X: допустимо 0x00 (SQLite), 0x01 (PostgreSQL Local), 0x02 (PostgreSQL Remote)", *c.TypeStart.DBByte)
	}
	if c.ConfigVersion != 1 {
		return errors.New("config_version: поддерживается только 1")
	}
	if !privateBindHost(c.Server.Host) || strings.TrimSpace(c.Server.Host) != c.Server.Host || strings.ContainsAny(c.Server.Host, " /\t\r\n") || c.Server.Port < 1 || c.Server.Port > 65535 || !goodPath(c.Server.WebSocketPath) {
		return errors.New("server: требуется host, port 1..65535 и корректный websocket_path, например /ws")
	}
	if c.Server.ReadHeaderTimeoutSeconds <= 0 || c.Server.ShutdownTimeoutSeconds <= 0 {
		return errors.New("server: неверные тайм-ауты")
	}
	switch c.Server.TLS.Mode {
	case "disabled":
		// Private-network deployments can explicitly choose unencrypted WS.
		// Do not claim confidentiality on LAN; Tailscale encrypts overlay traffic.
		if !privateBindHost(c.Server.Host) {
			return errors.New("server: без TLS доступны только auto_private или частные IP")
		}
	case "reverse_proxy":
		if !isLoopback(c.Server.Host) {
			return errors.New("reverse_proxy: Go listener должен слушать только localhost")
		}
		if len(c.Server.ReverseProxy.TrustedCIDRs) == 0 {
			return errors.New("reverse_proxy: укажите trusted_cidrs")
		}
		for _, cidr := range c.Server.ReverseProxy.TrustedCIDRs {
			if _, _, err := net.ParseCIDR(cidr); err != nil {
				return fmt.Errorf("trusted_cidrs: %w", err)
			}
		}
	case "native":
		pair := []string{c.Server.TLS.CertFile, c.Server.TLS.KeyFile}
		for i, p := range pair {
			if p == "" {
				return errors.New("native TLS требует cert_file и key_file")
			}
			if !filepath.IsAbs(p) {
				p = filepath.Join(root, p)
			}
			if _, err := os.Stat(p); err != nil {
				return fmt.Errorf("TLS file %s: %w", p, err)
			}
			pair[i] = p
		}
		if _, err := tls.LoadX509KeyPair(pair[0], pair[1]); err != nil {
			return fmt.Errorf("native TLS: сертификат и ключ не образуют корректную пару: %w", err)
		}
	default:
		return fmt.Errorf("server.tls.mode: неизвестное значение %q", c.Server.TLS.Mode)
	}
	// Tokens, access_token roles, key validation and issue-token have been removed.
	// The client-provided device_id is a routing label, NOT authenticated identity.
	if c.Security.RemoteAccess.Enabled {
		return errors.New("remote_access: внешний доступ ещё не реализован")
	}
	if c.WebSocket.ProtocolVersion != 1 || c.WebSocket.Encoding != "json" {
		return errors.New("websocket: поддерживаются protocol_version=1 и encoding=json")
	}
	ws := c.WebSocket
	if ws.MaxConnections <= 0 || ws.MaxConnectionsPerIP <= 0 || ws.MaxMessageBytes < 256 || ws.MaxMessageBytes > 4*1024*1024 ||
		ws.MaxMessagesPerSecond <= 0 || ws.WriteTimeoutSeconds <= 0 || ws.PingIntervalSeconds <= 0 || ws.PongTimeoutSeconds <= 0 ||
		ws.MaxOutboundQueueMessages <= 0 {
		return errors.New("websocket: неверные лимиты/тайм-ауты")
	}
	if ws.HelloTimeoutSeconds <= 0 {
		return errors.New("websocket.hello_timeout_seconds должен быть > 0")
	}
	for _, s := range ws.AllowedOrigins {
		parsed, err := url.Parse(s)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
			return fmt.Errorf("websocket.allowed_origins: неверный origin %q", s)
		}
	}
	if c.Monitoring.Enabled && (!goodPath(c.Monitoring.HealthPath) || c.Monitoring.HealthPath == c.Server.WebSocketPath) {
		return errors.New("monitoring.health_path: некорректный или совпадает с /ws")
	}
	if c.Monitoring.MetricsEndpointEnabled {
		return errors.New("metrics_endpoint_enabled: реализация отсутствует, отключите")
	}
	if c.Monitoring.SampleIntervalSeconds < 1 || c.Monitoring.SampleIntervalSeconds > 60 ||
		c.Monitoring.MetricsRetentionDays < 0 || c.Monitoring.MetricsRetentionDays > 14 {
		return errors.New("monitoring: sample_interval_seconds должен быть в диапазоне 1..60, metrics_retention_days — 0..14")
	}
	if c.MessageBus.EventHistory.Enabled {
		return errors.New("message_bus.event_history: пока не реализовано")
	}
	if c.MessageBus.SlowConsumerPolicy != "disconnect" {
		return errors.New("поддерживается только slow_consumer_policy=disconnect")
	}
	if c.DBServer.ConnectionTimeout <= 0 {
		return errors.New("db_server.connection_timeout должен быть > 0")
	}
	switch *c.TypeStart.DBByte {
	case DBPostgreSQLLocal, DBPostgreSQLRemote:
		d := c.DBServer
		if d.Database == "" || d.Username == "" {
			return errors.New("db_server.database и db_server.username обязательны для PostgreSQL")
		}
		if strings.TrimSpace(d.Database) != d.Database || strings.TrimSpace(d.Username) != d.Username ||
			strings.ContainsAny(d.Database+d.Username+d.Password, "\x00\r\n") {
			return errors.New("db_server: недопустимые символы в параметрах PostgreSQL")
		}
		if d.Port < 0 || d.Port > 65535 {
			return errors.New("db_server.port: допустимо 0 (порт по умолчанию 5432) или 1..65535")
		}
		host := d.Host
		if *c.TypeStart.DBByte == DBPostgreSQLLocal && host == "" {
			host = "127.0.0.1"
		}
		if host == "" || strings.TrimSpace(host) != host || strings.ContainsAny(host, " /\\\t\r\n") {
			return errors.New("db_server.host обязателен и должен содержать только hostname или IP")
		}
		if *c.TypeStart.DBByte == DBPostgreSQLLocal && !isLoopback(host) {
			return errors.New("PostgreSQL Local должен слушать только localhost или loopback IP; для удалённого сервера используйте db_byte 0x02")
		}
	}
	if *c.TypeStart.DBByte == DBSQLite {
		if c.DBServer.FilePath == "" {
			return errors.New("db_server.file_path обязателен для SQLite")
		}
		if c.DBServer.SQLite.BusyTimeoutMs < 0 {
			return errors.New("db_server.sqlite.busy_timeout_ms не может быть отрицательным")
		}
		if c.DBServer.SQLite.JournalMode != "WAL" && c.DBServer.SQLite.JournalMode != "DELETE" {
			return errors.New("SQLite journal_mode: WAL или DELETE")
		}
	}
	if c.Logging.LogSecrets {
		return errors.New("logging.log_secrets: логирование секретов запрещено по соображениям безопасности")
	}
	// WebSocket payload tracing is allowed: traceWSMessage recursively redacts
	// credential-like fields before logging. Raw secrets are never logged.
	if c.Logging.Output != "stdout" {
		return errors.New("logging.output: поддерживается только stdout")
	}
	if c.Logging.Format != "json" && c.Logging.Format != "text" {
		return errors.New("logging.format: json или text")
	}
	if c.Devices.Discovery.MDNSEnabled || c.Devices.Discovery.SSDPEnabled || c.Devices.Discovery.BluetoothEnabled || c.Devices.Discovery.NetworkScanEnabled {
		return errors.New("devices.discovery: механизмы обнаружения ещё не реализованы")
	}
	if c.Agents.Enabled || c.Screens.Enabled || c.Media.Enabled || c.Audio.Enabled || c.Automations.Enabled || c.Backups.Enabled ||
		c.Integrations.MQTT.Enabled || c.Integrations.HomeAssistant.Enabled || c.Integrations.Plex.Enabled || c.Integrations.CUPS.Enabled {
		return errors.New("один из интеграционных модулей включён, но ещё не реализован; отключите enabled")
	}
	if *c.TypeStart.StartByte == StartStrict {
		if err := validateNumericBounds(c); err != nil {
			return err
		}
		if err := validateStrictSettings(c); err != nil {
			return err
		}
		if c.Home.ID == "" || c.Home.Name == "" || c.Home.Locale == "" || c.Home.Timezone == "" {
			return errors.New("home: заполните id, name, locale")
		}
		if _, err := time.LoadLocation(c.Home.Timezone); err != nil {
			return fmt.Errorf("home.timezone: %w", err)
		}
		if c.Devices.Enabled && (c.Devices.HeartbeatIntervalSeconds <= 0 || c.Devices.OfflineAfterSeconds <= c.Devices.HeartbeatIntervalSeconds) {
			return errors.New("devices: offline_after_seconds должен быть больше heartbeat_interval_seconds")
		}
		if c.MessageBus.RequireActionNamespace == false || c.MessageBus.RequireMessageID == false {
			return errors.New("message_bus: включите require_message_id и require_action_namespace")
		}
		// The field is optional in the YAML schema. An absent value has
		// the same meaning as the sole supported backend (database).
		if c.Security.Authorization.PermissionsStorage == "" {
			c.Security.Authorization.PermissionsStorage = "database"
		}
		if c.Security.Authorization.PermissionsStorage != "database" {
			return errors.New("permissions_storage: поддерживается только database")
		}
	}
	return nil
}

// validateStrictSettings checks common constraints even for disabled modules.
// It does not pretend that unimplemented subsystems are operational.
func validateStrictSettings(c *Config) error {
	if c.Home.Units != "metric" && c.Home.Units != "imperial" {
		return errors.New("home.units: metric или imperial")
	}
	ws := c.WebSocket
	if ws.MaxConnectionsPerIP > ws.MaxConnections {
		return errors.New("websocket: лимиты per_ip/per_identity не могут превышать max_connections")
	}
	if ws.PongTimeoutSeconds >= ws.PingIntervalSeconds {
		return errors.New("websocket: pong_timeout_seconds должен быть меньше ping_interval_seconds")
	}
	b := c.MessageBus
	if b.CommandAckTimeoutSeconds <= 0 || b.DeduplicationWindowSeconds < 0 || b.MaxPendingCommandsPerClient <= 0 {
		return errors.New("message_bus: неверные интервалы или лимиты")
	}
	if b.EventHistory.RetentionDays < 0 {
		return errors.New("message_bus.event_history.retention_days не может быть отрицательным")
	}
	// Legacy auth fields are deliberately ignored; no secret is required.
	if c.Security.Pairing.PairingWindowSeconds <= 0 || c.Security.Pairing.MaxAttempts <= 0 {
		return errors.New("security.pairing: неверные интервалы")
	}
	if c.Security.CriticalActions.ConfirmationTimeoutSeconds <= 0 {
		return errors.New("security.critical_actions.confirmation_timeout_seconds должен быть >0")
	}
	d := c.DBServer
	if d.MaxOpenConnections < 1 || d.MaxIdleConnections < 0 || d.MaxIdleConnections > d.MaxOpenConnections || d.ConnectionMaxLifetimeSeconds < 0 || d.ConnectionMaxIdleSeconds < 0 {
		return errors.New("db_server: неверные параметры пула соединений")
	}
	if *c.TypeStart.DBByte == DBSQLite {
		if d.TLSEnabled {
			return errors.New("db_server.tls_enabled: не применяется к локальному SQLite")
		}
		if !d.SQLite.ForeignKeys {
			return errors.New("db_server.sqlite.foreign_keys должен быть true")
		}
	}
	if c.Devices.HeartbeatIntervalSeconds <= 0 || c.Devices.OfflineAfterSeconds <= c.Devices.HeartbeatIntervalSeconds {
		return errors.New("devices: неверные параметры heartbeat/offline")
	}
	if c.Agents.HeartbeatIntervalSeconds < 0 || c.Agents.OfflineAfterSeconds < 0 {
		return errors.New("agents: неверные интервалы")
	}
	if c.Screens.DefaultContentTimeoutSeconds <= 0 {
		return errors.New("screens.default_content_timeout_seconds должен быть >0")
	}
	if !c.Media.ControlOnly {
		return errors.New("media.control_only должен быть true: потоковая передача через WebSocket здесь не реализована")
	}
	if c.Audio.DefaultVolumePercent < 0 || c.Audio.DefaultVolumePercent > 100 || c.Audio.MaxVolumePercent < 0 || c.Audio.MaxVolumePercent > 100 || c.Audio.DefaultVolumePercent > c.Audio.MaxVolumePercent {
		return errors.New("audio: громкость должна быть в диапазоне 0..100")
	}
	if c.Storage.Enabled {
		if len(c.Storage.AllowedRoots) == 0 || c.Storage.MaxUploadSizeMB <= 0 {
			return errors.New("storage: allowed_roots и max_upload_size_mb обязательны")
		}
		if c.Storage.TransferTransport != "websocket" || !c.Storage.WebSocketBinaryFileTransfer {
			return errors.New("storage: для v1 укажите transfer_transport: websocket и websocket_binary_file_transfer: true")
		}
	}
	if c.Storage.MaxUploadSizeMB < 0 {
		return errors.New("storage.max_upload_size_mb не может быть отрицательным")
	}
	if c.Integrations.MQTT.QoS < 0 || c.Integrations.MQTT.QoS > 2 {
		return errors.New("integrations.mqtt.qos: диапазон 0..2")
	}
	if c.Notifications.DeduplicationWindowSeconds < 0 {
		return errors.New("notifications.deduplication_window_seconds не может быть отрицательным")
	}
	switch c.Logging.Level {
	case "debug", "info", "warn", "error":
	default:
		return errors.New("logging.level: debug|info|warn|error")
	}
	if c.Audit.RetentionDays < 1 {
		return errors.New("audit.retention_days должен быть >0")
	}
	if c.Backups.KeepLast < 0 {
		return errors.New("backups.keep_last не может быть отрицательным")
	}
	return nil
}
