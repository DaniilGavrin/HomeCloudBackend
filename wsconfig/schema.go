package wsconfig

// Code organized as strongly-typed YAML configuration schema.
// All fields in config.yaml are declared here.

type Config struct {
	ConfigVersion int                 `yaml:"config_version"`
	Home          HomeConfig          `yaml:"home"`
	Server        ServerConfig        `yaml:"server"`
	WebSocket     WebSocketConfig     `yaml:"websocket"`
	MessageBus    MessageBusConfig    `yaml:"message_bus"`
	Security      SecurityConfig      `yaml:"security"`
	DBServer      DBServerConfig      `yaml:"db_server"`
	Devices       DevicesConfig       `yaml:"devices"`
	Agents        AgentsConfig        `yaml:"agents"`
	Screens       ScreensConfig       `yaml:"screens"`
	Media         MediaConfig         `yaml:"media"`
	MediaGateway  MediaGatewayConfig  `yaml:"media_gateway"`
	Audio         AudioConfig         `yaml:"audio"`
	Storage       StorageConfig       `yaml:"storage"`
	Integrations  IntegrationsConfig  `yaml:"integrations"`
	Automations   AutomationsConfig   `yaml:"automations"`
	Notifications NotificationsConfig `yaml:"notifications"`
	Monitoring    MonitoringConfig    `yaml:"monitoring"`
	Logging       LoggingConfig       `yaml:"logging"`
	Audit         AuditConfig         `yaml:"audit"`
	Backups       BackupsConfig       `yaml:"backups"`
	TypeStart     TypeStartConfig     `yaml:"type_start"`
}

type HomeConfig struct {
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Timezone string `yaml:"timezone"`
	Locale   string `yaml:"locale"`
	Units    string `yaml:"units"`
}

type ServerConfig struct {
	Host                     string                   `yaml:"host"`
	Port                     int                      `yaml:"port"`
	WebSocketPath            string                   `yaml:"websocket_path"`
	ReadHeaderTimeoutSeconds int                      `yaml:"read_header_timeout_seconds"`
	ShutdownTimeoutSeconds   int                      `yaml:"shutdown_timeout_seconds"`
	TLS                      ServerTLSConfig          `yaml:"tls"`
	ReverseProxy             ServerReverseProxyConfig `yaml:"reverse_proxy"`
}

type ServerTLSConfig struct {
	Mode     string `yaml:"mode"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

type ServerReverseProxyConfig struct {
	TrustedCIDRs []string `yaml:"trusted_cidrs"`
}

type WebSocketConfig struct {
	ProtocolVersion          int      `yaml:"protocol_version"`
	Encoding                 string   `yaml:"encoding"`
	AllowedOrigins           []string `yaml:"allowed_origins"`
	MaxConnections           int      `yaml:"max_connections"`
	MaxConnectionsPerIP      int      `yaml:"max_connections_per_ip"`
	MaxMessageBytes          int      `yaml:"max_message_bytes"`
	MaxMessagesPerSecond     int      `yaml:"max_messages_per_second"`
	MaxOutboundQueueMessages int      `yaml:"max_outbound_queue_messages"`
	PingIntervalSeconds      int      `yaml:"ping_interval_seconds"`
	PongTimeoutSeconds       int      `yaml:"pong_timeout_seconds"`
	WriteTimeoutSeconds      int      `yaml:"write_timeout_seconds"`
	HelloTimeoutSeconds      int      `yaml:"hello_timeout_seconds"`
	CompressionEnabled       bool     `yaml:"compression_enabled"`
}

type MessageBusConfig struct {
	RequireMessageID            bool                         `yaml:"require_message_id"`
	RequireActionNamespace      bool                         `yaml:"require_action_namespace"`
	RequireSubscription         bool                         `yaml:"require_subscription"`
	SendInitialSnapshot         bool                         `yaml:"send_initial_snapshot"`
	PublishStateDeltas          bool                         `yaml:"publish_state_deltas"`
	CommandAckTimeoutSeconds    int                          `yaml:"command_ack_timeout_seconds"`
	DeduplicationWindowSeconds  int                          `yaml:"deduplication_window_seconds"`
	MaxPendingCommandsPerClient int                          `yaml:"max_pending_commands_per_client"`
	SlowConsumerPolicy          string                       `yaml:"slow_consumer_policy"`
	EventHistory                MessageBusEventHistoryConfig `yaml:"event_history"`
}

type MessageBusEventHistoryConfig struct {
	Enabled       bool `yaml:"enabled"`
	RetentionDays int  `yaml:"retention_days"`
}

type SecurityConfig struct {
	Authorization   SecurityAuthorizationConfig   `yaml:"authorization"`
	Pairing         SecurityPairingConfig         `yaml:"pairing"`
	CriticalActions SecurityCriticalActionsConfig `yaml:"critical_actions"`
	RemoteAccess    SecurityRemoteAccessConfig    `yaml:"remote_access"`
}

type SecurityAuthorizationConfig struct {
	DefaultPolicy      string   `yaml:"default_policy"`
	CheckEveryCommand  bool     `yaml:"check_every_command"`
	Roles              []string `yaml:"roles"`
	PermissionsStorage string   `yaml:"permissions_storage"`
}

type SecurityPairingConfig struct {
	RequireApproval      bool `yaml:"require_approval"`
	PairingWindowSeconds int  `yaml:"pairing_window_seconds"`
	MaxAttempts          int  `yaml:"max_attempts"`
}

type SecurityCriticalActionsConfig struct {
	RequireConfirmation        bool     `yaml:"require_confirmation"`
	ConfirmationTimeoutSeconds int      `yaml:"confirmation_timeout_seconds"`
	Actions                    []string `yaml:"actions"`
}

type SecurityRemoteAccessConfig struct {
	Enabled                 bool `yaml:"enabled"`
	RequireTLS              bool `yaml:"require_tls"`
	AutomaticPortForwarding bool `yaml:"automatic_port_forwarding"`
}

type DBServerConfig struct {
	Host                         string               `yaml:"host"`
	Port                         int                  `yaml:"port"`
	Database                     string               `yaml:"database"`
	Username                     string               `yaml:"username"`
	Password                     string               `yaml:"password"`
	FilePath                     string               `yaml:"file_path"`
	TLSEnabled                   bool                 `yaml:"tls_enabled"`
	ConnectionTimeout            int                  `yaml:"connection_timeout"`
	MaxOpenConnections           int                  `yaml:"max_open_connections"`
	MaxIdleConnections           int                  `yaml:"max_idle_connections"`
	ConnectionMaxLifetimeSeconds int                  `yaml:"connection_max_lifetime_seconds"`
	ConnectionMaxIdleSeconds     int                  `yaml:"connection_max_idle_seconds"`
	AutoMigrate                  bool                 `yaml:"auto_migrate"`
	SQLite                       DBServerSQLiteConfig `yaml:"sqlite"`
}

type DBServerSQLiteConfig struct {
	JournalMode   string `yaml:"journal_mode"`
	BusyTimeoutMs int    `yaml:"busy_timeout_ms"`
	ForeignKeys   bool   `yaml:"foreign_keys"`
}

type DevicesConfig struct {
	Enabled                  bool                   `yaml:"enabled"`
	RequirePairing           bool                   `yaml:"require_pairing"`
	AutomaticRegistration    bool                   `yaml:"automatic_registration"`
	HeartbeatIntervalSeconds int                    `yaml:"heartbeat_interval_seconds"`
	OfflineAfterSeconds      int                    `yaml:"offline_after_seconds"`
	PreserveLastKnownState   bool                   `yaml:"preserve_last_known_state"`
	Discovery                DevicesDiscoveryConfig `yaml:"discovery"`
}

type DevicesDiscoveryConfig struct {
	MDNSEnabled        bool `yaml:"mdns_enabled"`
	SSDPEnabled        bool `yaml:"ssdp_enabled"`
	BluetoothEnabled   bool `yaml:"bluetooth_enabled"`
	NetworkScanEnabled bool `yaml:"network_scan_enabled"`
}

type AgentsConfig struct {
	Enabled                  bool     `yaml:"enabled"`
	Transport                string   `yaml:"transport"`
	ApprovalRequired         bool     `yaml:"approval_required"`
	HeartbeatIntervalSeconds int      `yaml:"heartbeat_interval_seconds"`
	OfflineAfterSeconds      int      `yaml:"offline_after_seconds"`
	CollectMetrics           []string `yaml:"collect_metrics"`
	AllowRemoteShell         bool     `yaml:"allow_remote_shell"`
	AllowRemoteReboot        bool     `yaml:"allow_remote_reboot"`
	AllowServiceControl      bool     `yaml:"allow_service_control"`
}

type ScreensConfig struct {
	Enabled                      bool     `yaml:"enabled"`
	AllowRemoteContentPush       bool     `yaml:"allow_remote_content_push"`
	AllowRemoteURLOpen           bool     `yaml:"allow_remote_url_open"`
	AllowedURLHosts              []string `yaml:"allowed_url_hosts"`
	DefaultContentTimeoutSeconds int      `yaml:"default_content_timeout_seconds"`
}

type MediaConfig struct {
	Enabled                bool                `yaml:"enabled"`
	ControlOnly            bool                `yaml:"control_only"`
	AllowExternalMediaUrls bool                `yaml:"allow_external_media_urls"`
	AllowedMediaHosts      []string            `yaml:"allowed_media_hosts"`
	Adapters               MediaAdaptersConfig `yaml:"adapters"`
}

type MediaAdaptersConfig struct {
	DlnaEnabled            bool `yaml:"dlna_enabled"`
	CastEnabled            bool `yaml:"cast_enabled"`
	AndroidTvEnabled       bool `yaml:"android_tv_enabled"`
	WebrtcSignalingEnabled bool `yaml:"webrtc_signaling_enabled"`
}

type AudioConfig struct {
	Enabled                  bool   `yaml:"enabled"`
	DefaultVolumePercent     int    `yaml:"default_volume_percent"`
	MaxVolumePercent         int    `yaml:"max_volume_percent"`
	MultiroomEnabled         bool   `yaml:"multiroom_enabled"`
	MultiroomSyncMode        string `yaml:"multiroom_sync_mode"`
	MicrophoneCaptureEnabled bool   `yaml:"microphone_capture_enabled"`
}

type StorageConfig struct {
	Enabled                       bool     `yaml:"enabled"`
	DataDir                       string   `yaml:"data_dir"`
	TempDir                       string   `yaml:"temp_dir"`
	FilesDir                      string   `yaml:"files_dir"`
	AllowedRoots                  []string `yaml:"allowed_roots"`
	AllowArbitraryFilesystemPaths bool     `yaml:"allow_arbitrary_filesystem_paths"`
	MaxUploadSizeMB               int      `yaml:"max_upload_size_mb"`
	TransferTransport             string   `yaml:"transfer_transport"`
	WebSocketBinaryFileTransfer   bool     `yaml:"websocket_binary_file_transfer"`
	ExternalMounts                []string `yaml:"external_mounts"`
}

type IntegrationsConfig struct {
	MQTT          IntegrationsMQTTConfig          `yaml:"mqtt"`
	HomeAssistant IntegrationsHomeAssistantConfig `yaml:"home_assistant"`
	Plex          IntegrationsPlexConfig          `yaml:"plex"`
	CUPS          IntegrationsCUPSConfig          `yaml:"cups"`
}

type IntegrationsMQTTConfig struct {
	Enabled     bool   `yaml:"enabled"`
	Host        string `yaml:"host"`
	Port        int    `yaml:"port"`
	TLSEnabled  bool   `yaml:"tls_enabled"`
	ClientID    string `yaml:"client_id"`
	Username    string `yaml:"username"`
	Password    string `yaml:"password"`
	BaseTopic   string `yaml:"base_topic"`
	QoS         int    `yaml:"qos"`
	RetainState bool   `yaml:"retain_state"`
}

type IntegrationsHomeAssistantConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
	Token   string `yaml:"token"`
}

type IntegrationsPlexConfig struct {
	Enabled bool   `yaml:"enabled"`
	URL     string `yaml:"url"`
	Token   string `yaml:"token"`
}

type IntegrationsCUPSConfig struct {
	Enabled         bool   `yaml:"enabled"`
	URL             string `yaml:"url"`
	AllowSubmitJobs bool   `yaml:"allow_submit_jobs"`
}

type AutomationsConfig struct {
	Enabled                     bool `yaml:"enabled"`
	MaxConcurrentActions        int  `yaml:"max_concurrent_actions"`
	DefaultActionTimeoutSeconds int  `yaml:"default_action_timeout_seconds"`
	PreventDuplicateCommands    bool `yaml:"prevent_duplicate_commands"`
	AllowCriticalActions        bool `yaml:"allow_critical_actions"`
	PersistSchedules            bool `yaml:"persist_schedules"`
}

type NotificationsConfig struct {
	Enabled                    bool     `yaml:"enabled"`
	WebSocketEnabled           bool     `yaml:"websocket_enabled"`
	DeduplicationWindowSeconds int      `yaml:"deduplication_window_seconds"`
	NotifyOn                   []string `yaml:"notify_on"`
}

type MonitoringConfig struct {
	Enabled                bool   `yaml:"enabled"`
	HealthPath             string `yaml:"health_path"`
	MetricsEndpointEnabled bool   `yaml:"metrics_endpoint_enabled"`
	MetricsPath            string `yaml:"metrics_path"`
	SampleIntervalSeconds  int    `yaml:"sample_interval_seconds"`
	MetricsRetentionDays   int    `yaml:"metrics_retention_days"`
}

type LoggingConfig struct {
	Level              string `yaml:"level"`
	Format             string `yaml:"format"`
	Output             string `yaml:"output"`
	LogMessagePayloads bool   `yaml:"log_message_payloads"`
	LogSecrets         bool   `yaml:"log_secrets"`
}

type AuditConfig struct {
	Enabled              bool `yaml:"enabled"`
	RetentionDays        int  `yaml:"retention_days"`
	LogAuthEvents        bool `yaml:"log_auth_events"`
	LogPermissionChanges bool `yaml:"log_permission_changes"`
	LogCriticalCommands  bool `yaml:"log_critical_commands"`
}

type BackupsConfig struct {
	Enabled              bool   `yaml:"enabled"`
	DestinationDir       string `yaml:"destination_dir"`
	Schedule             string `yaml:"schedule"`
	KeepLast             int    `yaml:"keep_last"`
	IncludeDatabase      bool   `yaml:"include_database"`
	IncludeConfiguration bool   `yaml:"include_configuration"`
	IncludeMediaFiles    bool   `yaml:"include_media_files"`
	EncryptionEnabled    bool   `yaml:"encryption_enabled"`
	EncryptionKey        string `yaml:"encryption_key"`
}

type TypeStartConfig struct {
	StartByte *uint8 `yaml:"start_byte"`
	DBByte    *uint8 `yaml:"db_byte"`
}
