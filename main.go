package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"HomeCloudBackend/internal/cluster/consensus"
	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/registry"
	clusterruntime "HomeCloudBackend/internal/cluster/runtime"
	"HomeCloudBackend/internal/database"
	"HomeCloudBackend/internal/mediacompanion"
	"HomeCloudBackend/internal/netbind"
	"HomeCloudBackend/internal/storage"
	"HomeCloudBackend/internal/telemetry"
	"HomeCloudBackend/internal/wsserver"
	"HomeCloudBackend/wsconfig"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "[FATAL]", err)
		os.Exit(1)
	}
}
func run() error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	cfg, err := wsconfig.LoadAt(root)
	if errors.Is(err, wsconfig.ErrCreated) {
		fmt.Println("[CONFIG] Создан config/config.yaml — заполните конфигурацию и перезапустите сервер.")
		fmt.Println("[CONFIG] Обязательно укажите start_byte (0xA1 / 0xA2) и db_byte (0x00 для SQLite).")
		return nil // deliberate normal exit; no WS listener started
	}
	if err != nil {
		return fmt.Errorf("конфигурация: %w", err)
	}
	if cfg.TypeStart.StartByte != nil {
		fmt.Printf("[CONFIG] Mode: 0x%02X\n", *cfg.TypeStart.StartByte)
	}
	lvl := slog.LevelInfo
	switch cfg.Logging.Level {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: lvl}
	var logger *slog.Logger
	if cfg.Logging.Format == "json" {
		logger = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	} else {
		logger = slog.New(slog.NewTextHandler(os.Stdout, opts))
	}
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	dbCtx, cancel := context.WithTimeout(ctx, time.Duration(cfg.DBServer.ConnectionTimeout)*time.Second)
	db, err := database.Open(dbCtx, cfg, root)
	cancel()
	if err != nil {
		return fmt.Errorf("подключение SQLite: %w", err)
	}
	defer db.Close()
	if command == "init-db" {
		if err := database.Init(ctx, db); err != nil {
			return err
		}
		logger.Info("database schema initialized")
		return nil
	}
	if cfg.DBServer.AutoMigrate {
		if err := database.Init(ctx, db); err != nil {
			return err
		}
	} else if err := database.Verify(ctx, db); err != nil {
		return err
	}
	if command == "issue-token" {
		return errors.New("issue-token удалён: соединения теперь используют первое сообщение hello с device_id")
	}
	if command != "serve" {
		return fmt.Errorf("неизвестная команда %q (serve | init-db)", command)
	}
	// KAN-5: one persistent identity per HomeCloud Backend instance.
	// This is a preferred/bootstrap role, NOT a Raft leader election.
	identityStore := identity.NewStore(filepath.Join(root, "data", "cluster"))
	nodeIdentity, err := identityStore.LoadOrCreate()
	if err != nil {
		return fmt.Errorf("node identity: %w", err)
	}
	logger.Info("HomeCloud node identity loaded", "node_id", nodeIdentity.NodeID, "preferred_role", nodeIdentity.PreferredRole)
	// KAN-7 developer-only loopback Raft. Disabled by default; KAN-8 adds authenticated transport.
	if devBind := os.Getenv("HOMECLOUD_RAFT_DEV_BIND"); devBind != "" {
		consensusService, openErr := consensus.Open(consensus.Options{
			NodeID:    nodeIdentity.NodeID,
			BindAddr:  devBind,
			DataDir:   filepath.Join(root, "data", "cluster", "raft"),
			Bootstrap: os.Getenv("HOMECLOUD_RAFT_DEV_BOOTSTRAP") == "1",
		}, registry.NewStore(db))
		if openErr != nil {
			return fmt.Errorf("local raft developer startup: %w", openErr)
		}
		defer consensusService.Close()
		logger.Info("KAN-7 local Raft developer node started", "node_id", nodeIdentity.NodeID, "address", consensusService.Address())
	}
	logger.Warn("No application authentication: allow network access only to trusted LAN and restricted tailnet clients")
	var storageManager *storage.Manager
	if cfg.Storage.Enabled {
		storageManager, err = storage.New(cfg.Storage, db, root)
		if err != nil {
			return fmt.Errorf("storage initialization: %w", err)
		}
		// Explicit schema migration is required when auto_migrate=false.
		for _, table := range []string{"hc_storage_volumes", "hc_storage_index"} {
			if _, err = db.ExecContext(ctx, "SELECT 1 FROM "+table+" LIMIT 1"); err != nil {
				return fmt.Errorf("storage schema missing (%s): run go run . init-db: %w", table, err)
			}
		}
	}
	ws := wsserver.New(cfg, db, logger, storageManager)
	ws.SetLocalNodeID(nodeIdentity.NodeID)

	var telemetryCore *telemetry.Core
	if cfg.Monitoring.Enabled {
		retentionDays := cfg.Monitoring.MetricsRetentionDays
		if retentionDays <= 0 {
			retentionDays = 14
		}
		history, historyErr := telemetry.NewSQLiteHistoryWithRetention(
			db, 256, time.Hour, 24*time.Hour, time.Duration(retentionDays)*24*time.Hour,
		)
		if historyErr != nil {
			return fmt.Errorf("telemetry history initialization: %w", historyErr)
		}
		telemetryCore, err = telemetry.NewCore(nodeIdentity.NodeID, history)
		if err != nil {
			_ = history.Close()
			return fmt.Errorf("telemetry core initialization: %w", err)
		}
		sampleInterval := time.Duration(cfg.Monitoring.SampleIntervalSeconds) * time.Second
		if err := telemetry.RegisterHostCollectors(telemetryCore, sampleInterval); err != nil {
			_ = telemetryCore.Close()
			return fmt.Errorf("host telemetry collector registration: %w", err)
		}
		if err := telemetryCore.Register(telemetry.NewServiceCollector(ws.ServiceAvailability)); err != nil {
			_ = telemetryCore.Close()
			return fmt.Errorf("service telemetry collector registration: %w", err)
		}
		logger.Info("host telemetry collectors registered",
			"sample_interval_seconds", sampleInterval.Seconds(),
			"collectors", []string{"cpu", "memory", "network", "network_connections", "disk", "disk_inventory", "processes", "sensors", "fans", "smart", "backend", "host", "services"})
		if err := ws.SetTelemetryCore(telemetryCore); err != nil {
			_ = telemetryCore.Close()
			return fmt.Errorf("telemetry command registration: %w", err)
		}
		if err := telemetryCore.Start(ctx); err != nil {
			_ = telemetryCore.Close()
			return fmt.Errorf("telemetry core startup: %w", err)
		}
		defer func() {
			if err := telemetryCore.Close(); err != nil {
				logger.Error("telemetry shutdown failed", "error", err)
			}
		}()
	}

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Server.WebSocketPath, ws.Handle)
	mux.HandleFunc(ws.StreamPath(), ws.HandleStream)
	if cfg.Storage.Enabled {
		mux.HandleFunc(ws.StoragePath(), ws.HandleStorage)
	}
	if cfg.Monitoring.Enabled {
		mux.HandleFunc(cfg.Monitoring.HealthPath, ws.Health)
	}
	endpoints, err := netbind.Discover(cfg.Server.Host, cfg.Server.Port)
	if err != nil {
		return fmt.Errorf("обнаружение приватных интерфейсов: %w", err)
	}
	// KAN-9: standby TLS enrollment on a private interface, plus durable secure
	// Raft/PreparedRPC recovery when already joined. Owner writes stay disabled
	// until authenticated Cluster API (KAN-11), NEVER anonymous /ws.
	var clusterManager *clusterruntime.Manager
	if os.Getenv("HOMECLOUD_RAFT_DEV_BIND") == "" {
		started, openErr := startClusterRuntime(ctx, root, cfg.Server.Port, identityStore, db, endpoints, logger)
		if openErr != nil {
			// A cluster transport error must not take down independent IoT/Storage/WS.
			logger.Error("private cluster runtime unavailable; standalone HTTP services continue", "error", openErr)
		} else {
			clusterManager = started
			defer clusterManager.Close()
		}
	}

	if err := installClusterWS(ctx, ws, clusterManager, db, cfg.Server.Port, logger); err != nil {
		return fmt.Errorf("cluster websocket registration: %w", err)
	}
	type activeServer struct {
		server   *http.Server
		listener net.Listener
	}
	var active []activeServer
	for _, ep := range endpoints {
		listener, listenErr := net.Listen("tcp", ep.Address)
		if listenErr != nil {
			for _, a := range active {
				_ = a.listener.Close()
			}
			return fmt.Errorf("listen %s (%s): %w", ep.Address, ep.Interface, listenErr)
		}
		active = append(active, activeServer{
			listener: listener,
			server: &http.Server{
				Handler:           netbind.RestrictPeer(ep, mux),
				ReadHeaderTimeout: time.Duration(cfg.Server.ReadHeaderTimeoutSeconds) * time.Second,
				MaxHeaderBytes:    16 << 10,
			},
		})
		logger.Info("Private TCP listener ready", "interface", ep.Interface,
			"address", ep.Address, "allowed_remote_subnet", ep.Allowed.String())
	}
	defer func() {
		for _, a := range active {
			_ = a.listener.Close()
		}
	}()
	if cfg.Monitoring.Enabled {
		logger.Info("health endpoint", "path", cfg.Monitoring.HealthPath)
	}

	serverDone := make(chan error, len(active))
	for _, a := range active {
		a := a
		go func() {
			var e error
			if cfg.Server.TLS.Mode == "native" {
				cert, key := cfg.Server.TLS.CertFile, cfg.Server.TLS.KeyFile
				if !filepath.IsAbs(cert) {
					cert = filepath.Join(root, cert)
				}
				if !filepath.IsAbs(key) {
					key = filepath.Join(root, key)
				}
				e = a.server.ServeTLS(a.listener, cert, key)
			} else {
				e = a.server.Serve(a.listener)
			}
			serverDone <- e
		}()
	}
	mediaSupervisor := mediacompanion.Start(ctx, cfg.MediaGateway, root, logger)
	defer mediaSupervisor.Stop()
	select {
	case <-ctx.Done():
		logger.Info("shutdown requested")
	case err := <-serverDone:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			ws.CloseAll()
			return fmt.Errorf("HTTP server: %w", err)
		}
	}
	ws.CloseAll()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), time.Duration(cfg.Server.ShutdownTimeoutSeconds)*time.Second)
	defer shutdownCancel()
	for _, a := range active {
		if err := a.server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
	}
	return nil
}
