package wsserver

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/commands/bootstrap"
	"HomeCloudBackend/internal/connections"
	"HomeCloudBackend/internal/commands/system"
	"HomeCloudBackend/internal/telemetry"
	devicestore "HomeCloudBackend/internal/devices"
	"HomeCloudBackend/internal/events"
	"HomeCloudBackend/internal/mediagateway"
	"HomeCloudBackend/internal/mediarooms"
	"HomeCloudBackend/internal/roomcontrol"
	"HomeCloudBackend/internal/roomheartbeat"
	"HomeCloudBackend/internal/storage"
	"HomeCloudBackend/internal/storageagent"
	"HomeCloudBackend/internal/streaming"
	"HomeCloudBackend/wsconfig"
	"github.com/coder/websocket"
)

// Message contract remains unchanged for existing callers and Postman scripts.
type Inbound = commands.Inbound
type Outbound = commands.Outbound

type Server struct {
	cfg            *wsconfig.Config
	db             *sql.DB
	log            *slog.Logger
	clients        *connections.Manager
	registry       *commands.Registry
	events         *events.Bus
	streams        *streaming.Manager
	gateways       *mediagateway.Registry
	rooms          *mediarooms.Manager
	roomDispatchMu sync.Mutex
	heartbeats     *roomheartbeat.Tracker
	mediaControls  *roomcontrol.Manager
	heartbeatStart sync.Once
	heartbeatClose sync.Once
	heartbeatStop  chan struct{}
	localNodeID    string
	storage        *storage.Manager
	storageAgents *storageagent.Registry
	deviceStore   *devicestore.Store
}

func New(cfg *wsconfig.Config, db *sql.DB, logger *slog.Logger, storageManagers ...*storage.Manager) *Server {
	s := &Server{cfg: cfg, db: db, log: logger,
		clients:  connections.NewManager(cfg.WebSocket.MaxConnections, cfg.WebSocket.MaxConnectionsPerIP),
		registry: commands.NewRegistry(),
		events:   events.New(),
		gateways: mediagateway.New(), rooms: mediarooms.New(4), heartbeats: roomheartbeat.New(), mediaControls: roomcontrol.New(), heartbeatStop: make(chan struct{})}
	s.storageAgents = storageagent.New()
	// The device registry shares the primary SQLite database. Keep it optional
	// for isolated WebSocket tests and embedders that construct a server without DB.
	if db != nil {
		s.deviceStore = devicestore.New(db)
	}
	if len(storageManagers) > 0 {
		s.storage = storageManagers[0]
	}
	if s.storage != nil {
		s.storage.SetIndexPublisher(func(topic string, payload any) {
			eventCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_, _ = s.PublishEvent(eventCtx, topic, payload)
		})
	}
	s.streams = streaming.New(s.clients.IsOnline, func(info streaming.Info, result streaming.Result) {
		// Participants always get a direct control-plane completion/failure
		// event, even when they did not subscribe to Event Bus topics.
		event := commands.Outbound{V: 1, Type: "event", Topic: "streams.closed", Data: result}
		notifyCtx, stopNotify := context.WithTimeout(context.Background(), time.Second)
		_ = s.clients.QueueToDevice(notifyCtx, info.From, event)
		_ = s.clients.QueueToDevice(notifyCtx, info.Target, event)
		stopNotify()
		topic := "streams.finished"
		if result.Status != "completed" {
			topic = "streams.failed"
		}
		_, _ = s.PublishEvent(context.Background(), topic, map[string]any{
			"stream": info, "result": result,
		})
	})
	bootstrap.Register(s.registry, bootstrap.Dependencies{
		Online:              s.clients.Online,
		NodeID:              func() string { return s.localNodeID },
		ConnectedDevices:    s.clients.List,
		DeviceList: func(ctx context.Context) ([]devicestore.Record, error) {
			if s.deviceStore == nil { return nil, errors.New("реестр устройств недоступен без базы данных") }
			return s.deviceStore.List(ctx, s.clients.IsOnline)
		},
		DeviceRename: func(ctx context.Context, id, name string) (devicestore.Record, error) {
			if s.deviceStore == nil { return devicestore.Record{}, errors.New("реестр устройств недоступен без базы данных") }
			item, err := s.deviceStore.Rename(ctx, id, name)
			if err == nil { item.Online = s.clients.IsOnline(id) }
			return item, err
		},
		DeviceForget: func(ctx context.Context, id string) error {
			if s.deviceStore == nil { return errors.New("реестр устройств недоступен без базы данных") }
			if s.clients.IsOnline(id) { return errors.New("нельзя удалить подключённое устройство") }
			return s.deviceStore.Forget(ctx, id)
		},
		PublishDeviceEvent: func(ctx context.Context, topic string, payload any) error {
			_, err := s.PublishEvent(ctx, topic, payload)
			return err
		},
		QueueToDevice:       s.clients.QueueToDevice,
		RequireSubscription: cfg.MessageBus.RequireSubscription,
		PublishEvent:        s.events.Publish,
		StreamCreate:        s.streams.Create,
		StreamCancel:        s.streams.Cancel,
		StreamList:          s.streams.List,
		StreamPath:          s.StreamPath(),
		StorageRegister: func(ctx context.Context, name, path string, priority int) (storage.Volume, error) {
			if s.storage == nil {
				return storage.Volume{}, storage.ErrUnavailable
			}
			return s.storage.Register(ctx, name, path, priority)
		},
		StorageList: func(ctx context.Context) ([]storage.Volume, error) {
			if s.storage == nil {
				return nil, storage.ErrUnavailable
			}
			return s.storage.List(ctx)
		},
		StorageFiles: func(ctx context.Context, id string, limit int) ([]storage.File, error) {
			if s.storage == nil {
				return nil, storage.ErrUnavailable
			}
			return s.storage.ListFiles(ctx, id, limit)
		},
		StorageScan: func(ctx context.Context, id string, recursive bool, limit int) (storage.ScanResult, error) {
			if s.storage == nil {
				return storage.ScanResult{}, storage.ErrUnavailable
			}
			return s.storage.Scan(ctx, id, recursive, limit)
		},
		StorageIndex: func(ctx context.Context, req storage.IndexRequest, reindex bool) (storage.IndexJob, error) {
			if s.storage == nil {
				return storage.IndexJob{}, storage.ErrUnavailable
			}
			return s.storage.BeginIndex(ctx, req, reindex)
		},
		StorageIndexStatus: func(id string) (storage.IndexJob, error) {
			if s.storage == nil {
				return storage.IndexJob{}, storage.ErrUnavailable
			}
			return s.storage.IndexStatus(id)
		},
		StorageIndexCancel: func(id string) error {
			if s.storage == nil {
				return storage.ErrUnavailable
			}
			return s.storage.CancelIndex(id)
		},
		StorageGetFile: func(ctx context.Context, id string) (storage.File, error) {
			if s.storage == nil {
				return storage.File{}, storage.ErrUnavailable
			}
			return s.storage.GetFile(ctx, id)
		},
		StorageUpload: func(ctx context.Context, owner, name, mime, id string, size int64) (storage.TransferInfo, error) {
			if s.storage == nil {
				return storage.TransferInfo{}, storage.ErrUnavailable
			}
			t, e := s.storage.PrepareUpload(ctx, owner, name, mime, id, size)
			if e == nil {
				t.Path = s.StoragePath()
			}
			return t, e
		},
		StorageDownload: func(ctx context.Context, owner, id string) (storage.TransferInfo, error) {
			if s.storage == nil {
				return storage.TransferInfo{}, storage.ErrUnavailable
			}
			t, e := s.storage.PrepareDownload(ctx, owner, id)
			if e == nil {
				t.Path = s.StoragePath()
			}
			return t, e
		},
		StorageCancel: func(owner, id string) error {
			if s.storage == nil {
				return storage.ErrUnavailable
			}
			return s.storage.Cancel(owner, id)
		},
	})
	s.registerMediaGatewayCommands()
	s.registerMediaRoomCommands()
	s.registerStorageAgentCommands()
	return s
}

// RegisterCommand registers an application command before serving connections.
func (s *Server) RegisterCommand(action string, h commands.Handler) error {
	return s.registry.Register(action, h)
}

// SetTelemetryCore registers telemetry commands before the server starts serving.
func (s *Server) SetTelemetryCore(core *telemetry.Core) error {
	if core == nil {
		return errors.New("telemetry core is unavailable")
	}
	if err := core.SetObserver(func(samples []telemetry.Sample) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		_, err := s.PublishEvent(ctx, "system.metrics", map[string]any{
			"samples": samples,
			"collected_at": time.Now().UTC().Format(time.RFC3339Nano),
		})
		if err != nil && s.log != nil {
			s.log.Warn("publish telemetry sample event failed", "error", err)
		}
	}); err != nil {
		return err
	}
	return system.RegisterTelemetry(s.registry, core, s.cfg.MessageBus.RequireSubscription)
}

// PublishEvent permits future Go modules to broadcast without touching sockets.
func (s *Server) PublishEvent(ctx context.Context, topic string, payload any) (events.PublishResult, error) {
	return s.events.Publish(ctx, topic, "backend", payload)
}

func (s *Server) CloseAll() {
	s.heartbeatClose.Do(func() { close(s.heartbeatStop) })
	s.heartbeats.Close()
	s.streams.CloseAll()
	if s.storage != nil {
		s.storage.CloseAll()
	}
	s.rooms.Close()
	s.clients.CloseAll()
}

func (s *Server) StreamPath() string {
	return strings.TrimSuffix(s.cfg.Server.WebSocketPath, "/") + "/stream"
}

// SendToDevice preserves the existing synchronous JSON delivery API. It means
// written to the socket transport, not acted on by the receiving application.
func (s *Server) SendToDevice(ctx context.Context, id string, message Outbound) error {
	return s.clients.SendToDevice(ctx, id, message)
}

func (s *Server) Health(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.PingContext(ctx); err != nil {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (s *Server) Handle(w http.ResponseWriter, r *http.Request) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "invalid remote address", http.StatusBadRequest)
		return
	}
	release, ok := s.clients.Acquire(ip)
	if !ok {
		http.Error(w, "connection limit", http.StatusTooManyRequests)
		return
	}
	defer release()

	opts := &websocket.AcceptOptions{OriginPatterns: s.cfg.WebSocket.AllowedOrigins}
	if s.cfg.WebSocket.CompressionEnabled {
		opts.CompressionMode = websocket.CompressionContextTakeover
	}
	conn, err := websocket.Accept(w, r, opts)
	if err != nil {
		s.log.Warn("WS upgrade rejected", "error", err)
		return
	}
	defer conn.CloseNow()
	untrack := s.clients.Track(func() { conn.CloseNow() })
	defer untrack()
	conn.SetReadLimit(int64(s.cfg.WebSocket.MaxMessageBytes))

	// After Upgrade the request context is not a reliable lifetime signal.
	ctx, stop := context.WithCancel(context.Background())
	defer stop()

	timeout := s.cfg.WebSocket.HelloTimeoutSeconds
	if timeout <= 0 {
		timeout = 10
	}
	helloCtx, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	typ, raw, err := conn.Read(helloCtx)
	cancel()
	if err != nil || typ != websocket.MessageText {
		_ = conn.Close(websocket.StatusPolicyViolation, "hello required")
		return
	}
	var hello Inbound
	if json.Unmarshal(raw, &hello) != nil || hello.V != 1 || hello.Type != "hello" ||
		(s.cfg.MessageBus.RequireMessageID && hello.ID == "") ||
		!connections.ValidDeviceID(hello.DeviceID) || !connections.ValidClientType(hello.ClientType) {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid hello")
		return
	}

	// Zero-valued test configurations must not create an already-expired
	// response context. NewPeer uses the same 10-second fallback internally.
	writeTimeout := time.Duration(s.cfg.WebSocket.WriteTimeoutSeconds) * time.Second
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	peer := connections.NewPeer(hello.DeviceID, hello.ClientType,
		s.cfg.WebSocket.MaxOutboundQueueMessages,
		writeTimeout,
		func(ctx context.Context, b []byte) error { return conn.Write(ctx, websocket.MessageText, b) },
		func() { conn.CloseNow() },
	)
	if err := s.clients.Register(peer); err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	if s.deviceStore != nil {
		registerCtx, registerCancel := context.WithTimeout(ctx, 3*time.Second)
		if err := s.deviceStore.Register(registerCtx, peer.DeviceID, peer.ClientType); err != nil {
			registerCancel()
			s.clients.Unregister(peer)
			s.log.Error("device registry registration failed", "device_id", peer.DeviceID, "error", err)
			_ = conn.Close(websocket.StatusInternalError, "device registration failed")
			return
		}
		registerCancel()
	}
	subscriber := s.events.Attach(peer.DeviceID, func(ctx context.Context, message commands.Outbound) error {
		err := peer.QueueJSON(ctx, message)
		if errors.Is(err, connections.ErrQueueFull) && s.cfg.MessageBus.SlowConsumerPolicy == "disconnect" {
			peer.Close()
		}
		return err
	})
	defer func() {
		// Cancel all ongoing streams when the control connection disappears.
		s.streams.CancelDevice(peer.DeviceID)
		// Remove only this session's subscriptions before releasing its ID.
		s.events.Detach(subscriber)
		s.gateways.Unregister(peer.DeviceID, peer.ConnectedAt.UnixNano())
		s.storageAgents.Disconnect(peer.DeviceID, peer.ConnectedAt.UnixNano())
		s.clients.Unregister(peer)
		s.onMediaPeerDisconnected(peer.DeviceID)
		_, _ = s.PublishEvent(context.Background(), "clients.disconnected", map[string]string{
			"device_id": peer.DeviceID, "client_type": peer.ClientType,
		})
	}()
	go peer.RunWriter(ctx)

	write := func(message Outbound) error {
		writeCtx, writeCancel := context.WithTimeout(ctx, writeTimeout)
		defer writeCancel()
		return peer.SendJSON(writeCtx, message)
	}
	if err := write(Outbound{V: 1, ID: hello.ID, Type: "identified", Data: map[string]string{
		"device_id": peer.DeviceID, "client_type": peer.ClientType,
	}}); err != nil {
		return
	}

	s.log.Info("WS identified", "device_id", peer.DeviceID, "client_type", peer.ClientType, "remote_ip", ip)
	_, _ = s.PublishEvent(ctx, "clients.connected", map[string]string{
		"device_id": peer.DeviceID, "client_type": peer.ClientType,
	})
	defer s.log.Info("WS disconnected", "device_id", peer.DeviceID)

	hbCtx, stopHB := context.WithCancel(ctx)
	defer stopHB()
	if s.cfg.WebSocket.PingIntervalSeconds > 0 {
		go func() {
			ticker := time.NewTicker(time.Duration(s.cfg.WebSocket.PingIntervalSeconds) * time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-hbCtx.Done():
					return
				case <-ticker.C:
					pingCtx, cancelPing := context.WithTimeout(hbCtx, time.Duration(s.cfg.WebSocket.PongTimeoutSeconds)*time.Second)
					err := conn.Ping(pingCtx)
					cancelPing()
					if err != nil {
						conn.CloseNow()
						return
					}
				}
			}
		}()
	}

	var windowStart time.Time
	count := 0
	for {
		typ, raw, err := conn.Read(ctx)
		if err != nil {
			return
		}
		if typ != websocket.MessageText {
			if err := write(Outbound{V: 1, Type: "error", Error: "only JSON text accepted"}); err != nil {
				return
			}
			continue
		}
		now := time.Now()
		if now.Sub(windowStart) >= time.Second {
			windowStart = now
			count = 0
		}
		count++
		if count > s.cfg.WebSocket.MaxMessagesPerSecond {
			_ = conn.Close(websocket.StatusPolicyViolation, "rate limit")
			return
		}
		var m Inbound
		if err := json.Unmarshal(raw, &m); err != nil || m.V != 1 || m.Type == "" {
			if err := write(Outbound{V: 1, Type: "error", Error: "invalid message"}); err != nil {
				return
			}
			continue
		}
		if s.cfg.MessageBus.RequireMessageID && m.ID == "" {
			if err := write(Outbound{V: 1, Type: "error", Error: "id is required"}); err != nil {
				return
			}
			continue
		}
		response := Outbound{V: 1, ID: m.ID}
		switch {
		case m.Type == "ping":
			response.Type = "pong"
		case m.Type == "subscribe":
			_, err := s.events.Subscribe(subscriber, m.Topic)
			if err != nil {
				response.Type = "error"
				response.Error = err.Error()
			} else {
				response.Type = "subscribed"
				response.Data = map[string]any{"topic": m.Topic}
			}
		case m.Type == "unsubscribe":
			_, err := s.events.Unsubscribe(subscriber, m.Topic)
			if err != nil {
				response.Type = "error"
				response.Error = err.Error()
			} else {
				response.Type = "unsubscribed"
				response.Data = map[string]any{"topic": m.Topic}
			}
		case m.Type == "command":
			subs := make(map[string]struct{})
			for _, topic := range s.events.Topics(subscriber) {
				subs[topic] = struct{}{}
			}
			response = s.registry.Dispatch(ctx, m, commands.ClientContext{
				DeviceID: peer.DeviceID, ClientType: peer.ClientType, RemoteIP: ip, SessionID: peer.ConnectedAt.UnixNano(), Subscriptions: subs,
			})
		case m.Type == "disconnect":
			response.Type = "disconnected"
			_ = write(response)
			_ = conn.Close(websocket.StatusNormalClosure, "client disconnect")
			return
		default:
			response.Type = "error"
			response.Error = "unknown or unsupported action"
		}
		if err := write(response); err != nil && !errors.Is(err, context.Canceled) {
			return
		}
	}
}
