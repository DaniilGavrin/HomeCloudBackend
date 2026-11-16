package wsserver

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/wsconfig"
	"github.com/coder/websocket"
)

func TestCommandRoutingOverWebSocket(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.WebSocket.MaxConnections = 5
	cfg.WebSocket.MaxConnectionsPerIP = 5
	cfg.WebSocket.MaxMessagesPerSecond = 40
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 2
	cfg.WebSocket.PingIntervalSeconds = 60
	cfg.WebSocket.PongTimeoutSeconds = 2
	cfg.WebSocket.HelloTimeoutSeconds = 2
	cfg.MessageBus.RequireSubscription = true
	cfg.MessageBus.RequireMessageID = true
	s := New(cfg, nil, slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.Handle)
	httpServer := httptest.NewServer(mux)
	defer httpServer.Close()
	defer s.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(httpServer.URL, "http")+"/ws", nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.CloseNow()
	send := func(in Inbound) Outbound {
		t.Helper()
		bytes, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Write(ctx, websocket.MessageText, bytes); err != nil {
			t.Fatalf("write: %v", err)
		}
		typ, raw, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if typ != websocket.MessageText {
			t.Fatalf("type %v", typ)
		}
		var out Outbound
		if err := json.Unmarshal(raw, &out); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return out
	}
	hello := send(Inbound{V: 1, ID: "hello-1", Type: "hello", DeviceID: "desktop", ClientType: "computer"})
	if hello.Type != "identified" || hello.ID != "hello-1" {
		t.Fatalf("hello %+v", hello)
	}
	list := send(Inbound{V: 1, ID: "list-1", Type: "command", Action: "clients.list"})
	if list.Type != "result" || list.Action != "clients.list" {
		t.Fatalf("list %+v", list)
	}
	health := send(Inbound{V: 1, ID: "health-1", Type: "command", Action: "system.health"})
	if health.Type != "error" || health.Error != "subscribe to system.health first" {
		t.Fatalf("health without subscribe %+v", health)
	}
	sub := send(Inbound{V: 1, ID: "sub-1", Type: "subscribe", Topic: "system.health"})
	if sub.Type != "subscribed" {
		t.Fatalf("subscribe %+v", sub)
	}
	health = send(Inbound{V: 1, ID: "health-2", Type: "command", Action: "system.health"})
	if health.Type != "result" || health.Action != "system.health" {
		t.Fatalf("health %+v", health)
	}
	missing := send(Inbound{V: 1, ID: "unknown", Type: "command", Action: "unknown.command"})
	if missing.Type != "error" || missing.Error != "unknown or unsupported action" {
		t.Fatalf("unknown %+v", missing)
	}
}
