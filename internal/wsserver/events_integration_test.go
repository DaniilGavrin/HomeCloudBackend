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

func TestEventBusOverWebSocket(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.WebSocket.MaxConnections = 10
	cfg.WebSocket.MaxConnectionsPerIP = 10
	cfg.WebSocket.MaxOutboundQueueMessages = 20
	cfg.WebSocket.MaxMessagesPerSecond = 100
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 3
	cfg.WebSocket.PingIntervalSeconds = 60
	cfg.WebSocket.PongTimeoutSeconds = 2
	cfg.WebSocket.HelloTimeoutSeconds = 3
	cfg.MessageBus.RequireMessageID = true
	cfg.MessageBus.RequireSubscription = true

	s := New(cfg, nil, slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.Handle)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer s.CloseAll()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	connect := func(deviceID string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.Dial(ctx, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendWS(t, ctx, conn, Inbound{V: 1, ID: "hello-" + deviceID, Type: "hello", DeviceID: deviceID, ClientType: "desktop"})
		if got := readWS(t, ctx, conn); got.Type != "identified" {
			t.Fatalf("hello: %+v", got)
		}
		return conn
	}
	sub := func(conn *websocket.Conn, topic, id string) {
		t.Helper()
		sendWS(t, ctx, conn, Inbound{V: 1, ID: id, Type: "subscribe", Topic: topic})
		if got := readWS(t, ctx, conn); got.Type != "subscribed" {
			t.Fatalf("subscribe: %+v", got)
		}
	}
	pub := func(conn *websocket.Conn, requestID, topic string, data string) map[string]any {
		t.Helper()
		payload, _ := json.Marshal(map[string]any{"topic": topic, "payload": json.RawMessage(data)})
		sendWS(t, ctx, conn, Inbound{V: 1, ID: requestID, Type: "command", Action: "events.publish", Data: payload})
		out := readWS(t, ctx, conn)
		if out.Type != "result" || out.Action != "events.publish" {
			t.Fatalf("publish %+v", out)
		}
		return out.Data.(map[string]any)
	}
	// Two receivers subscribe to the same arbitrary event topic.
	tv := connect("tv")
	defer tv.CloseNow()
	tablet := connect("tablet")
	defer tablet.CloseNow()
	sub(tv, "media.playback.changed", "sub-tv")
	sub(tablet, "media.playback.changed", "sub-tablet")
	pc := connect("pc")
	defer pc.CloseNow()

	counts := pub(pc, "pub-1", "media.playback.changed", `{"position_ms":2500}`)
	if counts["subscribers"] != float64(2) || counts["queued"] != float64(2) || counts["dropped"] != float64(0) {
		t.Fatalf("wrong fanout %+v", counts)
	}
	for _, receiver := range []*websocket.Conn{tv, tablet} {
		out := readWS(t, ctx, receiver)
		if out.Type != "event" || out.Topic != "media.playback.changed" {
			t.Fatalf("delivered %+v", out)
		}
		value := out.Data.(map[string]any)
		if value["from"] != "pc" {
			t.Fatalf("wrong origin %+v", value)
		}
		payload := value["payload"].(map[string]any)
		if payload["position_ms"] != float64(2500) {
			t.Fatalf("wrong payload %+v", payload)
		}
	}

	// Unsubscribe works without affecting other clients.
	sendWS(t, ctx, tablet, Inbound{V: 1, ID: "un-1", Type: "unsubscribe", Topic: "media.playback.changed"})
	if got := readWS(t, ctx, tablet); got.Type != "unsubscribed" {
		t.Fatalf("unsubscribed: %+v", got)
	}
	counts = pub(pc, "pub-2", "media.playback.changed", `"playing"`)
	if counts["subscribers"] != float64(1) || counts["queued"] != float64(1) {
		t.Fatalf("after unsubscribe %+v", counts)
	}
	if out := readWS(t, ctx, tv); out.Topic != "media.playback.changed" {
		t.Fatalf("remaining subscription: %+v", out)
	}

	// A subscription snapshot is exposed through the command registry.
	sendWS(t, ctx, tv, Inbound{V: 1, ID: "topics-1", Type: "command", Action: "events.subscriptions"})
	out := readWS(t, ctx, tv)
	if out.Type != "result" || out.Action != "events.subscriptions" {
		t.Fatalf("topics: %+v", out)
	}
	snapshot := out.Data.(map[string]any)["topics"].([]any)
	if len(snapshot) != 1 || snapshot[0] != "media.playback.changed" {
		t.Fatalf("wrong topics %+v", snapshot)
	}

	// Disconnect the remaining subscriber. A new connection using the same
	// device_id must not inherit its subscriptions.
	sendWS(t, ctx, tv, Inbound{V: 1, ID: "bye", Type: "disconnect"})
	if out := readWS(t, ctx, tv); out.Type != "disconnected" {
		t.Fatalf("disconnect %+v", out)
	}
	_ = tv.Close(websocket.StatusNormalClosure, "test disconnect")
	deadline := time.Now().Add(2 * time.Second)
	for s.clients.Online() != 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if s.clients.Online() != 2 {
		t.Fatalf("old TV still online")
	}
	tv2 := connect("tv")
	defer tv2.CloseNow()
	counts = pub(pc, "pub-3", "media.playback.changed", `true`)
	if counts["subscribers"] != float64(0) {
		t.Fatalf("subscription leaked %+v", counts)
	}
}

func TestBackendPresenceEventsOverWebSocket(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.WebSocket.MaxConnections = 5
	cfg.WebSocket.MaxConnectionsPerIP = 5
	cfg.WebSocket.MaxOutboundQueueMessages = 16
	cfg.WebSocket.MaxMessagesPerSecond = 50
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 2
	cfg.WebSocket.HelloTimeoutSeconds = 2
	cfg.WebSocket.PingIntervalSeconds = 60
	cfg.MessageBus.RequireMessageID = true
	s := New(cfg, nil, slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.Handle)
	server := httptest.NewServer(mux)
	defer server.Close()
	defer s.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws"
	attach := func(id string) *websocket.Conn {
		t.Helper()
		c, _, err := websocket.Dial(ctx, endpoint, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendWS(t, ctx, c, Inbound{V: 1, ID: "hello-" + id, Type: "hello", DeviceID: id, ClientType: "desktop"})
		if result := readWS(t, ctx, c); result.Type != "identified" {
			t.Fatalf("hello %+v", result)
		}
		return c
	}
	observer := attach("observer")
	defer observer.CloseNow()
	for _, topic := range []string{"clients.connected", "clients.disconnected"} {
		sendWS(t, ctx, observer, Inbound{V: 1, ID: topic, Type: "subscribe", Topic: topic})
		if resp := readWS(t, ctx, observer); resp.Type != "subscribed" {
			t.Fatalf("sub %+v", resp)
		}
	}
	target := attach("tv")
	if evt := readWS(t, ctx, observer); evt.Type != "event" || evt.Topic != "clients.connected" {
		t.Fatalf("connected %+v", evt)
	}
	sendWS(t, ctx, target, Inbound{V: 1, ID: "bye", Type: "disconnect"})
	if out := readWS(t, ctx, target); out.Type != "disconnected" {
		t.Fatalf("tv bye %+v", out)
	}
	_ = target.Close(websocket.StatusNormalClosure, "test disconnect")
	if evt := readWS(t, ctx, observer); evt.Type != "event" || evt.Topic != "clients.disconnected" {
		t.Fatalf("disconnected %+v", evt)
	}
}
