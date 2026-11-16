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

func TestClientToClientMessageOverWebSocket(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.WebSocket.MaxConnections = 10
	cfg.WebSocket.MaxConnectionsPerIP = 10
	cfg.WebSocket.MaxOutboundQueueMessages = 16
	cfg.WebSocket.MaxMessagesPerSecond = 100
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 3
	cfg.WebSocket.PingIntervalSeconds = 60
	cfg.WebSocket.PongTimeoutSeconds = 2
	cfg.WebSocket.HelloTimeoutSeconds = 3
	cfg.MessageBus.RequireMessageID = true
	s := New(cfg, nil, slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.Handle)
	ts := httptest.NewServer(mux)
	defer ts.Close()
	defer s.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	url := "ws" + strings.TrimPrefix(ts.URL, "http") + "/ws"
	connect := func(id, kind string) *websocket.Conn {
		t.Helper()
		conn, _, err := websocket.Dial(ctx, url, nil)
		if err != nil {
			t.Fatal(err)
		}
		sendWS(t, ctx, conn, Inbound{V: 1, ID: "h-" + id, Type: "hello", DeviceID: id, ClientType: kind})
		response := readWS(t, ctx, conn)
		if response.Type != "identified" {
			t.Fatalf("hello %s: %+v", id, response)
		}
		return conn
	}
	tv := connect("kitchen-tv", "television")
	defer tv.CloseNow()
	pc := connect("desktop", "desktop")
	defer pc.CloseNow()
	sendWS(t, ctx, pc, Inbound{V: 1, ID: "r1", Type: "command", Action: "clients.send",
		Data: json.RawMessage(`{"target":"kitchen-tv","payload":{"action":"media.pause"}}`)})
	resp := readWS(t, ctx, pc)
	if resp.Type != "result" || resp.Action != "clients.send" {
		t.Fatalf("sender response: %+v", resp)
	}
	msg := readWS(t, ctx, tv)
	if msg.Type != "event" || msg.Topic != "clients.message" {
		t.Fatalf("target event: %+v", msg)
	}
	data, err := json.Marshal(msg.Data)
	if err != nil {
		t.Fatal(err)
	}
	var received struct {
		From      string            `json:"from"`
		RequestID string            `json:"request_id"`
		Payload   map[string]string `json:"payload"`
	}
	if err := json.Unmarshal(data, &received); err != nil {
		t.Fatal(err)
	}
	if received.From != "desktop" || received.RequestID != "r1" || received.Payload["action"] != "media.pause" {
		t.Fatalf("received %+v", received)
	}
	// A disconnected recipient must not be silently accepted.
	sendWS(t, ctx, tv, Inbound{V: 1, ID: "bye", Type: "disconnect"})
	if readWS(t, ctx, tv).Type != "disconnected" {
		t.Fatal("missing disconnect confirmation")
	}
	// Complete the client side of the WebSocket close handshake. Without
	// this the server's graceful Close may wait for its close timeout, while
	// the test expects the device to be unregistered within two seconds.
	_ = tv.Close(websocket.StatusNormalClosure, "test disconnect")
	// Wait for the server to unregister the closed peer.
	limit := time.Now().Add(2 * time.Second)
	for s.clients.Online() != 1 && time.Now().Before(limit) {
		time.Sleep(time.Millisecond)
	}
	sendWS(t, ctx, pc, Inbound{V: 1, ID: "r2", Type: "command", Action: "clients.send",
		Data: json.RawMessage(`{"target":"kitchen-tv","payload":{"action":"media.pause"}}`)})
	if out := readWS(t, ctx, pc); out.Type != "error" || out.Error != "device not connected" {
		t.Fatalf("offline: %+v", out)
	}
	tv2 := connect("kitchen-tv", "television")
	defer tv2.CloseNow()
	sendWS(t, ctx, pc, Inbound{V: 1, ID: "r3", Type: "command", Action: "clients.send",
		Data: json.RawMessage(`{"target":"kitchen-tv","payload":{"action":"media.play"}}`)})
	if out := readWS(t, ctx, pc); out.Type != "result" {
		t.Fatalf("reconnected: %+v", out)
	}
	if out := readWS(t, ctx, tv2); out.Topic != "clients.message" {
		t.Fatalf("new TV event: %+v", out)
	}
}

func sendWS(t *testing.T, ctx context.Context, conn *websocket.Conn, m Inbound) {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(ctx, websocket.MessageText, b); err != nil {
		t.Fatal(err)
	}
}
func readWS(t *testing.T, ctx context.Context, conn *websocket.Conn) Outbound {
	t.Helper()
	typ, b, err := conn.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if typ != websocket.MessageText {
		t.Fatalf("unexpected message type %v", typ)
	}
	var result Outbound
	if err := json.Unmarshal(b, &result); err != nil {
		t.Fatal(err)
	}
	return result
}
