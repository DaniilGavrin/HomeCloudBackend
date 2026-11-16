package wsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/wsconfig"
	"github.com/coder/websocket"
)

func TestBinaryStreamRelayOverWebSocket(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.Server.WebSocketPath = "/ws"
	cfg.WebSocket.MaxConnections = 12
	cfg.WebSocket.MaxConnectionsPerIP = 12
	cfg.WebSocket.MaxOutboundQueueMessages = 20
	cfg.WebSocket.MaxMessagesPerSecond = 100
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 3
	cfg.WebSocket.HelloTimeoutSeconds = 3
	cfg.WebSocket.PingIntervalSeconds = 60
	cfg.MessageBus.RequireMessageID = true
	s := New(cfg, nil, slog.Default())
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.Handle)
	mux.HandleFunc(s.StreamPath(), s.HandleStream)
	server := httptest.NewServer(mux)
	defer server.Close()
	defer s.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	base := "ws" + strings.TrimPrefix(server.URL, "http")
	connect := func(id string) *websocket.Conn {
		t.Helper()
		c, _, err := websocket.Dial(ctx, base+"/ws", nil)
		if err != nil {
			t.Fatal(err)
		}
		sendWS(t, ctx, c, Inbound{V: 1, ID: "hello-" + id, Type: "hello", DeviceID: id, ClientType: "desktop"})
		if out := readWS(t, ctx, c); out.Type != "identified" {
			t.Fatalf("identified: %+v", out)
		}
		return c
	}
	pc := connect("pc")
	defer pc.CloseNow()
	tv := connect("tv")
	defer tv.CloseNow()
	cmdData, _ := json.Marshal(map[string]any{"target": "tv", "name": "sample.pdf", "mime_type": "application/pdf", "size_bytes": 131072})
	sendWS(t, ctx, pc, Inbound{V: 1, ID: "c1", Type: "command", Action: "streams.create", Data: cmdData})
	created := readWS(t, ctx, pc)
	if created.Type != "result" {
		t.Fatalf("create: %+v", created)
	}
	id := created.Data.(map[string]any)["stream_id"].(string)
	offer := readWS(t, ctx, tv)
	if offer.Type != "event" || offer.Topic != "streams.offer" || offer.Data.(map[string]any)["stream_id"] != id {
		t.Fatalf("offer %+v", offer)
	}
	attach := func(device, role string) *websocket.Conn {
		t.Helper()
		c, _, err := websocket.Dial(ctx, base+"/ws/stream", nil)
		if err != nil {
			t.Fatal(err)
		}
		// Without this the first 64 KiB binary frame exceeds the library's
		// default 32 KiB read limit and the receiving endpoint is closed.
		c.SetReadLimit(1 << 20)
		b, _ := json.Marshal(map[string]any{"v": 1, "type": "stream.hello", "stream_id": id, "device_id": device, "role": role})
		if err := c.Write(ctx, websocket.MessageText, b); err != nil {
			t.Fatal(err)
		}
		_, b, err = c.Read(ctx)
		if err != nil || !bytes.Contains(b, []byte(`"stream.attached"`)) {
			t.Fatalf("attach %v %s", err, b)
		}
		return c
	}
	receiver := attach("tv", "receiver")
	defer receiver.CloseNow()
	sender := attach("pc", "sender")
	defer sender.CloseNow()
	for _, c := range []*websocket.Conn{receiver, sender} {
		kind, b, err := c.Read(ctx)
		if err != nil || kind != websocket.MessageText || !bytes.Contains(b, []byte(`"stream.ready"`)) {
			t.Fatalf("ready %v %s", err, b)
		}
	}
	content := bytes.Repeat([]byte("0123456789abcdef"), 8192) // 128 KiB
	half := len(content) / 2
	for _, chunk := range [][]byte{content[:half], content[half:]} {
		if err := sender.Write(ctx, websocket.MessageBinary, chunk); err != nil {
			t.Fatal(err)
		}
	}
	if err := sender.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"stream.end"}`)); err != nil {
		t.Fatal(err)
	}
	var received []byte
	for i := 0; i < 2; i++ {
		kind, reader, err := receiver.Reader(ctx)
		if err != nil || kind != websocket.MessageBinary {
			t.Fatalf("chunk %d: %v %v", i, kind, err)
		}
		b, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		received = append(received, b...)
	}
	if !bytes.Equal(received, content) {
		t.Fatal("corrupt relay payload")
	}
	kind, b, err := receiver.Read(ctx)
	if err != nil || kind != websocket.MessageText || !bytes.Contains(b, []byte(`"stream.complete"`)) {
		t.Fatalf("receiver completion %v %s", err, b)
	}
	kind, b, err = sender.Read(ctx)
	if err != nil || kind != websocket.MessageText || !bytes.Contains(b, []byte(`"stream.complete"`)) {
		t.Fatalf("sender completion %v %s", err, b)
	}
}

func TestStreamCreateRejectsOfflineTarget(t *testing.T) {
	cfg := &wsconfig.Config{}
	cfg.Server.WebSocketPath = "/ws"
	cfg.WebSocket.MaxConnections = 4
	cfg.WebSocket.MaxConnectionsPerIP = 4
	cfg.WebSocket.MaxOutboundQueueMessages = 8
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.MaxMessagesPerSecond = 20
	cfg.WebSocket.HelloTimeoutSeconds = 3
	s := New(cfg, nil, slog.Default())
	server := httptest.NewServer(http.HandlerFunc(s.Handle))
	defer server.Close()
	defer s.CloseAll()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.CloseNow()
	sendWS(t, ctx, c, Inbound{V: 1, ID: "h", Type: "hello", DeviceID: "pc", ClientType: "desktop"})
	_ = readWS(t, ctx, c)
	sendWS(t, ctx, c, Inbound{V: 1, ID: "c", Type: "command", Action: "streams.create", Data: json.RawMessage(`{"target":"offline-tv"}`)})
	if out := readWS(t, ctx, c); out.Type != "error" {
		t.Fatalf("offline target: %+v", out)
	}
}
