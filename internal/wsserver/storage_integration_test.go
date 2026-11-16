package wsserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"HomeCloudBackend/internal/database"
	store "HomeCloudBackend/internal/storage"
	"HomeCloudBackend/wsconfig"
	"github.com/coder/websocket"
)

func TestStorageUploadDownloadWebSocket(t *testing.T) {
	root := t.TempDir()
	folder := filepath.Join(root, "data", "files")
	if err := os.MkdirAll(folder, 0700); err != nil {
		t.Fatal(err)
	}
	byteZero := uint8(0)
	cfg := &wsconfig.Config{}
	cfg.TypeStart.DBByte = &byteZero
	cfg.DBServer.FilePath = "./data/db.sqlite"
	cfg.DBServer.SQLite.JournalMode = "WAL"
	cfg.DBServer.SQLite.BusyTimeoutMs = 5000
	cfg.DBServer.SQLite.ForeignKeys = true
	cfg.Storage.Enabled = true
	cfg.Storage.MaxUploadSizeMB = 100
	cfg.Storage.AllowedRoots = []string{"./data/files"}
	cfg.Server.WebSocketPath = "/ws"
	cfg.WebSocket.MaxConnections = 20
	cfg.WebSocket.MaxConnectionsPerIP = 20
	cfg.WebSocket.MaxOutboundQueueMessages = 32
	cfg.WebSocket.MaxMessagesPerSecond = 50
	cfg.WebSocket.MaxMessageBytes = 65536
	cfg.WebSocket.WriteTimeoutSeconds = 5
	cfg.WebSocket.HelloTimeoutSeconds = 5
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := database.Open(ctx, cfg, root)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := database.Init(ctx, db); err != nil {
		t.Fatal(err)
	}
	manager, err := store.New(cfg.Storage, db, root)
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, db, slog.Default(), manager)
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", srv.Handle)
	mux.HandleFunc("/ws/storage", srv.HandleStorage)
	web := httptest.NewServer(mux)
	defer web.Close()
	defer srv.CloseAll()
	base := "ws" + strings.TrimPrefix(web.URL, "http")
	ctl, _, err := websocket.Dial(ctx, base+"/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ctl.CloseNow()
	sendWS(t, ctx, ctl, Inbound{V: 1, ID: "hello", Type: "hello", DeviceID: "desktop", ClientType: "desktop"})
	if result := readWS(t, ctx, ctl); result.Type != "identified" {
		t.Fatalf("hello: %+v", result)
	}
	call := func(id, act string, args any) map[string]any {
		t.Helper()
		raw, _ := json.Marshal(args)
		sendWS(t, ctx, ctl, Inbound{V: 1, ID: id, Type: "command", Action: act, Data: raw})
		r := readWS(t, ctx, ctl)
		if r.Type != "result" {
			t.Fatalf("%s: %+v", act, r)
		}
		return r.Data.(map[string]any)
	}
	// Auto is unavailable until an explicit volume registration.
	raw, _ := json.Marshal(map[string]any{"name": "a.pdf", "size_bytes": 1, "storage_id": "auto"})
	sendWS(t, ctx, ctl, Inbound{V: 1, ID: "missing", Type: "command", Action: "storage.upload.prepare", Data: raw})
	if r := readWS(t, ctx, ctl); r.Type != "error" {
		t.Fatalf("no volume: %+v", r)
	}
	volume := call("reg", "storage.register", map[string]any{"name": "Local", "path": "./data/files", "priority": 10})
	volumeID := volume["storage_id"].(string)
	list := call("list", "storage.list", map[string]any{})
	if len(list["storages"].([]any)) != 1 {
		t.Fatalf("volumes: %+v", list)
	}
	content := bytes.Repeat([]byte("application/pdf\n"), 8192)
	meta := call("up", "storage.upload.prepare", map[string]any{"name": "vakansiya.pdf", "mime_type": "application/pdf", "size_bytes": len(content), "storage_id": "auto"})
	if meta["storage_id"] != volumeID {
		t.Fatalf("auto selection: %+v", meta)
	}
	connectStorage := func(id, role string) *websocket.Conn {
		t.Helper()
		c, _, err := websocket.Dial(ctx, base+"/ws/storage", nil)
		if err != nil {
			t.Fatal(err)
		}
		c.SetReadLimit(1 << 20)
		hello, _ := json.Marshal(map[string]any{"v": 1, "type": "storage.hello", "transfer_id": id, "device_id": "desktop", "role": role})
		if err := c.Write(ctx, websocket.MessageText, hello); err != nil {
			t.Fatal(err)
		}
		_, data, err := c.Read(ctx)
		if err != nil || !bytes.Contains(data, []byte(`"storage.ready"`)) {
			t.Fatalf("storage.ready: %v %s", err, data)
		}
		return c
	}
	up := connectStorage(meta["transfer_id"].(string), "upload")
	sum := sha256.Sum256(content)
	for start := 0; start < len(content); start += 32000 {
		end := start + 32000
		if end > len(content) {
			end = len(content)
		}
		if err := up.Write(ctx, websocket.MessageBinary, content[start:end]); err != nil {
			t.Fatal(err)
		}
	}
	if err := up.Write(ctx, websocket.MessageText, []byte(`{"v":1,"type":"storage.end","sha256":"`+hex.EncodeToString(sum[:])+`"}`)); err != nil {
		t.Fatal(err)
	}
	_, completion, err := up.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(completion, &result); err != nil {
		t.Fatal(err)
	}
	if result["type"] != "storage.complete" {
		t.Fatalf("upload: %s", completion)
	}
	fileID := result["file"].(map[string]any)["file_id"].(string)
	up.CloseNow()
	files := call("files", "storage.files.list", map[string]any{})
	if len(files["files"].([]any)) != 1 {
		t.Fatalf("indexed file: %+v", files)
	}
	download := call("down", "storage.download.prepare", map[string]any{"file_id": fileID})
	down := connectStorage(download["transfer_id"].(string), "download")
	defer down.CloseNow()
	var got bytes.Buffer
	for {
		kind, reader, err := down.Reader(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if kind == websocket.MessageBinary {
			if _, err := io.Copy(&got, reader); err != nil {
				t.Fatal(err)
			}
			continue
		}
		b, err := io.ReadAll(reader)
		if err != nil {
			t.Fatal(err)
		}
		var done map[string]any
		if err := json.Unmarshal(b, &done); err != nil {
			t.Fatal(err)
		}
		if done["type"] != "storage.complete" {
			t.Fatalf("download: %s", b)
		}
		break
	}
	if !bytes.Equal(got.Bytes(), content) {
		t.Fatal("download corrupted")
	}
}
