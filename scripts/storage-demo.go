//go:build ignore

// Run from project root:
//
//	go run ./scripts/storage-demo.go -file "C:\\Temp\\document.pdf"
//
// An explicitly registered storage volume is required before upload.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/coder/websocket"
)

type msg struct {
	V          int    `json:"v"`
	ID         string `json:"id,omitempty"`
	Type       string `json:"type"`
	Action     string `json:"action,omitempty"`
	DeviceID   string `json:"device_id,omitempty"`
	ClientType string `json:"client_type,omitempty"`
	Data       any    `json:"data,omitempty"`
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[ERROR] "+format+"\n", args...)
	os.Exit(1)
}
func writeJSON(ctx context.Context, c *websocket.Conn, v any) {
	b, e := json.Marshal(v)
	if e != nil {
		fatal("JSON: %v", e)
	}
	if e = c.Write(ctx, websocket.MessageText, b); e != nil {
		fatal("write: %v", e)
	}
}
func readJSON(ctx context.Context, c *websocket.Conn) map[string]any {
	k, b, e := c.Read(ctx)
	if e != nil {
		fatal("read: %v", e)
	}
	if k != websocket.MessageText {
		fatal("unexpected frame type %v", k)
	}
	var out map[string]any
	if e = json.Unmarshal(b, &out); e != nil {
		fatal("JSON response %s: %v", b, e)
	}
	if out["type"] == "error" || out["type"] == "storage.error" {
		fatal("server: %v", out["error"])
	}
	return out
}
func main() {
	endpoint := flag.String("ws", "ws://127.0.0.1:8080/ws", "control WebSocket URL")
	input := flag.String("file", "", "file to upload then download and verify")
	volume := flag.String("storage", "auto", "existing registered storage_id or auto")
	flag.Parse()
	if *input == "" {
		fatal("usage: go run ./scripts/storage-demo.go -file C:\\Temp\\document.pdf")
	}
	data, err := os.Open(*input)
	if err != nil {
		fatal("open: %v", err)
	}
	defer data.Close()
	stat, err := data.Stat()
	if err != nil {
		fatal("stat: %v", err)
	}
	if stat.Size() < 0 {
		fatal("invalid file size")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	c, _, err := websocket.Dial(ctx, *endpoint, nil)
	if err != nil {
		fatal("connect: %v", err)
	}
	defer c.CloseNow()
	identity := fmt.Sprintf("storage-demo-%d", time.Now().UnixNano())
	writeJSON(ctx, c, msg{V: 1, Type: "hello", ID: "hello-1", DeviceID: identity, ClientType: "desktop"})
	if out := readJSON(ctx, c); out["type"] != "identified" {
		fatal("hello: %v", out)
	}
	call := func(action string, payload any) map[string]any {
		writeJSON(ctx, c, msg{V: 1, ID: "req-" + strings.ReplaceAll(action, ".", "-"), Type: "command", Action: action, Data: payload})
		out := readJSON(ctx, c)
		if out["type"] != "result" {
			fatal("%s: %v", action, out)
		}
		return out["data"].(map[string]any)
	}
	p := call("storage.upload.prepare", map[string]any{"name": filepath.Base(*input), "mime_type": "application/octet-stream", "size_bytes": stat.Size(), "storage_id": *volume})
	streamURL := strings.TrimSuffix(*endpoint, "/ws") + "/ws/storage"
	attach := func(id, role string) *websocket.Conn {
		ws, _, e := websocket.Dial(ctx, streamURL, nil)
		if e != nil {
			fatal("storage socket: %v", e)
		}
		ws.SetReadLimit(1 << 20)
		writeJSON(ctx, ws, map[string]any{"v": 1, "type": "storage.hello", "transfer_id": id, "device_id": identity, "role": role})
		out := readJSON(ctx, ws)
		if out["type"] != "storage.ready" {
			fatal("storage.ready: %v", out)
		}
		return ws
	}
	uploading := attach(p["transfer_id"].(string), "upload")
	h := sha256.New()
	buf := make([]byte, 64*1024)
	var total int64
	for {
		n, e := data.Read(buf)
		if n > 0 {
			_, _ = h.Write(buf[:n])
			if e := uploading.Write(ctx, websocket.MessageBinary, buf[:n]); e != nil {
				fatal("upload chunk: %v", e)
			}
			total += int64(n)
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			fatal("file read: %v", e)
		}
	}
	sum := hex.EncodeToString(h.Sum(nil))
	writeJSON(ctx, uploading, map[string]any{"v": 1, "type": "storage.end", "sha256": sum})
	uploadResult := readJSON(ctx, uploading)
	if uploadResult["type"] != "storage.complete" {
		fatal("upload completion: %v", uploadResult)
	}
	uploading.CloseNow()
	fileID := uploadResult["file"].(map[string]any)["file_id"].(string)
	fmt.Printf("[UPLOADED] %d bytes | file_id=%s | sha256=%s\n", total, fileID, sum)
	p = call("storage.download.prepare", map[string]any{"file_id": fileID})
	downloading := attach(p["transfer_id"].(string), "download")
	defer downloading.CloseNow()
	hash := sha256.New()
	var downBytes int64
	for {
		kind, reader, e := downloading.Reader(ctx)
		if e != nil {
			fatal("download reader: %v", e)
		}
		if kind == websocket.MessageBinary {
			n, e := io.Copy(hash, reader)
			if e != nil {
				fatal("download chunk: %v", e)
			}
			downBytes += n
			continue
		}
		payload, e := io.ReadAll(reader)
		if e != nil {
			fatal("download metadata: %v", e)
		}
		var result map[string]any
		if e = json.Unmarshal(payload, &result); e != nil {
			fatal("download JSON: %v", e)
		}
		if result["type"] != "storage.complete" {
			fatal("download response: %s", payload)
		}
		break
	}
	if downBytes != total || !bytes.Equal(hash.Sum(nil), h.Sum(nil)) {
		fatal("checksum mismatch: received=%d expected=%d", downBytes, total)
	}
	fmt.Printf("[OK] Download verified: %d bytes, SHA-256 %s\n", downBytes, sum)
}
