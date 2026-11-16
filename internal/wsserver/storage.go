package wsserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"HomeCloudBackend/internal/storage"
	"github.com/coder/websocket"
)

type storageHello struct {
	V          int    `json:"v"`
	Type       string `json:"type"`
	TransferID string `json:"transfer_id"`
	DeviceID   string `json:"device_id"`
	Role       string `json:"role"`
}

func (s *Server) StoragePath() string {
	return strings.TrimSuffix(s.cfg.Server.WebSocketPath, "/") + "/storage"
}
func (s *Server) HandleStorage(w http.ResponseWriter, r *http.Request) {
	if s.storage == nil {
		http.Error(w, "storage disabled", http.StatusServiceUnavailable)
		return
	}
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		http.Error(w, "bad address", http.StatusBadRequest)
		return
	}
	release, ok := s.clients.Acquire(ip)
	if !ok {
		http.Error(w, "connection limit", http.StatusTooManyRequests)
		return
	}
	defer release()
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.cfg.WebSocket.AllowedOrigins})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	untrack := s.clients.Track(func() { conn.CloseNow() })
	defer untrack()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn.SetReadLimit(2048)
	helloCtx, stop := context.WithTimeout(ctx, 10*time.Second)
	kind, raw, err := conn.Read(helloCtx)
	stop()
	if err != nil || kind != websocket.MessageText {
		_ = conn.Close(websocket.StatusPolicyViolation, "storage.hello required")
		return
	}
	var h storageHello
	if json.Unmarshal(raw, &h) != nil || h.V != 1 || h.Type != "storage.hello" || h.TransferID == "" || h.DeviceID == "" || (h.Role != "upload" && h.Role != "download") {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid storage.hello")
		return
	}
	if !s.clients.IsOnline(h.DeviceID) {
		_ = conn.Close(websocket.StatusPolicyViolation, "device offline")
		return
	}
	t, err := s.storage.Claim(h.TransferID, h.DeviceID, h.Role)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "transfer unavailable")
		return
	}
	defer s.storage.Finish(t)
	conn.SetReadLimit(storage.MaxChunkBytes)
	write := func(obj any) error {
		b, e := json.Marshal(obj)
		if e != nil {
			return e
		}
		wctx, done := context.WithTimeout(ctx, 10*time.Second)
		defer done()
		return conn.Write(wctx, websocket.MessageText, b)
	}
	if err := write(map[string]any{"v": 1, "type": "storage.ready", "transfer_id": t.TransferID, "role": t.Role}); err != nil {
		return
	}
	// A transfer is bound to one dedicated socket, never to the main control WS.
	transferCtx, stopTransfer := context.WithTimeout(ctx, 60*time.Minute)
	defer stopTransfer()
	if t.Role == "upload" {
		info, err := s.storageUpload(transferCtx, conn, t)
		if err != nil {
			_ = write(map[string]any{"v": 1, "type": "storage.error", "error": err.Error()})
			return
		}
		if err := write(map[string]any{"v": 1, "type": "storage.complete", "file": info, "transfer_id": t.TransferID}); err != nil {
			return
		}
		_, _ = s.PublishEvent(context.Background(), "storage.file.created", info)
		_ = conn.Close(websocket.StatusNormalClosure, "storage complete")
		return
	}
	if err := s.storageDownload(transferCtx, conn, t); err != nil {
		_ = write(map[string]any{"v": 1, "type": "storage.error", "error": err.Error()})
		return
	}
	_ = conn.Close(websocket.StatusNormalClosure, "storage complete")
}

func (s *Server) storageUpload(ctx context.Context, conn *websocket.Conn, t *storage.Transfer) (storage.File, error) {
	path := s.storage.TempPath(t)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return storage.File{}, err
	}
	defer os.Remove(path)
	defer f.Close()
	hasher := sha256.New()
	var written int64
	buf := make([]byte, 32*1024)
	for {
		kind, r, err := conn.Reader(ctx)
		if err != nil {
			return storage.File{}, err
		}
		if kind == websocket.MessageText {
			var end struct {
				V      int    `json:"v"`
				Type   string `json:"type"`
				SHA256 string `json:"sha256"`
			}
			b, err := io.ReadAll(io.LimitReader(r, 512))
			if err != nil || json.Unmarshal(b, &end) != nil || end.V != 1 || end.Type != "storage.end" {
				return storage.File{}, errors.New("invalid storage.end")
			}
			sum := hex.EncodeToString(hasher.Sum(nil))
			if t.SizeBytes != written {
				return storage.File{}, fmt.Errorf("size mismatch: expected %d got %d", t.SizeBytes, written)
			}
			if end.SHA256 != "" && end.SHA256 != sum {
				return storage.File{}, errors.New("SHA256 mismatch")
			}
			if err := f.Sync(); err != nil {
				return storage.File{}, err
			}
			if err := f.Close(); err != nil {
				return storage.File{}, err
			}
			return s.storage.CommitUpload(ctx, t, sum)
		}
		if kind != websocket.MessageBinary {
			return storage.File{}, errors.New("binary data expected")
		}
		capRemaining := t.SizeBytes - written
		n, err := io.CopyBuffer(io.MultiWriter(f, hasher), io.LimitReader(r, capRemaining), buf)
		written += n
		if err != nil {
			return storage.File{}, err
		}
		var excess [1]byte
		extra, readErr := r.Read(excess[:])
		if extra > 0 {
			return storage.File{}, errors.New("transfer exceeds declared size")
		}
		if readErr != nil && readErr != io.EOF {
			return storage.File{}, readErr
		}
	}
}

func (s *Server) storageDownload(ctx context.Context, conn *websocket.Conn, t *storage.Transfer) error {
	f, err := os.Open(t.SourcePath)
	if err != nil {
		return err
	}
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() != t.File.SizeBytes {
		return errors.New("stored file size mismatch")
	}
	h := sha256.New()
	buf := make([]byte, 64*1024)
	var count int64
	for {
		n, readErr := f.Read(buf)
		if n > 0 {
			if _, err := h.Write(buf[:n]); err != nil {
				return err
			}
			if err := conn.Write(ctx, websocket.MessageBinary, buf[:n]); err != nil {
				return err
			}
			count += int64(n)
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	sum := hex.EncodeToString(h.Sum(nil))
	if t.File.SHA256 != "" && sum != t.File.SHA256 {
		return errors.New("stored file SHA256 mismatch")
	}
	data, _ := json.Marshal(map[string]any{"v": 1, "type": "storage.complete", "transfer_id": t.TransferID, "bytes": count, "sha256": sum, "file": t.File})
	return conn.Write(ctx, websocket.MessageText, data)
}
