package wsserver

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"

	"HomeCloudBackend/internal/streaming"
	"github.com/coder/websocket"
)

type streamHello struct {
	V        int    `json:"v"`
	Type     string `json:"type"`
	StreamID string `json:"stream_id"`
	DeviceID string `json:"device_id"`
	Role     string `json:"role"`
}

func (s *Server) HandleStream(w http.ResponseWriter, r *http.Request) {
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
	// Never read a stream ID from an untrusted URL or use it as authentication.
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: s.cfg.WebSocket.AllowedOrigins})
	if err != nil {
		s.log.Warn("stream WS upgrade rejected", "error", err)
		return
	}
	defer conn.CloseNow()
	untrack := s.clients.Track(func() { conn.CloseNow() })
	defer untrack()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn.SetReadLimit(2048) // first hello must be small
	timeout := s.cfg.WebSocket.HelloTimeoutSeconds
	if timeout <= 0 {
		timeout = 10
	}
	firstCtx, stop := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	kind, data, err := conn.Read(firstCtx)
	stop()
	if err != nil || kind != websocket.MessageText {
		_ = conn.Close(websocket.StatusPolicyViolation, "stream.hello required")
		return
	}
	var hello streamHello
	if json.Unmarshal(data, &hello) != nil || hello.V != 1 || hello.Type != "stream.hello" ||
		hello.StreamID == "" || hello.DeviceID == "" || (hello.Role != "sender" && hello.Role != "receiver") {
		_ = conn.Close(websocket.StatusPolicyViolation, "invalid stream.hello")
		return
	}
	session, err := s.streams.Attach(hello.StreamID, hello.DeviceID, hello.Role, conn)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, err.Error())
		return
	}
	defer func() {
		select {
		case <-session.Done():
		default:
			_ = s.streams.Cancel(hello.DeviceID, hello.StreamID)
		}
	}()
	conn.SetReadLimit(streaming.MaxMessageBytes)
	write := func(payload any) error {
		d, e := json.Marshal(payload)
		if e != nil {
			return e
		}
		t := s.cfg.WebSocket.WriteTimeoutSeconds
		if t <= 0 {
			t = 10
		}
		writeCtx, cancelWrite := context.WithTimeout(ctx, time.Duration(t)*time.Second)
		defer cancelWrite()
		return conn.Write(writeCtx, websocket.MessageText, d)
	}
	if err := write(map[string]any{"v": 1, "type": "stream.attached", "stream_id": hello.StreamID, "role": hello.Role}); err != nil {
		return
	}
	select {
	case <-session.Attached():
	case <-session.Done():
		return
	}
	if err := write(map[string]any{"v": 1, "type": "stream.ready", "stream_id": hello.StreamID}); err != nil {
		return
	}
	if err := s.streams.MarkReady(session, hello.Role); err != nil {
		return
	}
	select {
	case <-session.Ready():
	case <-session.Done():
		return
	}
	if hello.Role == "receiver" {
		<-session.Done()
		return
	}
	// Only the sender's handler reads from sender. Only that handler writes
	// binary to receiver. No concurrent writes happen on the stream sockets.
	result, relayErr := streaming.Relay(ctx, session.Sender(), session.Receiver(), session.Info.SizeBytes)
	result.StreamID = session.Info.StreamID
	if relayErr != nil {
		result.Status = "failed"
		result.Error = relayErr.Error()
	}
	if relayErr == nil {
		completion := map[string]any{"v": 1, "type": "stream.complete", "stream_id": result.StreamID,
			"bytes": result.Bytes, "sha256": result.SHA256}
		if err := writeStreamCompletion(ctx, session.Receiver(), completion); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("receiver completion: %v", err)
		}
		if err := write(completion); err != nil {
			result.Status = "failed"
			result.Error = fmt.Sprintf("sender completion: %v", err)
		}
	}
	// Write the close control frames before destroying transport state.
	if result.Status == "completed" {
		done := make(chan struct{})
		go func() { _ = session.Receiver().Close(websocket.StatusNormalClosure, "stream completed"); close(done) }()
		_ = conn.Close(websocket.StatusNormalClosure, "stream completed")
		select {
		case <-done:
		case <-time.After(2 * time.Second):
		}
	}
	s.streams.Finish(session, result)
}

func writeStreamCompletion(ctx context.Context, conn *websocket.Conn, value any) error {
	b, err := json.Marshal(value)
	if err != nil {
		return err
	}
	writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return conn.Write(writeCtx, websocket.MessageText, b)
}

// The stream control plane does not expose any arbitrary file path. The
// receiver application interprets bytes; the server only relays them.
