// Package streaming manages one-to-one, in-memory binary transfer sessions.
// Data is relayed via WebSocket Reader/Writer without buffering whole files.
// A stream owns TWO separate WS connections: it never blocks the control WS.
package streaming

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/coder/websocket"
)

const (
	MaxStreams                = 32
	MaxStreamsPerDevice       = 4
	MaxMessageBytes     int64 = 1 << 20 // stream WS frames/messages, not control JSON
	MaxStreamBytes      int64 = 8 << 30 // 8 GiB per transfer; metadata size=0 is unknown
	OfferTTL                  = 60 * time.Second
)

var (
	ErrNotFound        = errors.New("stream not found")
	ErrNotParticipant  = errors.New("device is not a stream participant")
	ErrAlreadyAttached = errors.New("stream endpoint already attached")
	ErrBusy            = errors.New("too many active streams")
	ErrUnavailable     = errors.New("target device not connected")
	ErrStreamClosed    = errors.New("stream closed")
)

type CreateRequest struct {
	Target    string `json:"target"`
	Name      string `json:"name,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type Info struct {
	StreamID  string `json:"stream_id"`
	From      string `json:"from"`
	Target    string `json:"target"`
	Name      string `json:"name,omitempty"`
	MIMEType  string `json:"mime_type,omitempty"`
	SizeBytes int64  `json:"size_bytes"`
	Path      string `json:"path"`
	Status    string `json:"status"`
	CreatedAt string `json:"created_at"`
}

type Result struct {
	StreamID string `json:"stream_id"`
	Bytes    int64  `json:"bytes"`
	SHA256   string `json:"sha256,omitempty"`
	Status   string `json:"status"`
	Error    string `json:"error,omitempty"`
}

type Session struct {
	Info                       Info
	sender                     *websocket.Conn
	receiver                   *websocket.Conn
	senderReady, receiverReady bool
	attached                   chan struct{}
	ready                      chan struct{}
	done                       chan struct{}
	timer                      *time.Timer
	finished                   bool
}

func (s *Session) Sender() *websocket.Conn   { return s.sender }
func (s *Session) Receiver() *websocket.Conn { return s.receiver }
func (s *Session) Attached() <-chan struct{} { return s.attached }
func (s *Session) Ready() <-chan struct{}    { return s.ready }
func (s *Session) Done() <-chan struct{}     { return s.done }

// Manager never authenticates identities: as with /ws hello, device IDs are
// trusted-network routing labels, not credentials.
type Manager struct {
	mu       sync.Mutex
	sessions map[string]*Session
	online   func(string) bool
	onFinish func(Info, Result)
	closed   bool
}

func New(online func(string) bool, onFinish func(Info, Result)) *Manager {
	return &Manager{sessions: make(map[string]*Session), online: online, onFinish: onFinish}
}

func (m *Manager) Create(source string, req CreateRequest, path string) (Info, error) {
	if source == "" || req.Target == "" || req.Target == source || len(req.Name) > 255 ||
		len(req.MIMEType) > 128 || req.SizeBytes < 0 || req.SizeBytes > MaxStreamBytes {
		return Info{}, errors.New("invalid stream parameters")
	}
	if m.online == nil || !m.online(source) || !m.online(req.Target) {
		return Info{}, ErrUnavailable
	}
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return Info{}, err
	}
	id := hex.EncodeToString(random[:])
	info := Info{StreamID: id, From: source, Target: req.Target, Name: req.Name, MIMEType: req.MIMEType,
		SizeBytes: req.SizeBytes, Path: path, Status: "waiting", CreatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return Info{}, ErrStreamClosed
	}
	if len(m.sessions) >= MaxStreams {
		return Info{}, ErrBusy
	}
	countSource, countTarget := 0, 0
	for _, s := range m.sessions {
		if s.Info.From == source || s.Info.Target == source {
			countSource++
		}
		if s.Info.From == req.Target || s.Info.Target == req.Target {
			countTarget++
		}
	}
	if countSource >= MaxStreamsPerDevice || countTarget >= MaxStreamsPerDevice {
		return Info{}, ErrBusy
	}
	s := &Session{Info: info, attached: make(chan struct{}), ready: make(chan struct{}), done: make(chan struct{})}
	m.sessions[id] = s
	s.timer = time.AfterFunc(OfferTTL, func() { m.end(s, Result{StreamID: id, Status: "failed", Error: "stream attach timeout"}) })
	return info, nil
}

func (m *Manager) List(deviceID string) []Info {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]Info, 0)
	for _, s := range m.sessions {
		if s.Info.From == deviceID || s.Info.Target == deviceID {
			result = append(result, s.Info)
		}
	}
	return result
}

func (m *Manager) Attach(id, deviceID, role string, conn *websocket.Conn) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.sessions[id]
	if !ok || s.finished {
		return nil, ErrNotFound
	}
	if (role != "sender" || deviceID != s.Info.From) && (role != "receiver" || deviceID != s.Info.Target) {
		return nil, ErrNotParticipant
	}
	if m.online == nil || !m.online(deviceID) {
		return nil, ErrUnavailable
	}
	if role == "sender" {
		if s.sender != nil {
			return nil, ErrAlreadyAttached
		}
		s.sender = conn
	} else {
		if s.receiver != nil {
			return nil, ErrAlreadyAttached
		}
		s.receiver = conn
	}
	if s.sender != nil && s.receiver != nil {
		s.Info.Status = "attached"
		close(s.attached)
	}
	return s, nil
}

// MarkReady must be called only after the server has WRITTEN the stream.ready
// control frame to that endpoint, preventing concurrent control/binary writes.
func (m *Manager) MarkReady(s *Session, role string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.finished {
		return ErrStreamClosed
	}
	if s.sender == nil || s.receiver == nil {
		return errors.New("both peers are not attached")
	}
	if role == "sender" {
		s.senderReady = true
	} else if role == "receiver" {
		s.receiverReady = true
	} else {
		return errors.New("invalid stream role")
	}
	if s.senderReady && s.receiverReady {
		s.Info.Status = "streaming"
		s.timer.Stop()
		close(s.ready)
	}
	return nil
}

func (m *Manager) Cancel(deviceID, id string) error {
	m.mu.Lock()
	s := m.sessions[id]
	if s == nil {
		m.mu.Unlock()
		return ErrNotFound
	}
	if s.Info.From != deviceID && s.Info.Target != deviceID {
		m.mu.Unlock()
		return ErrNotParticipant
	}
	m.mu.Unlock()
	m.end(s, Result{StreamID: id, Status: "cancelled", Error: "cancelled by " + deviceID})
	return nil
}

func (m *Manager) CancelDevice(deviceID string) {
	m.mu.Lock()
	var active []*Session
	for _, s := range m.sessions {
		if s.Info.From == deviceID || s.Info.Target == deviceID {
			active = append(active, s)
		}
	}
	m.mu.Unlock()
	for _, s := range active {
		m.end(s, Result{StreamID: s.Info.StreamID, Status: "cancelled", Error: "control connection closed"})
	}
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	m.closed = true
	active := make([]*Session, 0, len(m.sessions))
	for _, s := range m.sessions {
		active = append(active, s)
	}
	m.mu.Unlock()
	for _, s := range active {
		m.end(s, Result{StreamID: s.Info.StreamID, Status: "cancelled", Error: "server shutdown"})
	}
}

func (m *Manager) end(s *Session, result Result) {
	m.mu.Lock()
	if s.finished {
		m.mu.Unlock()
		return
	}
	s.finished = true
	s.timer.Stop()
	delete(m.sessions, s.Info.StreamID)
	close(s.done)
	info := s.Info
	info.Status = result.Status
	from, to := s.sender, s.receiver
	callback := m.onFinish
	m.mu.Unlock()
	if from != nil {
		from.CloseNow()
	}
	if to != nil {
		to.CloseNow()
	}
	if callback != nil {
		callback(info, result)
	}
}

func (m *Manager) Finish(s *Session, result Result) { m.end(s, result) }

// Relay transfers each BINARY WS message to the receiver as one BINARY WS
// message while copying via io.CopyBuffer (constant-memory). The source closes
// a stream by sending {"v":1,"type":"stream.end"} on the stream socket.
func Relay(ctx context.Context, sender, receiver *websocket.Conn, expectedBytes int64) (Result, error) {
	result := Result{Status: "completed"}
	hash := sha256.New()
	buf := make([]byte, 32*1024)
	for {
		kind, input, err := sender.Reader(ctx)
		if err != nil {
			return result, fmt.Errorf("sender disconnected before stream.end: %w", err)
		}
		if kind == websocket.MessageText {
			b, e := io.ReadAll(io.LimitReader(input, 256))
			var end struct {
				V    int    `json:"v"`
				Type string `json:"type"`
			}
			if e != nil || json.Unmarshal(b, &end) != nil || end.V != 1 || end.Type != "stream.end" {
				return result, errors.New("expected binary data or stream.end")
			}
			break
		}
		if kind != websocket.MessageBinary {
			return result, errors.New("non-binary stream payload")
		}
		output, err := receiver.Writer(ctx, websocket.MessageBinary)
		if err != nil {
			return result, fmt.Errorf("open receiver: %w", err)
		}
		remaining := MaxStreamBytes - result.Bytes
		if expectedBytes > 0 && expectedBytes-result.Bytes < remaining {
			remaining = expectedBytes - result.Bytes
		}
		capped := io.LimitReader(input, remaining)
		n, copyErr := io.CopyBuffer(io.MultiWriter(output, hash), capped, buf)
		result.Bytes += n
		closeErr := output.Close()
		// A message exceeding the configured total is an error even if only
		// one byte remained; do not relay that overflowing byte.
		var extra [1]byte
		nExtra, extraErr := input.Read(extra[:])
		if nExtra > 0 {
			return result, errors.New("stream byte limit exceeded")
		}
		if extraErr != nil && extraErr != io.EOF {
			return result, extraErr
		}
		if copyErr != nil {
			return result, fmt.Errorf("copy stream chunk: %w", copyErr)
		}
		if closeErr != nil {
			return result, fmt.Errorf("flush receiver chunk: %w", closeErr)
		}
	}
	if expectedBytes > 0 && result.Bytes != expectedBytes {
		return result, fmt.Errorf("stream size mismatch: expected %d got %d", expectedBytes, result.Bytes)
	}
	result.SHA256 = hex.EncodeToString(hash.Sum(nil))
	return result, nil
}
