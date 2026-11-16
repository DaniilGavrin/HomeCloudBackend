// Package connections owns connected client routing labels, admission limits,
// and outgoing JSON delivery. It deliberately knows nothing about WebSocket.
package connections

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"HomeCloudBackend/internal/commands"
)

var (
	ErrDisconnected = errors.New("device not connected")
	ErrDuplicateID  = errors.New("device_id already connected")
	ErrQueueFull    = errors.New("device outbound queue full")
	ErrClosed       = errors.New("connection closed")
)

var deviceIDPattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.:-]{0,127}$`)
var clientTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

func ValidDeviceID(id string) bool     { return deviceIDPattern.MatchString(id) }
func ValidClientType(kind string) bool { return clientTypePattern.MatchString(kind) }

// Sender is the only transport-specific operation the Connection Manager needs.
// The wsserver supplies an implementation backed by websocket.Conn.Write.
type Sender func(context.Context, []byte) error

type outbound struct {
	bytes   []byte
	receipt chan error // nil for enqueue-only delivery
}

// Peer owns ONE outbound writer. No other goroutine should write directly to
// the underlying WebSocket connection. A slow client has a bounded queue.
type Peer struct {
	DeviceID       string
	ClientType     string
	ConnectedAt    time.Time
	sender         Sender
	closeTransport func()
	writeTimeout   time.Duration
	queue          chan outbound
	done           chan struct{}
	mu             sync.Mutex
	closed         bool
	runOnce        sync.Once
}

func NewPeer(id, kind string, queueSize int, writeTimeout time.Duration, sender Sender, closeTransport func()) *Peer {
	if queueSize < 1 {
		queueSize = 1
	}
	if writeTimeout <= 0 {
		writeTimeout = 10 * time.Second
	}
	return &Peer{DeviceID: id, ClientType: kind, ConnectedAt: time.Now().UTC(),
		sender: sender, closeTransport: closeTransport, writeTimeout: writeTimeout,
		queue: make(chan outbound, queueSize), done: make(chan struct{})}
}

func (p *Peer) enqueue(ctx context.Context, msg commands.Outbound, receipt chan error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if msg.V == 0 {
		msg.V = 1
	}
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("encode outbound: %w", err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case p.queue <- outbound{bytes: data, receipt: receipt}:
		return nil
	default:
		return ErrQueueFull
	}
}

// QueueJSON acknowledges only enqueueing, not writing to a socket and not
// remote-side execution. Used by clients.send.
func (p *Peer) QueueJSON(ctx context.Context, msg commands.Outbound) error {
	return p.enqueue(ctx, msg, nil)
}

// SendJSON waits for transport write to complete. Used for control responses.
func (p *Peer) SendJSON(ctx context.Context, msg commands.Outbound) error {
	receipt := make(chan error, 1)
	if err := p.enqueue(ctx, msg, receipt); err != nil {
		return err
	}
	select {
	case err := <-receipt:
		return err
	case <-p.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RunWriter must run once per peer. On transport failure it closes the socket
// so the wsserver's Read can exit and unregister the peer.
func (p *Peer) RunWriter(ctx context.Context) {
	p.runOnce.Do(func() {
		defer p.Close()
		for {
			select {
			case <-p.done:
				return
			case <-ctx.Done():
				return
			case item := <-p.queue:
				writeCtx, cancel := context.WithTimeout(ctx, p.writeTimeout)
				err := p.sender(writeCtx, item.bytes)
				cancel()
				if item.receipt != nil {
					item.receipt <- err
				}
				if err != nil {
					return
				}
			}
		}
	})
}

func (p *Peer) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	close(p.done)
	p.mu.Unlock()
	if p.closeTransport != nil {
		p.closeTransport()
	}
}

// Manager stores active peers and transport admission state separately. The
// registry holds routing labels only; it does not authenticate those labels.
type Manager struct {
	mu                 sync.RWMutex
	peers              map[string]*Peer
	byIP               map[string]int
	live               int
	maxTotal, maxPerIP int
	closers            map[uint64]func()
	nextID             uint64
	closed             bool
}

func NewManager(maxTotal, maxPerIP int) *Manager {
	return &Manager{peers: make(map[string]*Peer), byIP: make(map[string]int),
		closers: make(map[uint64]func()), maxTotal: maxTotal, maxPerIP: maxPerIP}
}

// Acquire reserves a transport slot before websocket.Accept. The release
// function is idempotent and must be deferred by the handler.
func (m *Manager) Acquire(ip string) (func(), bool) {
	m.mu.Lock()
	if m.closed || (m.maxTotal > 0 && m.live >= m.maxTotal) ||
		(m.maxPerIP > 0 && m.byIP[ip] >= m.maxPerIP) {
		m.mu.Unlock()
		return nil, false
	}
	m.byIP[ip]++
	m.live++
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			defer m.mu.Unlock()
			m.byIP[ip]--
			m.live--
			if m.byIP[ip] == 0 {
				delete(m.byIP, ip)
			}
		})
	}, true
}

// Track includes pre-hello sockets in shutdown. The returned function removes
// this socket from the shutdown registry.
func (m *Manager) Track(closeFn func()) func() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		closeFn()
		return func() {}
	}
	m.nextID++
	id := m.nextID
	m.closers[id] = closeFn
	m.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { m.mu.Lock(); delete(m.closers, id); m.mu.Unlock() }) }
}

func (m *Manager) Register(p *Peer) error {
	if !ValidDeviceID(p.DeviceID) || !ValidClientType(p.ClientType) {
		return errors.New("invalid device identity")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	if _, ok := m.peers[p.DeviceID]; ok {
		return ErrDuplicateID
	}
	m.peers[p.DeviceID] = p
	return nil
}

// Unregister does not remove a NEW session if an older connection closes late.
func (m *Manager) Unregister(p *Peer) {
	m.mu.Lock()
	if m.peers[p.DeviceID] == p {
		delete(m.peers, p.DeviceID)
	}
	m.mu.Unlock()
	p.Close()
}

func (m *Manager) IsOnline(id string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.peers[id]
	return ok
}

func (m *Manager) Online() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.peers)
}

func (m *Manager) List() []map[string]string {
	m.mu.RLock()
	out := make([]map[string]string, 0, len(m.peers))
	for _, p := range m.peers {
		out = append(out, map[string]string{"device_id": p.DeviceID, "client_type": p.ClientType})
	}
	m.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i]["device_id"] < out[j]["device_id"] })
	return out
}

func (m *Manager) find(id string) (*Peer, error) {
	m.mu.RLock()
	p := m.peers[id]
	m.mu.RUnlock()
	if p == nil {
		return nil, ErrDisconnected
	}
	return p, nil
}

func (m *Manager) QueueToDevice(ctx context.Context, id string, msg commands.Outbound) error {
	p, err := m.find(id)
	if err != nil {
		return err
	}
	return p.QueueJSON(ctx, msg)
}

func (m *Manager) SendToDevice(ctx context.Context, id string, msg commands.Outbound) error {
	p, err := m.find(id)
	if err != nil {
		return err
	}
	return p.SendJSON(ctx, msg)
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	m.closed = true
	peers := make([]*Peer, 0, len(m.peers))
	for _, p := range m.peers {
		peers = append(peers, p)
	}
	closers := make([]func(), 0, len(m.closers))
	for _, closeFn := range m.closers {
		closers = append(closers, closeFn)
	}
	m.mu.Unlock()
	for _, p := range peers {
		p.Close()
	}
	for _, closeFn := range closers {
		closeFn()
	}
}
