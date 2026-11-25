// Package devicebridge tracks the Backend's control-plane view of its local
// Device Bridge process. It does not own physical-device records.
package devicebridge

import (
	"errors"
	"sync"
	"time"
)

const (
	StatusUnavailable = "unavailable"
	StatusRegistered  = "registered"
	StatusReady       = "ready"
)

var ErrInvalidTransition = errors.New("invalid device bridge state transition")

type Status struct {
	State       string    `json:"state"`
	Connected   bool      `json:"connected"`
	Ready       bool      `json:"ready"`
	RegisteredAt time.Time `json:"registered_at,omitempty"`
	LastSeenAt  time.Time `json:"last_seen_at,omitempty"`
	UpdatedAt   time.Time `json:"updated_at"`
	Reason      string    `json:"reason,omitempty"`
	SessionID  int64     `json:"-"`
}

type Tracker struct {
	mu sync.RWMutex
	status Status
}

func New() *Tracker {
	now := time.Now().UTC()
	return &Tracker{status: Status{State: StatusUnavailable, UpdatedAt: now, Reason: "not_connected"}}
}

func (t *Tracker) Register(sessionID int64) error {
	if sessionID <= 0 { return errors.New("invalid bridge session") }
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	t.status = Status{State: StatusRegistered, Connected: true, RegisteredAt: now, LastSeenAt: now, UpdatedAt: now, SessionID: sessionID}
	return nil
}

func (t *Tracker) Ready(sessionID int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.status.Connected || t.status.SessionID != sessionID { return ErrInvalidTransition }
	now := time.Now().UTC()
	t.status.State, t.status.Ready, t.status.LastSeenAt, t.status.UpdatedAt = StatusReady, true, now, now
	t.status.Reason = ""
	return nil
}

func (t *Tracker) Heartbeat(sessionID int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.status.Connected || t.status.SessionID != sessionID { return ErrInvalidTransition }
	now := time.Now().UTC()
	t.status.LastSeenAt, t.status.UpdatedAt = now, now
	return nil
}

func (t *Tracker) Disconnect(sessionID int64, reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.status.SessionID != sessionID { return }
	now := time.Now().UTC()
	t.status.State, t.status.Connected, t.status.Ready = StatusUnavailable, false, false
	t.status.UpdatedAt, t.status.Reason = now, reason
	t.status.SessionID = 0
}

func (t *Tracker) Unavailable(reason string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now().UTC()
	t.status.State, t.status.Connected, t.status.Ready = StatusUnavailable, false, false
	t.status.SessionID = 0
	t.status.UpdatedAt, t.status.Reason = now, reason
}

func (t *Tracker) Snapshot() Status {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.status
}
