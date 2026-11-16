// Package roomcontrol tracks one serialized playback control operation per room.
// Delivery is not execution. TV and Gateway acknowledge only after applying it.
package roomcontrol

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrInvalid  = errors.New("INVALID_CONTROL")
	ErrStale    = errors.New("STALE_CONTROL")
	ErrConflict = errors.New("CONTROL_CONFLICT")
)

const Timeout = 12 * time.Second

type Command struct {
	CommandID       string `json:"command_id"`
	RoomID          string `json:"room_id"`
	PlaybackID      string `json:"playback_id"`
	Generation      uint32 `json:"generation"`
	ConnectionEpoch uint64 `json:"connection_epoch"`
	CommandSequence uint64 `json:"command_sequence"`
	Action          string `json:"action"`
	Volume          *int   `json:"volume,omitempty"`
	Muted           *bool  `json:"muted,omitempty"`
	Origin          string `json:"origin"`
}

type Record struct {
	Command        Command   `json:"command"`
	Status         string    `json:"status"`
	TVApplied      bool      `json:"tv_applied"`
	GatewayApplied bool      `json:"gateway_applied"`
	ErrorCode      string    `json:"error_code,omitempty"`
	Deadline       time.Time `json:"-"`
}

type Ack struct {
	CommandID       string `json:"command_id"`
	RoomID          string `json:"room_id"`
	PlaybackID      string `json:"playback_id"`
	Generation      uint32 `json:"generation"`
	ConnectionEpoch uint64 `json:"connection_epoch"`
	CommandSequence uint64 `json:"command_sequence"`
	Participant     string `json:"participant"`
	Phase           string `json:"phase"`
	ErrorCode       string `json:"error_code,omitempty"`
}

type row struct {
	latest   Record
	sequence uint64
	// Bounded history; once full, refuse new work rather than silently reuse old IDs.
	history map[string]Record
}
type Manager struct {
	mu    sync.Mutex
	rooms map[string]*row
}

func New() *Manager { return &Manager{rooms: make(map[string]*row)} }

func validID(v string) bool {
	if len(v) == 0 || len(v) > 128 {
		return false
	}
	for i := 0; i < len(v); i++ {
		c := v[i]
		if !(c >= 'a' && c <= 'z') && !(c >= 'A' && c <= 'Z') && !(c >= '0' && c <= '9') && c != '-' && c != '_' && c != '.' && c != ':' {
			return false
		}
	}
	return true
}
func needsGateway(action string) bool {
	return action == "pause" || action == "resume" || action == "play"
}
func ValidCommand(q Command) bool {
	if !validID(q.CommandID) || !validID(q.RoomID) || !validID(q.PlaybackID) || q.Generation == 0 || q.ConnectionEpoch == 0 || (q.Origin != "ui" && q.Origin != "tv") {
		return false
	}
	switch q.Action {
	case "pause", "resume", "play":
		return q.Volume == nil && q.Muted == nil
	case "volume":
		return q.Volume != nil && *q.Volume >= 0 && *q.Volume <= 100 && q.Muted == nil
	case "mute":
		return q.Muted != nil && q.Volume == nil
	default:
		return false
	}
}

func equalCommand(a, b Command) bool {
	if a.CommandID != b.CommandID || a.RoomID != b.RoomID || a.PlaybackID != b.PlaybackID || a.Generation != b.Generation || a.ConnectionEpoch != b.ConnectionEpoch || a.Action != b.Action || a.Origin != b.Origin {
		return false
	}
	if (a.Volume == nil) != (b.Volume == nil) || (a.Muted == nil) != (b.Muted == nil) {
		return false
	}
	if a.Volume != nil && *a.Volume != *b.Volume {
		return false
	}
	if a.Muted != nil && *a.Muted != *b.Muted {
		return false
	}
	return true
}

func (m *Manager) Issue(q Command, now time.Time) (Record, bool, error) {
	if !ValidCommand(q) || now.IsZero() {
		return Record{}, false, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.rooms[q.RoomID]
	if r == nil {
		r = &row{history: make(map[string]Record)}
		m.rooms[q.RoomID] = r
	}
	if previous, exists := r.history[q.CommandID]; exists {
		if !equalCommand(previous.Command, q) {
			return Record{}, false, ErrConflict
		}
		return previous, false, nil
	}
	if r.sequence == ^uint64(0) {
		return Record{}, false, ErrConflict
	}
	if r.latest.Status == "PENDING" || r.latest.Status == "DELIVERED" {
		previous := r.latest
		previous.Status = "SUPERSEDED"
		r.history[previous.Command.CommandID] = previous
	}
	if len(r.history) >= 8192 {
		return Record{}, false, ErrConflict
	}
	r.sequence++
	q.CommandSequence = r.sequence
	record := Record{Command: q, Status: "PENDING", TVApplied: q.Origin == "tv", Deadline: now.Add(Timeout)}
	if q.Origin == "tv" && !needsGateway(q.Action) {
		record.Status = "EXECUTED"
	}
	r.latest = record
	r.history[q.CommandID] = record
	return record, true, nil
}

func (m *Manager) MarkDelivered(roomID, commandID string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.rooms[roomID]
	if r == nil || r.latest.Command.CommandID != commandID {
		return Record{}, ErrStale
	}
	if r.latest.Status == "PENDING" {
		r.latest.Status = "DELIVERED"
		r.history[commandID] = r.latest
	}
	return r.latest, nil
}
func (m *Manager) Fail(roomID, commandID, code string) (Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.rooms[roomID]
	if r == nil || r.latest.Command.CommandID != commandID {
		return Record{}, ErrStale
	}
	if r.latest.Status == "EXECUTED" {
		return Record{}, ErrStale
	}
	r.latest.Status = "FAILED"
	r.latest.ErrorCode = code
	r.history[commandID] = r.latest
	return r.latest, nil
}
func (m *Manager) Ack(q Ack) (Record, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.rooms[q.RoomID]
	if r == nil {
		return Record{}, false, ErrStale
	}
	rec := r.latest
	c := rec.Command
	if c.CommandID != q.CommandID || c.PlaybackID != q.PlaybackID || c.Generation != q.Generation || c.ConnectionEpoch != q.ConnectionEpoch || c.CommandSequence != q.CommandSequence || (q.Participant != "tv" && q.Participant != "gateway") {
		return Record{}, false, ErrStale
	}
	if q.Participant == "gateway" && !needsGateway(c.Action) {
		return Record{}, false, ErrInvalid
	}
	if (q.Phase != "APPLIED" && q.Phase != "FAILED") || (q.Phase == "APPLIED" && q.ErrorCode != "") {
		return Record{}, false, ErrInvalid
	}
	if rec.Status == "FAILED" || rec.Status == "SUPERSEDED" {
		return Record{}, false, ErrStale
	}
	if rec.Status == "EXECUTED" {
		return rec, false, nil
	}
	if q.Phase == "FAILED" {
		rec.Status = "FAILED"
		rec.ErrorCode = "EXECUTION_FAILED"
		if q.ErrorCode != "" {
			if len(q.ErrorCode) > 64 {
				return Record{}, false, ErrInvalid
			}
			rec.ErrorCode = q.ErrorCode
		}
	} else {
		if q.Participant == "tv" {
			if rec.TVApplied {
				return rec, false, nil
			}
			rec.TVApplied = true
		} else {
			if rec.GatewayApplied {
				return rec, false, nil
			}
			rec.GatewayApplied = true
		}
		if rec.TVApplied && (!needsGateway(c.Action) || rec.GatewayApplied) {
			rec.Status = "EXECUTED"
		}
	}
	r.latest = rec
	r.history[c.CommandID] = rec
	return rec, true, nil
}
func (m *Manager) Expire(now time.Time) []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	var expired []Record
	for _, r := range m.rooms {
		if (r.latest.Status == "PENDING" || r.latest.Status == "DELIVERED") && now.After(r.latest.Deadline) {
			r.latest.Status = "FAILED"
			r.latest.ErrorCode = "CONTROL_TIMEOUT"
			r.history[r.latest.Command.CommandID] = r.latest
			expired = append(expired, r.latest)
		}
	}
	return expired
}
func (m *Manager) Remove(roomID string) { m.mu.Lock(); delete(m.rooms, roomID); m.mu.Unlock() }
