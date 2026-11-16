// Package mediarooms stores the authoritative, ephemeral control-plane room state.
// No file payload or media bytes are stored or transported here.
package mediarooms

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

var (
	ErrInvalid  = errors.New("INVALID_REQUEST")
	ErrNotFound = errors.New("ROOM_NOT_FOUND")
	ErrConflict = errors.New("ROOM_CONFLICT")
	ErrLimit    = errors.New("ROOM_LIMIT")
	ErrStale    = errors.New("STALE_PLAYBACK")
)

type Target struct {
	NodeID   string `json:"node_id"`
	DeviceID string `json:"device_id"`
}
type Source struct {
	StorageNodeID string `json:"storage_node_id"`
	FileID        string `json:"file_id"`
}
type Participants struct {
	TV      string `json:"tv"`
	Gateway string `json:"gateway"`
}
type Snapshot struct {
	RoomID           string        `json:"room_id"`
	PlaybackID       string        `json:"playback_id"`
	Target           Target        `json:"target"`
	Source           Source        `json:"source"`
	GatewayID        string        `json:"gateway_id"`
	GatewayWSURL     string        `json:"gateway_ws_url"`
	RoomState        string        `json:"room_state"`
	PlaybackState    string        `json:"playback_state"`
	Revision         uint64        `json:"revision"`
	Generation       uint32        `json:"generation"`
	Participants     Participants  `json:"participants"`
	CloseReason      string        `json:"close_reason,omitempty"`
	TVAttach         string        `json:"tv_attach"`
	AttachEpoch      uint64        `json:"attach_epoch"`
	InstanceID       string        `json:"instance_id"`
	CommandID        string        `json:"command_id,omitempty"`
	PositionMS       int64         `json:"position_ms"`
	SeekTargetMS     *int64        `json:"seek_target_ms,omitempty"`
	SeekSourceMS     *int64        `json:"seek_source_ms,omitempty"`
	TVExecution      string        `json:"tv_execution,omitempty"`
	GatewayExecution string        `json:"gateway_execution,omitempty"`
	ErrorCode        string        `json:"error_code,omitempty"`
	Sequence         uint64        `json:"sequence"`
	ControlSequence  uint64        `json:"control_sequence"`
	ControlCommandID string        `json:"control_command_id,omitempty"`
	ControlAction    string        `json:"control_action,omitempty"`
	ControlStatus    string        `json:"control_status,omitempty"`
	ControlOrigin    string        `json:"control_origin,omitempty"`
	ControlErrorCode string        `json:"control_error_code,omitempty"`
	Volume           *int          `json:"volume,omitempty"`
	Muted            *bool         `json:"muted,omitempty"`
	tvReport         executionMemo `json:"-"`
	gatewayReport    executionMemo `json:"-"`
}
type PlayRequest struct {
	CommandID string `json:"command_id"`
	Network   string `json:"network,omitempty"`
	RoomID    string `json:"room_id,omitempty"`
	Target    Target `json:"target"`
	Source    Source `json:"source"`
}
type Outcome struct {
	Snapshot Snapshot
	Fresh    bool
}
type memo struct {
	Signature string
	Result    Snapshot
}
type Manager struct {
	mu           sync.Mutex
	rooms        map[string]Snapshot
	byTV         map[Target]string
	seen         map[string]memo
	order        []string
	max          int
	sequence     uint64
	instanceID   string
	stopped      map[string]Snapshot
	stoppedOrder []string
}

// SnapshotBundle is a consistent active-room snapshot across all HomeCloud UIs.
// Subscribe to events before requesting it, then discard older sequence values.
type SnapshotBundle struct {
	SchemaVersion int        `json:"schema_version"`
	InstanceID    string     `json:"instance_id"`
	Sequence      uint64     `json:"sequence"`
	Rooms         []Snapshot `json:"rooms"`
}

func (m *Manager) bumpLocked(s *Snapshot) {
	m.sequence++
	s.Sequence = m.sequence
}

func (m *Manager) Snapshot() SnapshotBundle {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := SnapshotBundle{SchemaVersion: 1, InstanceID: m.instanceID, Sequence: m.sequence, Rooms: make([]Snapshot, 0, len(m.rooms))}
	for _, r := range m.rooms {
		out.Rooms = append(out.Rooms, r)
	}
	sort.Slice(out.Rooms, func(i, j int) bool { return out.Rooms[i].RoomID < out.Rooms[j].RoomID })
	return out
}

const seenLimit = 2048

func New(max int) *Manager {
	if max < 1 {
		max = 4
	}
	instanceID, err := identifier("backend-")
	if err != nil {
		panic("mediarooms: cannot generate instance ID: " + err.Error())
	}
	return &Manager{rooms: map[string]Snapshot{}, byTV: map[Target]string{}, seen: map[string]memo{}, max: max, instanceID: instanceID, stopped: map[string]Snapshot{}}
}
func valid(s string) bool {
	return s != "" && len(s) <= 256 && strings.TrimSpace(s) == s && !strings.ContainsAny(s, "\r\n\x00")
}

// HCS1 file IDs encode a root-scoped relative path and may exceed the 256
// byte limit used for short control identifiers. Never relax limits for
// device IDs, room IDs or command IDs.
func validFileID(s string) bool {
	return s != "" && len(s) <= 2048 && strings.TrimSpace(s) == s &&
		!strings.ContainsAny(s, "\r\n\x00")
}
func identifier(prefix string) (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
func (m *Manager) remember(id, sig string, s Snapshot) {
	m.seen[id] = memo{sig, s}
	m.order = append(m.order, id)
	if len(m.order) > seenLimit {
		delete(m.seen, m.order[0])
		m.order = m.order[1:]
	}
}
func sigPlay(q PlayRequest) string {
	return fmt.Sprintf("play|%q|%q|%q|%q|%q|%q", q.RoomID, q.Network, q.Target.NodeID, q.Target.DeviceID, q.Source.StorageNodeID, q.Source.FileID)
}
func (m *Manager) Play(q PlayRequest, nodeID, gatewayID, wsURL string) (Outcome, error) {
	if !valid(q.CommandID) || !valid(q.Target.NodeID) || !valid(q.Target.DeviceID) || !valid(q.Source.StorageNodeID) || !validFileID(q.Source.FileID) || q.Target.NodeID != nodeID || !valid(gatewayID) || !strings.HasPrefix(wsURL, "ws://") {
		return Outcome{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sig := sigPlay(q)
	if prev, ok := m.seen[q.CommandID]; ok {
		if prev.Signature != sig {
			return Outcome{}, ErrConflict
		}
		return Outcome{Snapshot: prev.Result}, nil
	}
	var s Snapshot
	if q.RoomID == "" {
		if _, exists := m.byTV[q.Target]; exists {
			return Outcome{}, ErrConflict
		}
		if len(m.rooms) >= m.max {
			return Outcome{}, ErrLimit
		}
		id, err := identifier("room-")
		if err != nil {
			return Outcome{}, err
		}
		s = Snapshot{RoomID: id, Target: q.Target, GatewayID: gatewayID, GatewayWSURL: wsURL, RoomState: "CONNECTING", Revision: 1, TVAttach: "PENDING"}
		m.byTV[q.Target] = id
	} else {
		var ok bool
		s, ok = m.rooms[q.RoomID]
		if !ok {
			return Outcome{}, ErrNotFound
		}
		if s.Target != q.Target || s.GatewayID != gatewayID || s.GatewayWSURL != wsURL {
			return Outcome{}, ErrConflict
		}
		if s.Generation == ^uint32(0) {
			return Outcome{}, ErrConflict
		}
		s.Revision++
	}
	p, err := identifier("play-")
	if err != nil {
		if q.RoomID == "" {
			delete(m.byTV, q.Target)
		}
		return Outcome{}, err
	}
	s.PlaybackID = p
	s.Source = q.Source
	s.Generation++
	s.PlaybackState = "PREPARING"
	s.Participants = Participants{TV: "PENDING", Gateway: "PENDING"}
	s.TVAttach = "PENDING"
	s.InstanceID = m.instanceID
	s.CommandID = q.CommandID
	s.PositionMS = 0
	s.SeekTargetMS = nil
	s.SeekSourceMS = nil
	s.TVExecution = ""
	s.GatewayExecution = ""
	s.ControlCommandID = ""
	s.ControlAction = ""
	s.ControlStatus = ""
	s.ControlOrigin = ""
	s.ControlErrorCode = ""
	s.ControlSequence = 0
	s.ErrorCode = ""
	s.tvReport = executionMemo{}
	s.gatewayReport = executionMemo{}
	m.bumpLocked(&s)
	m.rooms[s.RoomID] = s
	m.remember(q.CommandID, sig, s)
	return Outcome{Snapshot: s, Fresh: true}, nil
}
func (m *Manager) Get(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	return s, nil
}
func (m *Manager) List() []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Snapshot, 0, len(m.rooms))
	for _, s := range m.rooms {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].RoomID < out[j].RoomID })
	return out
}

// ParticipantAcknowledged changes only the recorded delivery/application phase.
// PLAYING is never inferred from a WebSocket write or an offer acknowledgement.
func (m *Manager) ParticipantAcknowledged(roomID, playbackID, participant, phase string) (Snapshot, error) {
	if phase != "APPLIED" && phase != "FAILED" {
		return Snapshot{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[roomID]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	if s.PlaybackID != playbackID {
		return Snapshot{}, ErrStale
	}
	switch participant {
	case "tv":
		if s.Participants.TV == phase {
			return s, nil
		}
		s.Participants.TV = phase
	case "gateway":
		if s.Participants.Gateway == phase {
			return s, nil
		}
		s.Participants.Gateway = phase
	default:
		return Snapshot{}, ErrInvalid
	}
	s.Revision++
	if s.Participants.TV == "APPLIED" && s.Participants.Gateway == "APPLIED" {
		s.RoomState = "ACTIVE"
	}
	if phase == "FAILED" {
		s.RoomState = "LOST"
	}
	if phase == "APPLIED" {
		s.PlaybackState = resolvedPlayback(s)
	}
	m.bumpLocked(&s)
	m.rooms[roomID] = s
	return s, nil
}
func (m *Manager) closeLocked(id, reason string) Snapshot {
	s := m.rooms[id]
	delete(m.rooms, id)
	delete(m.byTV, s.Target)
	s.Revision++
	s.RoomState = "CLOSED"
	s.CloseReason = reason
	m.bumpLocked(&s)
	// A bounded tombstone makes retries with NEW command IDs idempotent too.
	m.stopped[id] = s
	m.stoppedOrder = append(m.stoppedOrder, id)
	if len(m.stoppedOrder) > stoppedHistoryLimit {
		delete(m.stopped, m.stoppedOrder[0])
		m.stoppedOrder = m.stoppedOrder[1:]
	}
	return s
}
func (m *Manager) Stop(cmdID, id string) (Snapshot, bool, error) {
	if !valid(cmdID) || !valid(id) {
		return Snapshot{}, false, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	sig := "stop|" + id
	if prev, ok := m.seen[cmdID]; ok {
		if prev.Signature != sig {
			return Snapshot{}, false, ErrConflict
		}
		return prev.Result, false, nil
	}
	if _, ok := m.rooms[id]; !ok {
		if ended, exists := m.stopped[id]; exists && ended.CloseReason == "STOP" {
			m.remember(cmdID, sig, ended)
			return ended, false, nil
		}
		return Snapshot{}, false, ErrNotFound
	}
	s := m.closeLocked(id, "STOP")
	m.remember(cmdID, sig, s)
	return s, true, nil
}

// MarkLost makes LOST observable before terminal cleanup. It is a real
// authoritative state transition, not merely a fabricated event payload.
func (m *Manager) MarkLost(id string) (Snapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[id]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	if s.RoomState != "LOST" {
		s.RoomState = "LOST"
		s.Revision++
		m.bumpLocked(&s)
		m.rooms[id] = s
	}
	return s, nil
}
func (m *Manager) Fail(id, reason string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.rooms[id]; !ok {
		return Snapshot{}, false
	}
	return m.closeLocked(id, reason), true
}
func (m *Manager) LostPeer(deviceID string) []Snapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := []Snapshot{}
	for id, s := range m.rooms {
		if s.Target.DeviceID == deviceID || s.GatewayID == deviceID {
			out = append(out, m.closeLocked(id, "PEER_LOST"))
		}
	}
	return out
}
func (m *Manager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.rooms {
		m.closeLocked(id, "SHUTDOWN")
	}
}

// UpdateAttach is an independent direct-TV transport status. READY never
// implies video PLAYING, and stale detach/ready cannot mutate newer epochs.
func (m *Manager) UpdateAttach(roomID, playbackID string, generation uint32, epoch uint64, phase string) (Snapshot, error) {
	if epoch == 0 || (phase != "ATTACHING" && phase != "READY" && phase != "DISCONNECTED") {
		return Snapshot{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[roomID]
	if !ok {
		return Snapshot{}, ErrNotFound
	}
	if s.PlaybackID != playbackID || s.Generation != generation {
		return Snapshot{}, ErrStale
	}
	if epoch < s.AttachEpoch {
		return Snapshot{}, ErrStale
	}
	if epoch == s.AttachEpoch && s.TVAttach == phase {
		return s, nil
	}
	if epoch == s.AttachEpoch && s.TVAttach == "DISCONNECTED" {
		return Snapshot{}, ErrStale
	}
	if epoch == s.AttachEpoch && s.TVAttach == "READY" && phase == "ATTACHING" {
		return Snapshot{}, ErrStale
	}
	if epoch > s.AttachEpoch && phase != "ATTACHING" {
		return Snapshot{}, ErrInvalid
	}
	if epoch > s.AttachEpoch {
		s.AttachEpoch = epoch
		// A new direct WS epoch cannot inherit old playback execution receipts.
		s.TVExecution = ""
		s.GatewayExecution = ""
		s.ErrorCode = ""
		s.tvReport = executionMemo{}
		s.gatewayReport = executionMemo{}
		s.PlaybackState = "BUFFERING"
	}
	s.TVAttach = phase
	s.Revision++
	if phase == "DISCONNECTED" {
		s.TVExecution = ""
		s.GatewayExecution = ""
		s.tvReport = executionMemo{}
		s.gatewayReport = executionMemo{}
		s.PlaybackState = "BUFFERING"
	}
	if phase == "READY" {
		s.PlaybackState = resolvedPlayback(s)
	}
	m.bumpLocked(&s)
	m.rooms[roomID] = s
	return s, nil
}
