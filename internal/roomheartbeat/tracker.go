// Package roomheartbeat tracks both members of a media room independently.
// It never transports media or stores credentials and uses monotonic time for
// its deadlines (time.Now values retain the monotonic component in memory).
package roomheartbeat

import (
	"errors"
	"sort"
	"sync"
	"time"
)

var (
	ErrUnknown = errors.New("ROOM_NOT_FOUND")
	ErrStale   = errors.New("STALE_PLAYBACK")
	ErrInvalid = errors.New("INVALID_HEARTBEAT")
)

const (
	MinInterval     = 20 * time.Second
	MaxInterval     = 30 * time.Second
	DefaultInterval = 20 * time.Second
	Grace           = 10 * time.Second
)

type Record struct {
	RoomID, PlaybackID          string
	Generation                  uint32
	LastSeenTV, LastSeenGateway time.Time
	TVSession, GatewaySession   int64
	Interval, Grace             time.Duration
}
type Expired struct {
	Record      Record
	Participant string
}
type Tracker struct {
	mu    sync.Mutex
	rooms map[string]Record
}

func New() *Tracker { return &Tracker{rooms: make(map[string]Record)} }
func ValidPolicy(interval, grace time.Duration) bool {
	return interval >= MinInterval && interval <= MaxInterval && grace == Grace
}

// Start resets only the current playback's deadlines on a new generation.
// The room's TV session is acquired by its first authenticated heartbeat.
func (t *Tracker) Start(roomID, playbackID string, generation uint32, gatewaySession int64, interval, grace time.Duration, now time.Time) error {
	if roomID == "" || playbackID == "" || generation == 0 || gatewaySession <= 0 || !ValidPolicy(interval, grace) || now.IsZero() {
		return ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.rooms[roomID]
	if ok && generation <= prev.Generation {
		if generation == prev.Generation && playbackID == prev.PlaybackID && gatewaySession == prev.GatewaySession {
			return nil
		} // replay
		return ErrStale
	}
	t.rooms[roomID] = Record{RoomID: roomID, PlaybackID: playbackID, Generation: generation, GatewaySession: gatewaySession, Interval: interval, Grace: grace, LastSeenTV: now, LastSeenGateway: now}
	return nil
}

// Beat is called only AFTER the control layer verifies the claimed participant
// against the identified WebSocket's device ID, role and session.
func (t *Tracker) Beat(roomID, playbackID string, generation uint32, participant string, session int64, now time.Time) (Record, error) {
	if session <= 0 || now.IsZero() {
		return Record{}, ErrInvalid
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	rec, ok := t.rooms[roomID]
	if !ok {
		return Record{}, ErrUnknown
	}
	if rec.PlaybackID != playbackID || rec.Generation != generation {
		return Record{}, ErrStale
	}
	deadline := rec.Interval + rec.Grace
	switch participant {
	case "tv":
		if rec.TVSession != 0 && session < rec.TVSession {
			return Record{}, ErrStale
		}
		if now.Sub(rec.LastSeenTV) > deadline {
			return Record{}, ErrStale
		}
		rec.TVSession = session
		if !now.Before(rec.LastSeenTV) {
			rec.LastSeenTV = now
		}
	case "gateway":
		if session != rec.GatewaySession {
			return Record{}, ErrStale
		}
		if now.Sub(rec.LastSeenGateway) > deadline {
			return Record{}, ErrStale
		}
		if !now.Before(rec.LastSeenGateway) {
			rec.LastSeenGateway = now
		}
	default:
		return Record{}, ErrInvalid
	}
	t.rooms[roomID] = rec
	return rec, nil
}

// Expire fences overdue entries before returning them, preventing late beats
// from resurrecting a room while the control layer performs cleanup.
func (t *Tracker) Expire(now time.Time) []Expired {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []Expired
	for id, rec := range t.rooms {
		var side string
		if now.Sub(rec.LastSeenTV) > rec.Interval+rec.Grace {
			side = "tv"
		}
		if now.Sub(rec.LastSeenGateway) > rec.Interval+rec.Grace && side == "" {
			side = "gateway"
		}
		if side == "" {
			continue
		}
		delete(t.rooms, id)
		out = append(out, Expired{rec, side})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Record.RoomID < out[j].Record.RoomID })
	return out
}
func (t *Tracker) Remove(id string) { t.mu.Lock(); delete(t.rooms, id); t.mu.Unlock() }
func (t *Tracker) Close()           { t.mu.Lock(); clear(t.rooms); t.mu.Unlock() }
func (t *Tracker) Get(id string) (Record, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	v, ok := t.rooms[id]
	return v, ok
}
