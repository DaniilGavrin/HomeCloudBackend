package mediarooms

import (
	"fmt"
)

// MaxSeekMS bounds untrusted timeline requests without assuming a bitrate or
// converting a timestamp into a byte offset. Seven days covers long recordings.
const MaxSeekMS int64 = 7 * 24 * 60 * 60 * 1000

type SeekRequest struct {
	CommandID  string `json:"command_id"`
	RoomID     string `json:"room_id"`
	PlaybackID string `json:"playback_id"`
	Generation uint32 `json:"generation"`
	PositionMS int64  `json:"position_ms"`
}

// Seek creates a new transport generation for the SAME playback and room.
// Actual progress is only accepted from a new-generation TV report; the target
// is exposed separately to avoid presenting a requested time as confirmed.
func (m *Manager) Seek(q SeekRequest) (Outcome, error) {
	if !valid(q.CommandID) || !valid(q.RoomID) || !valid(q.PlaybackID) ||
		q.Generation == 0 || q.PositionMS < 0 || q.PositionMS > MaxSeekMS {
		return Outcome{}, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	signature := fmt.Sprintf("seek|%q|%q|%d|%d", q.RoomID, q.PlaybackID, q.Generation, q.PositionMS)
	if previous, exists := m.seen[q.CommandID]; exists {
		if previous.Signature != signature {
			return Outcome{}, ErrConflict
		}
		return Outcome{Snapshot: previous.Result}, nil
	}
	s, ok := m.rooms[q.RoomID]
	if !ok {
		return Outcome{}, ErrNotFound
	}
	if s.PlaybackID != q.PlaybackID || s.Generation != q.Generation {
		return Outcome{}, ErrStale
	}
	if s.RoomState != "ACTIVE" || s.TVAttach != "READY" ||
		(s.PlaybackState != "PLAYING" && s.PlaybackState != "PAUSED" &&
			s.PlaybackState != "BUFFERING" && s.PlaybackState != "ENDED") {
		return Outcome{}, ErrConflict
	}
	if s.Generation == ^uint32(0) {
		return Outcome{}, ErrConflict
	}
	target := q.PositionMS
	s.Generation++
	s.Revision++
	s.TVAttach = "PENDING"
	s.Participants = Participants{TV: "PENDING", Gateway: "PENDING"}
	s.PlaybackState = "PREPARING"
	s.TVExecution = ""
	s.GatewayExecution = ""
	s.ErrorCode = ""
	s.CommandID = q.CommandID
	s.SeekTargetMS = &target
	s.SeekSourceMS = nil
	s.tvReport = executionMemo{}
	s.gatewayReport = executionMemo{}
	s.ControlSequence = 0
	s.ControlCommandID = ""
	s.ControlAction = ""
	s.ControlStatus = ""
	s.ControlOrigin = ""
	s.ControlErrorCode = ""
	m.bumpLocked(&s)
	m.rooms[q.RoomID] = s
	m.remember(q.CommandID, signature, s)
	return Outcome{Snapshot: s, Fresh: true}, nil
}

// ReportSeekSource tracks the *source* keyframe/time chosen by Gateway.
// This is NOT the decoder's presented position; only the TV may report that.
func (m *Manager) ReportSeekSource(roomID, playbackID string, generation uint32, epoch uint64, actualMS int64) (Snapshot, bool, error) {
	if !valid(roomID) || !valid(playbackID) || generation == 0 || epoch == 0 || actualMS < 0 || actualMS > MaxSeekMS {
		return Snapshot{}, false, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, exists := m.rooms[roomID]
	if !exists {
		return Snapshot{}, false, ErrNotFound
	}
	if s.PlaybackID != playbackID || s.Generation != generation || s.AttachEpoch != epoch || s.SeekTargetMS == nil {
		return Snapshot{}, false, ErrStale
	}
	if s.SeekSourceMS != nil {
		if *s.SeekSourceMS == actualMS {
			return s, false, nil
		}
		return Snapshot{}, false, ErrStale
	}
	position := actualMS
	s.SeekSourceMS = &position
	s.Revision++
	m.bumpLocked(&s)
	m.rooms[roomID] = s
	return s, true, nil
}
