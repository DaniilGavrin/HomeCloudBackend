package mediarooms

// ExecutionReport describes observed work, NEVER a command delivery receipt.
// Sequence increases independently for TV and Gateway within one attachment
// epoch and control session. It is not a global event sequence.
type ExecutionReport struct {
	RoomID          string `json:"room_id"`
	PlaybackID      string `json:"playback_id"`
	Generation      uint32 `json:"generation"`
	ConnectionEpoch uint64 `json:"connection_epoch"`
	Participant     string `json:"participant"`
	ReportSequence  uint64 `json:"report_sequence"`
	State           string `json:"state"`
	PositionMS      int64  `json:"position_ms"`
	ErrorCode       string `json:"error_code,omitempty"`
	SessionID       int64  `json:"-"`
}

type executionMemo struct {
	SessionID  int64
	Epoch      uint64
	Sequence   uint64
	State      string
	PositionMS int64
	ErrorCode  string
}

func validErrorCode(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, r := range s {
		if !(r >= 'A' && r <= 'Z') && !(r >= '0' && r <= '9') && r != '_' {
			return false
		}
	}
	return true
}

// ReportExecution updates authoritative playback state from one identified
// participant, with a per-connection monotonic report fence. A duplicate of
// identical content is idempotent; a stale/conflicting report is rejected.
// The caller (wsserver) MUST authenticate participant and session before here.
func (m *Manager) ReportExecution(q ExecutionReport) (Snapshot, bool, error) {
	if !valid(q.RoomID) || !valid(q.PlaybackID) || q.Generation == 0 || q.ConnectionEpoch == 0 ||
		q.SessionID <= 0 || q.ReportSequence == 0 || q.PositionMS < 0 || q.PositionMS > 1<<52 ||
		((q.State == "ERROR") != (q.ErrorCode != "")) ||
		(q.ErrorCode != "" && !validErrorCode(q.ErrorCode)) {
		return Snapshot{}, false, ErrInvalid
	}
	switch q.Participant {
	case "tv":
		if q.State != "PLAYING" && q.State != "PAUSED" && q.State != "BUFFERING" && q.State != "ENDED" && q.State != "IDLE" && q.State != "ERROR" {
			return Snapshot{}, false, ErrInvalid
		}
	case "gateway":
		if q.State != "STREAMING" && q.State != "BUFFERING" && q.State != "ENDED" && q.State != "IDLE" && q.State != "ERROR" {
			return Snapshot{}, false, ErrInvalid
		}
		if q.PositionMS != 0 {
			return Snapshot{}, false, ErrInvalid
		} // TV owns actual decoder position
	default:
		return Snapshot{}, false, ErrInvalid
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[q.RoomID]
	if !ok {
		return Snapshot{}, false, ErrNotFound
	}
	if s.PlaybackID != q.PlaybackID || s.Generation != q.Generation || s.AttachEpoch != q.ConnectionEpoch || s.TVAttach != "READY" ||
		(s.RoomState != "ACTIVE" && s.RoomState != "CONNECTING") {
		return Snapshot{}, false, ErrStale
	}
	// EOF is terminal for the current playback generation. A delayed
	// PLAYING/STREAMING report cannot resurrect it after ENDED or IDLE.
	if q.Participant == "tv" && (s.TVExecution == "ENDED" || s.TVExecution == "IDLE") {
		if q.State != "ENDED" && q.State != "IDLE" {
			return Snapshot{}, false, ErrStale
		}
		if s.TVExecution == "IDLE" && q.State != "IDLE" {
			return Snapshot{}, false, ErrStale
		}
	}
	if q.Participant == "gateway" && s.GatewayExecution == "ENDED" && q.State != "ENDED" && q.State != "IDLE" {
		return Snapshot{}, false, ErrStale
	}
	var previous *executionMemo
	if q.Participant == "tv" {
		previous = &s.tvReport
	} else {
		previous = &s.gatewayReport
	}
	if previous.SessionID != 0 {
		if q.SessionID < previous.SessionID || (q.SessionID == previous.SessionID && q.ConnectionEpoch < previous.Epoch) {
			return Snapshot{}, false, ErrStale
		}
		if q.SessionID == previous.SessionID && q.ConnectionEpoch == previous.Epoch {
			if q.ReportSequence < previous.Sequence {
				return Snapshot{}, false, ErrStale
			}
			if q.ReportSequence == previous.Sequence {
				if previous.State == q.State && previous.PositionMS == q.PositionMS && previous.ErrorCode == q.ErrorCode {
					return s, false, nil
				}
				return Snapshot{}, false, ErrStale
			}
		}
	}
	next := executionMemo{SessionID: q.SessionID, Epoch: q.ConnectionEpoch, Sequence: q.ReportSequence, State: q.State, PositionMS: q.PositionMS, ErrorCode: q.ErrorCode}
	if q.Participant == "tv" {
		s.tvReport = next
		s.TVExecution = q.State
		s.PositionMS = q.PositionMS
	} else {
		s.gatewayReport = next
		s.GatewayExecution = q.State
	}
	s.ErrorCode = ""
	if s.TVExecution == "ERROR" {
		s.ErrorCode = s.tvReport.ErrorCode
	}
	if s.GatewayExecution == "ERROR" {
		s.ErrorCode = s.gatewayReport.ErrorCode
	}
	s.PlaybackState = resolvedPlayback(s)
	s.Revision++
	m.bumpLocked(&s)
	m.rooms[q.RoomID] = s
	return s, true, nil
}

func resolvedPlayback(s Snapshot) string {
	if s.TVExecution == "ERROR" || s.GatewayExecution == "ERROR" {
		return "ERROR"
	}
	if s.TVExecution == "ENDED" {
		return "ENDED"
	}
	if s.TVExecution == "IDLE" {
		return "IDLE"
	}
	if s.TVExecution == "PAUSED" {
		return "PAUSED"
	}
	if s.TVExecution == "BUFFERING" || s.GatewayExecution == "BUFFERING" {
		return "BUFFERING"
	}
	if s.RoomState == "ACTIVE" && s.TVAttach == "READY" && s.Participants.TV == "APPLIED" && s.Participants.Gateway == "APPLIED" &&
		s.TVExecution == "PLAYING" && (s.GatewayExecution == "STREAMING" || s.GatewayExecution == "ENDED") {
		return "PLAYING"
	}
	if s.TVExecution != "" || s.GatewayExecution != "" {
		return "BUFFERING"
	}
	return "PREPARING"
}
