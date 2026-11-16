package mediarooms

import "HomeCloudBackend/internal/roomcontrol"

// ApplyControl projects the transport/ack ledger into the authoritative
// room snapshot. It MUST be called under wsserver.roomDispatchMu, after the
// controller has fenced command ID, sequence, generation and attach epoch.
// No media state is changed on ACCEPTED or DELIVERED; only TV execution counts.
func (m *Manager) ApplyControl(rec roomcontrol.Record) (Snapshot, bool, error) {
	c := rec.Command
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.rooms[c.RoomID]
	if !ok {
		return Snapshot{}, false, ErrNotFound
	}
	if s.PlaybackID != c.PlaybackID || s.Generation != c.Generation || s.AttachEpoch != c.ConnectionEpoch {
		return Snapshot{}, false, ErrStale
	}
	if c.CommandSequence < s.ControlSequence || (c.CommandSequence == s.ControlSequence && s.ControlCommandID != c.CommandID) {
		return Snapshot{}, false, ErrStale
	}
	if c.CommandSequence == s.ControlSequence && s.ControlStatus == rec.Status && s.ControlCommandID == c.CommandID {
		return s, false, nil
	}
	s.ControlSequence = c.CommandSequence
	s.ControlCommandID = c.CommandID
	s.ControlAction = c.Action
	s.ControlStatus = rec.Status
	s.ControlOrigin = c.Origin
	s.ControlErrorCode = rec.ErrorCode
	if rec.Status == "EXECUTED" {
		switch c.Action {
		case "volume":
			if c.Volume != nil {
				value := *c.Volume
				s.Volume = &value
			}
		case "mute":
			if c.Muted != nil {
				value := *c.Muted
				s.Muted = &value
			}
		case "pause":
			s.TVExecution = "PAUSED"
			s.PlaybackState = resolvedPlayback(s)
		case "resume", "play":
			// An executed resume means decoder unpaused, not that first
			// samples were rendered. KAN-51 still needs execution reports.
			s.TVExecution = "BUFFERING"
			s.PlaybackState = "BUFFERING"
		}
	}
	s.Revision++
	m.bumpLocked(&s)
	m.rooms[c.RoomID] = s
	return s, true, nil
}
