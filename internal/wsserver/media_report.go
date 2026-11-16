package wsserver

import (
	"context"
	"errors"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

// A subscribed UI must subscribe to media.room.changed and media.room.closed
// FIRST, then call media.rooms.snapshot. Use instance_id + sequence as a
// high-water mark and discard queued events whose sequence <= snapshot seq.
func (s *Server) mediaRoomsSnapshot(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	if len(msg.Data) != 0 && string(msg.Data) != "{}" && string(msg.Data) != "null" {
		return nil, mediarooms.ErrInvalid
	}
	if caller.ClientType == "media_gateway" {
		return nil, errors.New("Gateway cannot read panel snapshots")
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	return s.rooms.Snapshot(), nil
}

// media.room.report records independently observed TV/Gateway execution.
// It is NOT a command to start a stream and does not accept claims from UI.
func (s *Server) mediaRoomReport(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req mediarooms.ExecutionReport
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	room, err := s.rooms.Get(req.RoomID)
	if err != nil {
		return nil, err
	}
	if req.PlaybackID != room.PlaybackID || req.Generation != room.Generation || req.ConnectionEpoch != room.AttachEpoch {
		return nil, mediarooms.ErrStale
	}
	switch req.Participant {
	case "gateway":
		reg, ok := s.gateways.Get()
		if !ok || reg.GatewayID != room.GatewayID || caller.DeviceID != reg.GatewayID || caller.ClientType != "media_gateway" || caller.SessionID != reg.SessionID {
			return nil, errors.New("REPORT_REJECTED: wrong Gateway control session")
		}
	case "tv":
		if caller.DeviceID != room.Target.DeviceID || (caller.ClientType != "tv" && caller.ClientType != "television") || caller.SessionID <= 0 {
			return nil, errors.New("REPORT_REJECTED: wrong TV control session")
		}
	default:
		return nil, mediarooms.ErrInvalid
	}
	req.SessionID = caller.SessionID // never trust wire-supplied session fields
	before := room.PlaybackState
	updated, fresh, err := s.rooms.ReportExecution(req)
	if err != nil {
		return nil, err
	}
	if fresh {
		s.emitRoomChanged(updated)
		// Source EOF means bytes were sent, NOT that the TV rendered its final
		// frame. Only an authenticated TV ENDED report completes playback.
		// This event does not close the room, heartbeat or direct media socket.
		if req.Participant == "tv" && updated.PlaybackState == "ENDED" && before != "ENDED" {
			_, _ = s.PublishEvent(context.Background(), "media.playback.ended", map[string]any{
				"room_id": updated.RoomID, "playback_id": updated.PlaybackID,
				"generation": updated.Generation, "position_ms": updated.PositionMS,
				"revision": updated.Revision, "sequence": updated.Sequence,
				"instance_id": updated.InstanceID})
		}
		if updated.PlaybackState == "PLAYING" && before != "PLAYING" {
			s.emitCommandStatus(updated.CommandID, updated.RoomID, "EXECUTED", "tv+gateway")
		}
		if updated.PlaybackState == "ERROR" && before != "ERROR" {
			_, _ = s.PublishEvent(context.Background(), "media.room.error", updated)
			s.emitCommandStatus(updated.CommandID, updated.RoomID, "FAILED", req.Participant)
		}
	}
	return map[string]any{"room_id": updated.RoomID, "playback_id": updated.PlaybackID,
		"generation": updated.Generation, "room_state": updated.RoomState, "playback_state": updated.PlaybackState,
		"revision": updated.Revision, "sequence": updated.Sequence, "duplicate": !fresh}, nil
}
