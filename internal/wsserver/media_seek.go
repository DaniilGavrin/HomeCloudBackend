package wsserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

// media.rooms.seek is a control-plane request, not a byte-range calculation.
// It advances the HCM1 generation and delivers a fresh, one-use attach grant.
// Actual source and decoder positions are reported independently afterwards.
func (s *Server) mediaRoomSeek(ctx context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var q mediarooms.SeekRequest
	if err := decodeRoomData(msg.Data, &q); err != nil {
		return nil, err
	}
	if caller.ClientType == "media_gateway" || caller.ClientType == "" {
		return nil, errors.New("SEEK_REJECTED: UI or target TV required")
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	before, err := s.rooms.Get(q.RoomID)
	if err != nil {
		return nil, err
	}
	if caller.ClientType == "tv" || caller.ClientType == "television" {
		if caller.DeviceID != before.Target.DeviceID || caller.SessionID <= 0 {
			return nil, errors.New("SEEK_REJECTED: wrong TV")
		}
	}
	if !s.clients.IsOnline(before.Target.DeviceID) || !s.clients.IsOnline(before.GatewayID) {
		return nil, errors.New("SEEK_REJECTED: participant offline")
	}
	reg, ok := s.gateways.Get()
	if !ok || reg.GatewayID != before.GatewayID || reg.SessionID <= 0 {
		return nil, errors.New("SEEK_REJECTED: Gateway session mismatch")
	}
	outcome, err := s.rooms.Seek(q)
	if err != nil {
		return nil, err
	}
	updated := outcome.Snapshot
	if !outcome.Fresh {
		return map[string]any{"command_id": q.CommandID, "room_id": updated.RoomID,
			"playback_id": updated.PlaybackID, "generation": updated.Generation,
			"seek_target_ms": updated.SeekTargetMS, "revision": updated.Revision, "replayed": true}, nil
	}
	s.mediaControls.Remove(updated.RoomID) // old pending play/pause/volume cannot complete against new epoch
	abort := func(reason string) {
		s.heartbeats.Remove(updated.RoomID)
		closed, exists := s.rooms.Fail(updated.RoomID, reason)
		if !exists {
			return
		}
		event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{
			"room_id": closed.RoomID, "reason": reason, "revision": closed.Revision}}
		for _, device := range []string{before.Target.DeviceID, before.GatewayID} {
			sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = s.clients.QueueToDevice(sendCtx, device, event)
			cancel()
		}
		s.emitRoomClosed(closed)
	}
	if err := s.heartbeatStartForRoom(updated, reg.SessionID,
		time.Duration(reg.HeartbeatIntervalSeconds)*time.Second,
		time.Duration(reg.HeartbeatGraceSeconds)*time.Second); err != nil {
		abort("SEEK_HEARTBEAT_FAILED")
		return nil, fmt.Errorf("seek heartbeat: %w", err)
	}
	ticket, expires, err := newAttachTicket()
	if err != nil {
		abort("SEEK_TICKET_FAILED")
		return nil, err
	}
	payload := map[string]any{
		"room_id": updated.RoomID, "playback_id": updated.PlaybackID,
		"generation": updated.Generation, "revision": updated.Revision,
		"target": updated.Target, "source": updated.Source, "gateway_id": updated.GatewayID,
		"gateway_ws_url": updated.GatewayWSURL, "seek_position_ms": updated.SeekTargetMS,
		"attach_ticket": ticket, "attach_expires_at": expires,
		"heartbeat_interval_seconds": reg.HeartbeatIntervalSeconds,
		"heartbeat_grace_seconds":    reg.HeartbeatGraceSeconds,
	}
	offer := commands.Outbound{V: 1, Type: "event", Topic: "media.room.offer", Data: payload}
	// The new offer fences old direct sockets and old HCM1 generations.
	for _, device := range []string{before.GatewayID, before.Target.DeviceID} {
		sendCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		e := s.clients.SendToDevice(sendCtx, device, offer)
		cancel()
		if e != nil {
			abort("SEEK_DELIVERY_FAILED")
			return nil, fmt.Errorf("seek offer to %s: %w", device, e)
		}
	}
	s.emitRoomChanged(updated)
	s.emitCommandStatus(q.CommandID, q.RoomID, "DELIVERED", "backend")
	return map[string]any{"command_id": q.CommandID, "command_status": "accepted",
		"room_id": updated.RoomID, "playback_id": updated.PlaybackID,
		"generation": updated.Generation, "seek_target_ms": updated.SeekTargetMS,
		"revision": updated.Revision, "replayed": false}, nil
}

// Source seek completion is only sent by the registered Gateway control WS.
// PositionMS from the TV decoder remains the authoritative UI playhead.
func (s *Server) mediaRoomSeekSource(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var q struct {
		RoomID           string `json:"room_id"`
		PlaybackID       string `json:"playback_id"`
		Generation       uint32 `json:"generation"`
		ConnectionEpoch  uint64 `json:"connection_epoch"`
		SourcePositionMS int64  `json:"source_position_ms"`
	}
	if err := decodeRoomData(msg.Data, &q); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	room, err := s.rooms.Get(q.RoomID)
	if err != nil {
		return nil, err
	}
	reg, ok := s.gateways.Get()
	if !ok || room.GatewayID != reg.GatewayID || caller.DeviceID != reg.GatewayID || caller.ClientType != "media_gateway" || caller.SessionID != reg.SessionID {
		return nil, errors.New("SEEK_SOURCE_REJECTED: Gateway session mismatch")
	}
	updated, fresh, err := s.rooms.ReportSeekSource(q.RoomID, q.PlaybackID, q.Generation, q.ConnectionEpoch, q.SourcePositionMS)
	if err != nil {
		return nil, err
	}
	if fresh {
		s.emitRoomChanged(updated)
	}
	return map[string]any{"room_id": updated.RoomID, "generation": updated.Generation,
		"source_position_ms": updated.SeekSourceMS, "revision": updated.Revision, "duplicate": !fresh}, nil
}
