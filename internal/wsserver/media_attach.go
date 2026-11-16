package wsserver

import (
	"HomeCloudBackend/internal/commands"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"time"
)

func newAttachTicket() (string, time.Time, error) {
	var key [32]byte
	if _, err := rand.Read(key[:]); err != nil {
		return "", time.Time{}, err
	}
	return hex.EncodeToString(key[:]), time.Now().UTC().Add(60 * time.Second), nil
}

// media.room.ticket is callable only by the room's TV control client. Rotate
// the one-use ticket on reconnect without exposing it to other UI clients.
func (s *Server) mediaRoomTicket(ctx context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req struct {
		RoomID string `json:"room_id"`
	}
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	room, err := s.rooms.Get(req.RoomID)
	if err != nil {
		return nil, err
	}
	if caller.DeviceID != room.Target.DeviceID || caller.ClientType != "tv" {
		return nil, errors.New("ATTACH_REJECTED: target TV control session required")
	}
	reg, ok := s.gateways.Get()
	if !ok || reg.GatewayID != room.GatewayID || !s.clients.IsOnline(room.GatewayID) {
		return nil, errors.New("Gateway is offline")
	}
	ticket, expires, err := newAttachTicket()
	if err != nil {
		return nil, err
	}
	payload := map[string]any{"room_id": room.RoomID, "playback_id": room.PlaybackID, "generation": room.Generation, "target": room.Target, "attach_ticket": ticket, "attach_expires_at": expires, "seek_position_ms": room.SeekTargetMS}
	event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.ticket", Data: payload}
	if err = s.clients.SendToDevice(ctx, room.GatewayID, event); err != nil {
		return nil, err
	}
	return payload, nil
}

// A direct attachment is only a transport milestone, not PLAYING.
func (s *Server) mediaRoomAttach(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req struct {
		RoomID          string `json:"room_id"`
		PlaybackID      string `json:"playback_id"`
		Generation      uint32 `json:"generation"`
		ConnectionEpoch uint64 `json:"connection_epoch"`
		Phase           string `json:"phase"`
	}
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	reg, ok := s.gateways.Get()
	if !ok || caller.ClientType != "media_gateway" || caller.DeviceID != reg.GatewayID || caller.SessionID != reg.SessionID {
		return nil, errors.New("ATTACH_REJECTED: Gateway session mismatch")
	}
	room, err := s.rooms.Get(req.RoomID)
	if err != nil {
		return nil, err
	}
	if room.GatewayID != reg.GatewayID {
		return nil, errors.New("ATTACH_REJECTED: wrong Gateway")
	}
	updated, err := s.rooms.UpdateAttach(req.RoomID, req.PlaybackID, req.Generation, req.ConnectionEpoch, req.Phase)
	if err != nil {
		return nil, err
	}
	s.emitRoomChanged(updated)
	_, _ = s.PublishEvent(context.Background(), "media.room.attach.changed", map[string]any{"room_id": req.RoomID, "playback_id": req.PlaybackID, "generation": req.Generation, "connection_epoch": req.ConnectionEpoch, "phase": req.Phase, "revision": updated.Revision, "sequence": updated.Sequence, "instance_id": updated.InstanceID})
	return map[string]any{"room_id": req.RoomID, "tv_attach": updated.TVAttach, "revision": updated.Revision}, nil
}
