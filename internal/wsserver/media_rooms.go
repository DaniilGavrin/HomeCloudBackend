package wsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

func decodeRoomData(data json.RawMessage, dest any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dest); err != nil {
		return fmt.Errorf("INVALID_REQUEST: %w", err)
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("INVALID_REQUEST: trailing data")
	}
	return nil
}
func (s *Server) registerMediaRoomCommands() {
	s.registry.MustRegister("media.rooms.play", s.mediaRoomPlay)
	s.registry.MustRegister("media.rooms.get", s.mediaRoomGet)
	s.registry.MustRegister("media.rooms.list", s.mediaRoomList)
	s.registry.MustRegister("media.rooms.stop", s.mediaRoomStop)
	s.registry.MustRegister("media.room.participant", s.mediaRoomParticipant)
	s.registry.MustRegister("media.room.ticket", s.mediaRoomTicket)
	s.registry.MustRegister("media.room.attach", s.mediaRoomAttach)
	s.registry.MustRegister("media.room.heartbeat", s.mediaRoomHeartbeat)
	s.registry.MustRegister("media.rooms.snapshot", s.mediaRoomsSnapshot)
	s.registry.MustRegister("media.room.report", s.mediaRoomReport)
	s.registry.MustRegister("media.room.control", s.mediaRoomControl)
	s.registry.MustRegister("media.room.control.ack", s.mediaRoomControlAck)
	s.registry.MustRegister("media.rooms.seek", s.mediaRoomSeek)
	s.registry.MustRegister("media.room.seek.source", s.mediaRoomSeekSource)
}
func (s *Server) mediaRoomPlay(ctx context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	if !s.cfg.MediaGateway.Enabled {
		return nil, errors.New("Media Gateway disabled")
	}
	if caller.ClientType == "media_gateway" {
		return nil, errors.New("Gateway cannot create rooms")
	}
	var req mediarooms.PlayRequest
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	if req.Network == "" {
		req.Network = "lan"
	}
	if req.Network != "lan" && req.Network != "tailscale" {
		return nil, mediarooms.ErrInvalid
	}
	if req.Target.NodeID != s.localNodeID || req.Target.DeviceID == "" {
		return nil, errors.New("target is not a TV on this Backend node")
	}
	if !s.clients.IsOnline(req.Target.DeviceID) {
		return nil, errors.New("target TV is offline")
	}
	reg, ok := s.gateways.Get()
	if !ok || !s.clients.IsOnline(reg.GatewayID) {
		return nil, errors.New("Gateway is offline")
	}
	ep, err := s.gateways.Resolve(req.Network)
	if err != nil {
		return nil, err
	}
	// Serializes creation, offer delivery and switches from all concurrent UI sessions.
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	outcome, err := s.rooms.Play(req, s.localNodeID, reg.GatewayID, ep.URL)
	if err != nil {
		return nil, err
	}
	snap := outcome.Snapshot
	if outcome.Fresh {
		s.mediaControls.Remove(snap.RoomID)
		if err := s.heartbeatStartForRoom(snap, reg.SessionID, time.Duration(reg.HeartbeatIntervalSeconds)*time.Second, time.Duration(reg.HeartbeatGraceSeconds)*time.Second); err != nil {
			s.rooms.Fail(snap.RoomID, "HEARTBEAT_SETUP_FAILED")
			return nil, err
		}
		ticket, expires, ticketErr := newAttachTicket()
		if ticketErr != nil {
			s.heartbeats.Remove(snap.RoomID)
			s.rooms.Fail(snap.RoomID, "INTERNAL_ERROR")
			return nil, ticketErr
		}
		payload := map[string]any{"room_id": snap.RoomID, "playback_id": snap.PlaybackID, "revision": snap.Revision, "generation": snap.Generation, "target": snap.Target, "source": snap.Source, "gateway_id": snap.GatewayID, "gateway_ws_url": snap.GatewayWSURL, "attach_ticket": ticket, "attach_expires_at": expires, "heartbeat_interval_seconds": reg.HeartbeatIntervalSeconds, "heartbeat_grace_seconds": reg.HeartbeatGraceSeconds}
		offer := commands.Outbound{V: 1, Type: "event", Topic: "media.room.offer", Data: payload}
		// Write receipt means DELIVERED to transport, never APPLIED/PLAYING.
		for _, device := range []string{reg.GatewayID, req.Target.DeviceID} {
			if e := s.clients.SendToDevice(ctx, device, offer); e != nil {
				s.heartbeats.Remove(snap.RoomID)
				closed, _ := s.rooms.Fail(snap.RoomID, "DELIVERY_FAILED")
				_ = s.clients.QueueToDevice(context.Background(), reg.GatewayID, commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{"room_id": snap.RoomID, "reason": "DELIVERY_FAILED"}})
				_ = s.clients.QueueToDevice(context.Background(), req.Target.DeviceID, commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{"room_id": snap.RoomID, "reason": "DELIVERY_FAILED"}})
				s.emitRoomClosed(closed)
				return nil, fmt.Errorf("room offer not delivered to %s: %w", device, e)
			}
		}
		s.emitRoomChanged(snap)
		s.emitCommandStatus(req.CommandID, snap.RoomID, "DELIVERED", "backend")
	}
	return map[string]any{"command_id": req.CommandID, "command_status": "accepted", "room_id": snap.RoomID, "playback_id": snap.PlaybackID, "revision": snap.Revision, "room_state": snap.RoomState, "delivered": outcome.Fresh}, nil
}
func (s *Server) mediaRoomGet(_ context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
	var req struct {
		RoomID string `json:"room_id"`
	}
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	return s.rooms.Get(req.RoomID)
}
func (s *Server) mediaRoomList(_ context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
	if len(bytes.TrimSpace(msg.Data)) > 0 && string(bytes.TrimSpace(msg.Data)) != "null" && string(bytes.TrimSpace(msg.Data)) != "{}" {
		return nil, errors.New("INVALID_REQUEST: media.rooms.list accepts no arguments")
	}
	return s.rooms.List(), nil
}
func (s *Server) mediaRoomStop(ctx context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req struct {
		CommandID string `json:"command_id"`
		RoomID    string `json:"room_id"`
	}
	if err := decodeRoomData(msg.Data, &req); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	if caller.ClientType == "media_gateway" {
		return nil, errors.New("Gateway cannot stop rooms through UI command")
	}
	before, err := s.rooms.Get(req.RoomID)
	if err != nil {
		var found bool
		before, found = s.rooms.ClosedRoom(req.RoomID)
		if !found {
			return nil, err
		}
	}
	if caller.ClientType == "tv" || caller.ClientType == "television" {
		if caller.DeviceID != before.Target.DeviceID || caller.SessionID <= 0 {
			return nil, errors.New("STOP_REJECTED: target TV session required")
		}
	}
	closed, fresh, err := s.rooms.Stop(req.CommandID, req.RoomID)
	if err != nil {
		return nil, err
	}
	if fresh {
		s.heartbeats.Remove(closed.RoomID)
		s.mediaControls.Remove(closed.RoomID)
		data := map[string]any{"room_id": closed.RoomID, "playback_id": closed.PlaybackID,
			"generation": closed.Generation, "reason": "STOP", "revision": closed.Revision,
			"sequence": closed.Sequence, "instance_id": closed.InstanceID}
		event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: data}
		// Wait for socket writes, never for remote execution. Best-effort cleanup
		// remains authoritative when a device disconnects mid-STOP.
		for _, device := range []string{before.GatewayID, before.Target.DeviceID} {
			notifyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			if notifyErr := s.clients.SendToDevice(notifyCtx, device, event); notifyErr != nil {
				s.log.Warn("STOP media.room.closed transport not delivered", "device_id", device, "error", notifyErr)
			}
			cancel()
		}
		s.emitRoomClosed(closed)
		s.emitCommandStatus(req.CommandID, req.RoomID, "ACCEPTED", "backend")
	}
	return map[string]any{"command_id": req.CommandID, "command_status": "accepted", "room_id": closed.RoomID, "room_state": "CLOSED", "revision": closed.Revision}, nil
}
func (s *Server) mediaRoomParticipant(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req struct {
		RoomID      string `json:"room_id"`
		PlaybackID  string `json:"playback_id"`
		Participant string `json:"participant"`
		Phase       string `json:"phase"`
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
	switch req.Participant {
	case "gateway":
		reg, ok := s.gateways.Get()
		if !ok || caller.DeviceID != reg.GatewayID || caller.ClientType != "media_gateway" || caller.SessionID != reg.SessionID {
			return nil, errors.New("gateway session mismatch")
		}
	case "tv":
		if caller.DeviceID != room.Target.DeviceID || !strings.Contains(caller.ClientType, "tv") {
			return nil, errors.New("TV session mismatch")
		}
	default:
		return nil, mediarooms.ErrInvalid
	}
	changed, err := s.rooms.ParticipantAcknowledged(req.RoomID, req.PlaybackID, req.Participant, req.Phase)
	if err != nil {
		return nil, err
	}
	s.emitRoomChanged(changed)
	if req.Phase == "FAILED" {
		s.heartbeats.Remove(req.RoomID)
		s.mediaControls.Remove(req.RoomID)
		closed, ok := s.rooms.Fail(req.RoomID, "PARTICIPANT_FAILED")
		if ok {
			event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{"room_id": closed.RoomID, "reason": "PARTICIPANT_FAILED", "revision": closed.Revision}}
			_ = s.clients.QueueToDevice(context.Background(), room.Target.DeviceID, event)
			_ = s.clients.QueueToDevice(context.Background(), room.GatewayID, event)
			s.emitRoomClosed(closed)
		}
	}
	if req.Phase == "APPLIED" {
		s.emitCommandStatus("", req.RoomID, "APPLIED", req.Participant)
	} else {
		s.emitCommandStatus("", req.RoomID, "FAILED", req.Participant)
	}
	return map[string]any{"room_id": changed.RoomID, "room_state": changed.RoomState, "revision": changed.Revision, "participant": req.Participant, "phase": req.Phase}, nil
}
func (s *Server) emitRoomChanged(room mediarooms.Snapshot) {
	_, _ = s.PublishEvent(context.Background(), "media.room.changed", room)
}
func (s *Server) emitRoomClosed(room mediarooms.Snapshot) {
	_, _ = s.PublishEvent(context.Background(), "media.room.closed", room)
}
func (s *Server) emitCommandStatus(commandID, roomID, status, executor string) {
	_, _ = s.PublishEvent(context.Background(), "media.command.status", map[string]any{"command_id": commandID, "room_id": roomID, "state": status, "executor": executor})
}
func (s *Server) onMediaPeerDisconnected(id string) {
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	// Do not destroy rooms owned by a newer replacement control session.
	if s.clients.IsOnline(id) {
		return
	}
	for _, room := range s.rooms.LostPeer(id) {
		s.heartbeats.Remove(room.RoomID)
		s.mediaControls.Remove(room.RoomID)
		closed := commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{"room_id": room.RoomID, "reason": "PEER_LOST", "revision": room.Revision}}
		if id != room.GatewayID {
			_ = s.clients.QueueToDevice(context.Background(), room.GatewayID, closed)
		}
		if id != room.Target.DeviceID {
			_ = s.clients.QueueToDevice(context.Background(), room.Target.DeviceID, closed)
		}
		s.emitRoomClosed(room)
	}
}
