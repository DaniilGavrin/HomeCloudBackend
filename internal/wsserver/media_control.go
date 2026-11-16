package wsserver

import (
	"context"
	"errors"
	"strings"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
	"HomeCloudBackend/internal/roomcontrol"
)

type mediaControlRequest struct {
	RoomID     string `json:"room_id"`
	PlaybackID string `json:"playback_id"`
	Generation uint32 `json:"generation"`
	CommandID  string `json:"command_id"`
	Action     string `json:"action"`
	Volume     *int   `json:"volume,omitempty"`
	Muted      *bool  `json:"muted,omitempty"`
}

func (s *Server) mediaRoomControl(ctx context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var request mediaControlRequest
	if err := decodeRoomData(msg.Data, &request); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	room, err := s.rooms.Get(request.RoomID)
	if err != nil {
		return nil, err
	}
	if room.PlaybackID != request.PlaybackID || room.Generation != request.Generation {
		return nil, mediarooms.ErrStale
	}
	if room.RoomState != "ACTIVE" || room.TVAttach != "READY" || room.AttachEpoch == 0 {
		return nil, errors.New("ROOM_NOT_READY")
	}
	if !s.clients.IsOnline(room.Target.DeviceID) {
		return nil, errors.New("TV_OFFLINE")
	}
	origin := "ui"
	if caller.ClientType == "tv" || caller.ClientType == "television" {
		if caller.DeviceID != room.Target.DeviceID || caller.SessionID <= 0 {
			return nil, errors.New("CONTROL_REJECTED: wrong TV")
		}
		origin = "tv"
	} else if caller.ClientType == "media_gateway" || caller.ClientType == "" {
		return nil, errors.New("CONTROL_REJECTED: UI or target TV required")
	}
	if (request.Action == "pause" || request.Action == "resume" || request.Action == "play") && !s.clients.IsOnline(room.GatewayID) {
		return nil, errors.New("GATEWAY_OFFLINE")
	}
	command := roomcontrol.Command{RoomID: room.RoomID, PlaybackID: room.PlaybackID, Generation: room.Generation, ConnectionEpoch: room.AttachEpoch,
		CommandID: request.CommandID, Action: request.Action, Volume: request.Volume, Muted: request.Muted, Origin: origin}
	rec, fresh, err := s.mediaControls.Issue(command, time.Now())
	if err != nil {
		return nil, err
	}
	if !fresh {
		return rec, nil
	}
	if updated, changed, e := s.rooms.ApplyControl(rec); e == nil && changed {
		s.emitRoomChanged(updated)
	}
	event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.control", Data: rec.Command}
	devices := []string{}
	if origin == "ui" {
		devices = append(devices, room.Target.DeviceID)
	}
	if request.Action == "pause" || request.Action == "resume" || request.Action == "play" {
		devices = append(devices, room.GatewayID)
	}
	for _, target := range devices {
		deliveryCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		e := s.clients.SendToDevice(deliveryCtx, target, event)
		cancel()
		if e != nil {
			failed, _ := s.mediaControls.Fail(room.RoomID, rec.Command.CommandID, "DELIVERY_FAILED")
			if updated, changed, projectionErr := s.rooms.ApplyControl(failed); projectionErr == nil && changed {
				s.emitRoomChanged(updated)
			}
			s.emitCommandStatus(rec.Command.CommandID, room.RoomID, "FAILED", "backend")
			return failed, e
		}
	}
	if rec.Status != "EXECUTED" {
		rec, err = s.mediaControls.MarkDelivered(room.RoomID, rec.Command.CommandID)
		if err != nil {
			return nil, err
		}
		if updated, changed, e := s.rooms.ApplyControl(rec); e == nil && changed {
			s.emitRoomChanged(updated)
		}
		s.emitCommandStatus(rec.Command.CommandID, room.RoomID, "DELIVERED", "backend")
	} else {
		// Local TV-originated volume/mute is already physically applied;
		// do not send it back to the same TV (echo feedback loop).
		s.emitCommandStatus(rec.Command.CommandID, room.RoomID, "EXECUTED", "tv")
	}
	return rec, nil
}

func (s *Server) mediaRoomControlAck(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var ack roomcontrol.Ack
	if err := decodeRoomData(msg.Data, &ack); err != nil {
		return nil, err
	}
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	room, err := s.rooms.Get(ack.RoomID)
	if err != nil {
		return nil, err
	}
	if room.PlaybackID != ack.PlaybackID || room.Generation != ack.Generation || room.AttachEpoch != ack.ConnectionEpoch {
		return nil, mediarooms.ErrStale
	}
	switch ack.Participant {
	case "tv":
		if caller.DeviceID != room.Target.DeviceID || (caller.ClientType != "tv" && caller.ClientType != "television") || caller.SessionID <= 0 {
			return nil, errors.New("CONTROL_ACK_REJECTED: wrong TV")
		}
	case "gateway":
		reg, ok := s.gateways.Get()
		if !ok || caller.DeviceID != room.GatewayID || caller.ClientType != "media_gateway" || caller.SessionID != reg.SessionID {
			return nil, errors.New("CONTROL_ACK_REJECTED: wrong Gateway session")
		}
	default:
		return nil, roomcontrol.ErrInvalid
	}
	if ack.ErrorCode != "" && (len(ack.ErrorCode) > 64 || strings.Trim(ack.ErrorCode, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_") != "") {
		return nil, roomcontrol.ErrInvalid
	}
	rec, changed, err := s.mediaControls.Ack(ack)
	if err != nil {
		return nil, err
	}
	if changed {
		if updated, wasUpdated, e := s.rooms.ApplyControl(rec); e == nil && wasUpdated {
			s.emitRoomChanged(updated)
		}
		if rec.Status == "EXECUTED" || rec.Status == "FAILED" {
			s.emitCommandStatus(rec.Command.CommandID, room.RoomID, rec.Status, ack.Participant)
		}
	}
	return rec, nil
}

func (s *Server) expireMediaControls(now time.Time) {
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	for _, rec := range s.mediaControls.Expire(now) {
		if updated, changed, e := s.rooms.ApplyControl(rec); e == nil && changed {
			s.emitRoomChanged(updated)
		}
		s.emitCommandStatus(rec.Command.CommandID, rec.Command.RoomID, "FAILED", "timeout")
	}
}

// No public send-media-byte operations are added. The Gateway and TV perform
// control effects locally and confirm actual execution on this same WS.
