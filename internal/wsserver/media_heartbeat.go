package wsserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
	"HomeCloudBackend/internal/roomheartbeat"
)

// media.room.heartbeat travels ONLY over the already-identified control WS.
// The Backend verifies device and session; a TV may never impersonate a Gateway.
func (s *Server) mediaRoomHeartbeat(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	var req struct {
		RoomID      string `json:"room_id"`
		PlaybackID  string `json:"playback_id"`
		Generation  uint32 `json:"generation"`
		Participant string `json:"participant"`
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
	if room.PlaybackID != req.PlaybackID || room.Generation != req.Generation {
		return nil, mediarooms.ErrStale
	}
	switch req.Participant {
	case "gateway":
		reg, ok := s.gateways.Get()
		if !ok || reg.GatewayID != room.GatewayID || caller.DeviceID != reg.GatewayID || caller.ClientType != "media_gateway" || caller.SessionID != reg.SessionID {
			return nil, errors.New("HEARTBEAT_REJECTED: wrong Gateway session")
		}
	case "tv":
		if caller.DeviceID != room.Target.DeviceID || (caller.ClientType != "tv" && caller.ClientType != "television") {
			return nil, errors.New("HEARTBEAT_REJECTED: wrong TV session")
		}
	default:
		return nil, roomheartbeat.ErrInvalid
	}
	rec, err := s.heartbeats.Beat(req.RoomID, req.PlaybackID, req.Generation, req.Participant, caller.SessionID, time.Now())
	if err != nil {
		return nil, err
	}
	return map[string]any{"room_id": rec.RoomID, "playback_id": rec.PlaybackID, "generation": rec.Generation, "participant": req.Participant,
		"last_seen_tv": rec.LastSeenTV.UTC(), "last_seen_gateway": rec.LastSeenGateway.UTC()}, nil
}

func (s *Server) startMediaHeartbeatWatcher() {
	s.heartbeatStart.Do(func() {
		go func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-s.heartbeatStop:
					return
				case now := <-ticker.C:
					s.expireMediaHeartbeats(now)
					s.expireMediaControls(now)
				}
			}
		}()
	})
}

func (s *Server) expireMediaHeartbeats(now time.Time) {
	s.roomDispatchMu.Lock()
	defer s.roomDispatchMu.Unlock()
	for _, expired := range s.heartbeats.Expire(now) {
		rec := expired.Record
		before, err := s.rooms.Get(rec.RoomID)
		if err != nil || before.PlaybackID != rec.PlaybackID || before.Generation != rec.Generation {
			continue
		}
		reason := "HEARTBEAT_TIMEOUT_" + strings.ToUpper(expired.Participant)
		lost, err := s.rooms.MarkLost(rec.RoomID)
		if err != nil {
			continue
		}
		s.emitRoomChanged(lost)
		_, _ = s.PublishEvent(context.Background(), "media.room.lost", map[string]any{"room_id": rec.RoomID, "participant": expired.Participant, "reason": reason, "revision": lost.Revision, "sequence": lost.Sequence, "instance_id": lost.InstanceID})
		s.mediaControls.Remove(rec.RoomID)
		closed, ok := s.rooms.Fail(rec.RoomID, reason)
		if !ok {
			continue
		}
		event := commands.Outbound{V: 1, Type: "event", Topic: "media.room.closed", Data: map[string]any{"room_id": closed.RoomID, "reason": reason, "revision": closed.Revision}}
		for _, device := range []string{before.Target.DeviceID, before.GatewayID} {
			sendCtx, cancel := context.WithTimeout(context.Background(), time.Second)
			_ = s.clients.QueueToDevice(sendCtx, device, event)
			cancel()
		}
		s.emitRoomClosed(closed)
		s.log.Warn("Media room lost: missed independent heartbeat", "room_id", rec.RoomID, "participant", expired.Participant, "reason", reason)
	}
}

func (s *Server) heartbeatStartForRoom(room mediarooms.Snapshot, session int64, interval, grace time.Duration) error {
	if err := s.heartbeats.Start(room.RoomID, room.PlaybackID, room.Generation, session, interval, grace, time.Now()); err != nil {
		return fmt.Errorf("start room heartbeat: %w", err)
	}
	s.startMediaHeartbeatWatcher()
	return nil
}
