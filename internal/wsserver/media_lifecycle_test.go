package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

func TestKAN54DecoderEOFEventAndHeartbeatSurvives(t *testing.T) {
	s, r := heartbeatFixture(t)
	for _, side := range []string{"tv", "gateway"} {
		if _, err := s.rooms.ParticipantAcknowledged(r.RoomID, r.PlaybackID, side, "APPLIED"); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []string{"ATTACHING", "READY"} {
		if _, err := s.rooms.UpdateAttach(r.RoomID, r.PlaybackID, r.Generation, 1, phase); err != nil {
			t.Fatal(err)
		}
	}
	events := make(chan commands.Outbound, 6)
	sub := s.events.Attach("panel-1", func(_ context.Context, ev commands.Outbound) error { events <- ev; return nil })
	defer s.events.Detach(sub)
	if _, err := s.events.Subscribe(sub, "media.playback.ended"); err != nil {
		t.Fatal(err)
	}
	send := func(side, state string, seq uint64, sid int64) error {
		data, _ := json.Marshal(map[string]any{"room_id": r.RoomID, "playback_id": r.PlaybackID, "generation": r.Generation, "connection_epoch": 1, "participant": side, "report_sequence": seq, "state": state, "position_ms": 0})
		kind, id := "tv", "tv-1"
		if side == "gateway" {
			kind = "media_gateway"
			id = "homecloud-media-gateway"
		}
		_, err := s.mediaRoomReport(context.Background(), commands.Inbound{Data: data}, commands.ClientContext{DeviceID: id, ClientType: kind, SessionID: sid})
		return err
	}
	if err := send("gateway", "ENDED", 1, 99); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatal("source EOF must not claim TV decoder completed")
	}
	if err := send("tv", "ENDED", 1, 17); err != nil {
		t.Fatal(err)
	}
	select {
	case ev := <-events:
		if ev.Topic != "media.playback.ended" {
			t.Fatalf("unexpected event %s", ev.Topic)
		}
		payload := ev.Data.(map[string]any)["payload"].(map[string]any)
		if payload["room_id"] != r.RoomID || payload["playback_id"] != r.PlaybackID {
			t.Fatalf("wrong EOF payload %+v", payload)
		}
	case <-time.After(time.Second):
		t.Fatal("missing EOF broadcast")
	}
	if err := send("tv", "ENDED", 1, 17); err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatal("duplicate EOF broadcast")
	}
	got, err := s.rooms.Get(r.RoomID)
	if err != nil || got.RoomState != "ACTIVE" || got.PlaybackState != "ENDED" {
		t.Fatalf("EOF closed room %+v %v", got, err)
	}
	if _, exists := s.heartbeats.Get(r.RoomID); !exists {
		t.Fatal("EOF destroyed independent heartbeat")
	}
}

func TestKAN54StopOneBroadcastAndTVIdentity(t *testing.T) {
	s, r := heartbeatFixture(t)
	events := make(chan commands.Outbound, 8)
	sub := s.events.Attach("panel", func(_ context.Context, ev commands.Outbound) error { events <- ev; return nil })
	defer s.events.Detach(sub)
	if _, err := s.events.Subscribe(sub, "media.room.closed"); err != nil {
		t.Fatal(err)
	}
	makeReq := func(id string) commands.Inbound {
		b, _ := json.Marshal(map[string]any{"room_id": r.RoomID, "command_id": id})
		return commands.Inbound{Data: b}
	}
	if _, err := s.mediaRoomStop(context.Background(), makeReq("wrong"), commands.ClientContext{ClientType: "tv", DeviceID: "neighbor-tv", SessionID: 4}); err == nil {
		t.Fatal("another TV stopped room")
	}
	if _, err := s.mediaRoomStop(context.Background(), makeReq("gateway"), commands.ClientContext{ClientType: "media_gateway", DeviceID: r.GatewayID, SessionID: 99}); err == nil {
		t.Fatal("gateway stopped room")
	}
	tv := commands.ClientContext{ClientType: "tv", DeviceID: r.Target.DeviceID, SessionID: 11}
	if _, err := s.mediaRoomStop(context.Background(), makeReq("stop1"), tv); err != nil {
		t.Fatal(err)
	}
	if _, err := s.mediaRoomStop(context.Background(), makeReq("stop2"), tv); err != nil {
		t.Fatal("repeat STOP failed", err)
	}
	if len(events) != 1 {
		t.Fatalf("STOP broadcast count %d", len(events))
	}
	if _, err := s.rooms.Get(r.RoomID); !errors.Is(err, mediarooms.ErrNotFound) {
		t.Fatal("room still active", err)
	}
	if _, exists := s.heartbeats.Get(r.RoomID); exists {
		t.Fatal("STOP left heartbeat")
	}
}
