package wsserver

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/connections"
)

func TestControlDeliveryAckTVRemoteAndNoEcho(t *testing.T) {
	s, room := heartbeatFixture(t)
	for _, side := range []string{"tv", "gateway"} {
		if _, e := s.rooms.ParticipantAcknowledged(room.RoomID, room.PlaybackID, side, "APPLIED"); e != nil {
			t.Fatal(e)
		}
	}
	for _, phase := range []string{"ATTACHING", "READY"} {
		if _, e := s.rooms.UpdateAttach(room.RoomID, room.PlaybackID, room.Generation, 1, phase); e != nil {
			t.Fatal(e)
		}
	}
	tvDelivery := make(chan []byte, 5)
	gwDelivery := make(chan []byte, 5)
	for _, side := range []struct {
		id, kind string
		delivery chan []byte
	}{{"tv-1", "tv", tvDelivery}, {"homecloud-media-gateway", "media_gateway", gwDelivery}} {
		target := side
		p := connections.NewPeer(side.id, side.kind, 16, time.Second, func(_ context.Context, data []byte) error { target.delivery <- data; return nil }, func() {})
		if e := s.clients.Register(p); e != nil {
			t.Fatal(e)
		}
		go p.RunWriter(context.Background())
		defer s.clients.Unregister(p)
	}
	issue := func(id, action string, extra map[string]any, caller commands.ClientContext) (map[string]any, error) {
		data := map[string]any{"room_id": room.RoomID, "playback_id": room.PlaybackID, "generation": room.Generation, "command_id": id, "action": action}
		for k, v := range extra {
			data[k] = v
		}
		b, _ := json.Marshal(data)
		res, err := s.mediaRoomControl(context.Background(), commands.Inbound{Data: b}, caller)
		if err != nil {
			return nil, err
		}
		encoded, _ := json.Marshal(res)
		var decoded map[string]any
		_ = json.Unmarshal(encoded, &decoded)
		return decoded, nil
	}
	ui := commands.ClientContext{DeviceID: "panel", ClientType: "desktop", SessionID: 200}
	first, err := issue("volume-1", "volume", map[string]any{"volume": 37}, ui)
	if err != nil || first["status"] != "DELIVERED" {
		t.Fatal(first, err)
	}
	select {
	case <-tvDelivery:
	default:
		t.Fatal("TV never received its command")
	}
	select {
	case <-gwDelivery:
		t.Fatal("volume must never reach Gateway")
	default:
	}
	b, _ := json.Marshal(map[string]any{"room_id": room.RoomID, "playback_id": room.PlaybackID, "generation": room.Generation, "connection_epoch": 1, "command_id": "volume-1", "command_sequence": 1, "participant": "tv", "phase": "APPLIED"})
	ack, err := s.mediaRoomControlAck(context.Background(), commands.Inbound{Data: b}, commands.ClientContext{DeviceID: "tv-1", ClientType: "tv", SessionID: 11})
	if err != nil || ack == nil {
		t.Fatal(err)
	}
	snapshot, _ := s.rooms.Get(room.RoomID)
	if snapshot.Volume == nil || *snapshot.Volume != 37 || snapshot.ControlStatus != "EXECUTED" {
		t.Fatal(snapshot)
	}
	remote, err := issue("remote-mute", "mute", map[string]any{"muted": true}, commands.ClientContext{DeviceID: "tv-1", ClientType: "tv", SessionID: 11})
	if err != nil || remote["status"] != "EXECUTED" {
		t.Fatal(remote, err)
	}
	select {
	case <-tvDelivery:
		t.Fatal("remote action echoed back to TV")
	default:
	}
	snapshot, _ = s.rooms.Get(room.RoomID)
	if snapshot.Muted == nil || !*snapshot.Muted {
		t.Fatal(snapshot)
	}
	_ = issue
}
