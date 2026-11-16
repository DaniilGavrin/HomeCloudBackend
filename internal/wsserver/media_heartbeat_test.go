package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediagateway"
	"HomeCloudBackend/internal/mediarooms"
	"HomeCloudBackend/internal/roomheartbeat"
	"HomeCloudBackend/wsconfig"
)

func heartbeatFixture(t *testing.T) (*Server, mediarooms.Snapshot) {
	t.Helper()
	cfg := &wsconfig.Config{}
	s := New(cfg, nil, slog.Default())
	t.Cleanup(s.CloseAll)
	s.SetLocalNodeID("node-1")
	_, err := s.gateways.Register(mediagateway.Registration{
		GatewayID: "homecloud-media-gateway", SessionID: 99,
		Endpoints:                []mediagateway.Endpoint{{Network: "lan", URL: "ws://192.168.1.2:8091/ws/stream"}},
		Capabilities:             mediagateway.Capabilities{MediaProtocol: 1, BinaryFrame: "HCM1", DirectWS: true},
		HeartbeatIntervalSeconds: 20, HeartbeatGraceSeconds: 10,
	}, 8091, func(netip.Addr) bool { return true })
	if err != nil {
		t.Fatal(err)
	}
	o, err := s.rooms.Play(mediarooms.PlayRequest{CommandID: "cmd", Target: mediarooms.Target{NodeID: "node-1", DeviceID: "tv-1"}, Source: mediarooms.Source{StorageNodeID: "s", FileID: "file"}}, "node-1", "homecloud-media-gateway", "ws://192.168.1.2:8091/ws/stream")
	if err != nil {
		t.Fatal(err)
	}
	if err = s.heartbeats.Start(o.Snapshot.RoomID, o.Snapshot.PlaybackID, o.Snapshot.Generation, 99, 20*time.Second, 10*time.Second, time.Now()); err != nil {
		t.Fatal(err)
	}
	return s, o.Snapshot
}
func TestMediaRoomHeartbeatAuthenticatesRolesAndGeneration(t *testing.T) {
	s, room := heartbeatFixture(t)
	req := map[string]any{"room_id": room.RoomID, "playback_id": room.PlaybackID, "generation": room.Generation, "participant": "tv"}
	b, _ := json.Marshal(req)
	input := commands.Inbound{V: 1, Type: "command", Action: "media.room.heartbeat", Data: b}
	tv := commands.ClientContext{DeviceID: "tv-1", ClientType: "tv", SessionID: 11}
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, tv); err != nil {
		t.Fatal(err)
	}
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, commands.ClientContext{DeviceID: "other", ClientType: "tv", SessionID: 12}); err == nil {
		t.Fatal("wrong TV accepted")
	}
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, commands.ClientContext{DeviceID: "homecloud-media-gateway", ClientType: "media_gateway", SessionID: 99}); err == nil {
		t.Fatal("Gateway impersonated TV")
	}
	req["participant"] = "gateway"
	b, _ = json.Marshal(req)
	input.Data = b
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, commands.ClientContext{DeviceID: "homecloud-media-gateway", ClientType: "media_gateway", SessionID: 98}); err == nil {
		t.Fatal("wrong Gateway session accepted")
	}
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, commands.ClientContext{DeviceID: "homecloud-media-gateway", ClientType: "media_gateway", SessionID: 99}); err != nil {
		t.Fatal(err)
	}
	req["generation"] = 999
	b, _ = json.Marshal(req)
	input.Data = b
	if _, err := s.mediaRoomHeartbeat(context.Background(), input, commands.ClientContext{DeviceID: "homecloud-media-gateway", ClientType: "media_gateway", SessionID: 99}); !errors.Is(err, mediarooms.ErrStale) {
		t.Fatal(err)
	}
}
func TestMediaHeartbeatTimeoutEmitsCleanup(t *testing.T) {
	s, room := heartbeatFixture(t)
	base := time.Now()
	s.heartbeats.Remove(room.RoomID)
	if err := s.heartbeats.Start(room.RoomID, room.PlaybackID, room.Generation, 99, roomheartbeat.DefaultInterval, roomheartbeat.Grace, base); err != nil {
		t.Fatal(err)
	}
	_, _ = s.heartbeats.Beat(room.RoomID, room.PlaybackID, room.Generation, "gateway", 99, base.Add(20*time.Second))
	s.expireMediaHeartbeats(base.Add(30 * time.Second))
	if _, err := s.rooms.Get(room.RoomID); err != nil {
		t.Fatalf("expired early: %v", err)
	}
	s.expireMediaHeartbeats(base.Add(30*time.Second + time.Millisecond))
	if _, err := s.rooms.Get(room.RoomID); !errors.Is(err, mediarooms.ErrNotFound) {
		t.Fatalf("room not cleaned up: %v", err)
	}
	if _, ok := s.heartbeats.Get(room.RoomID); ok {
		t.Fatal("heartbeat state leaked")
	}
}
