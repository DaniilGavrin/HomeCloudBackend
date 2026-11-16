package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

// Two independent UI subscribers receive exactly the same authoritative
// revision and can rejoin by subscribing before taking a full snapshot.
func TestTwoPanelsSnapshotAndStateBroadcast(t *testing.T) {
	s, room := heartbeatFixture(t)
	for _, side := range []string{"tv", "gateway"} {
		if _, err := s.rooms.ParticipantAcknowledged(room.RoomID, room.PlaybackID, side, "APPLIED"); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []string{"ATTACHING", "READY"} {
		if _, err := s.rooms.UpdateAttach(room.RoomID, room.PlaybackID, room.Generation, 1, phase); err != nil {
			t.Fatal(err)
		}
	}
	uiA := make(chan commands.Outbound, 8)
	uiB := make(chan commands.Outbound, 8)
	subA := s.events.Attach("panel-A", func(_ context.Context, m commands.Outbound) error { uiA <- m; return nil })
	subB := s.events.Attach("panel-B", func(_ context.Context, m commands.Outbound) error { uiB <- m; return nil })
	defer s.events.Detach(subA)
	defer s.events.Detach(subB)
	if _, err := s.events.Subscribe(subA, "media.room.changed"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.events.Subscribe(subB, "media.room.changed"); err != nil {
		t.Fatal(err)
	}
	initial, err := s.mediaRoomsSnapshot(context.Background(), commands.Inbound{}, commands.ClientContext{DeviceID: "panel-A", ClientType: "desktop"})
	if err != nil {
		t.Fatal(err)
	}
	first := initial.(mediarooms.SnapshotBundle)
	if len(first.Rooms) != 1 || first.Rooms[0].PlaybackState == "PLAYING" {
		t.Fatalf("bad initial snapshot %+v", first)
	}
	report := func(role, state string, seq uint64, session int64, position int64) error {
		payload, _ := json.Marshal(map[string]any{"room_id": room.RoomID, "playback_id": room.PlaybackID, "generation": room.Generation, "connection_epoch": 1, "participant": role, "report_sequence": seq, "state": state, "position_ms": position})
		id, kind := "tv-1", "tv"
		if role == "gateway" {
			id = "homecloud-media-gateway"
			kind = "media_gateway"
		}
		_, err := s.mediaRoomReport(context.Background(), commands.Inbound{Data: payload}, commands.ClientContext{DeviceID: id, ClientType: kind, SessionID: session})
		return err
	}
	if err := report("tv", "PLAYING", 1, 11, 1234); err != nil {
		t.Fatal(err)
	}
	v1 := <-uiA
	v2 := <-uiB
	a := v1.Data.(map[string]any)["payload"].(mediarooms.Snapshot)
	b := v2.Data.(map[string]any)["payload"].(mediarooms.Snapshot)
	if a.Sequence != b.Sequence || a.PlaybackState == "PLAYING" {
		t.Fatal("premature playing", a, b)
	}
	if err := report("gateway", "STREAMING", 1, 99, 0); err != nil {
		t.Fatal(err)
	}
	a = (<-uiA).Data.(map[string]any)["payload"].(mediarooms.Snapshot)
	b = (<-uiB).Data.(map[string]any)["payload"].(mediarooms.Snapshot)
	if a.PlaybackState != "PLAYING" || b.PlaybackState != "PLAYING" || a.Sequence != b.Sequence {
		t.Fatal("diverging panels", a, b)
	}
	if err := report("tv", "PAUSED", 2, 11, 1300); err != nil {
		t.Fatal(err)
	}
	a = (<-uiA).Data.(map[string]any)["payload"].(mediarooms.Snapshot)
	_ = <-uiB
	if a.PlaybackState != "PAUSED" || a.PositionMS != 1300 {
		t.Fatal("pause didn't broadcast", a)
	}
	if err := report("tv", "BUFFERING", 3, 11, 75500); err != nil {
		t.Fatal(err)
	}
	_ = <-uiA
	_ = <-uiB
	snapshot, err := s.mediaRoomsSnapshot(context.Background(), commands.Inbound{}, commands.ClientContext{DeviceID: "panel-B", ClientType: "desktop"})
	if err != nil {
		t.Fatal(err)
	}
	now := snapshot.(mediarooms.SnapshotBundle)
	if len(now.Rooms) != 1 || now.Rooms[0].PlaybackState != "BUFFERING" || now.Rooms[0].PositionMS != 75500 || now.Sequence <= first.Sequence {
		t.Fatalf("reconnect snapshot %+v", now)
	}
	// A replay from an older TV report must not rewrite either UI.
	if err := report("tv", "PLAYING", 2, 11, 300); !errors.Is(err, mediarooms.ErrStale) {
		t.Fatalf("accepted stale report %v", err)
	}
	if len(uiA) != 0 || len(uiB) != 0 {
		t.Fatal("stale event published")
	}
}
