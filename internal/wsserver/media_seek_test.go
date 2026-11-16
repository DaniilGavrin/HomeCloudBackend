package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediarooms"
)

func TestSeekSourceRejectsImpersonationAndStaleGeneration(t *testing.T) {
	s, initial := heartbeatFixture(t)
	for _, role := range []string{"tv", "gateway"} {
		if _, err := s.rooms.ParticipantAcknowledged(initial.RoomID, initial.PlaybackID, role, "APPLIED"); err != nil {
			t.Fatal(err)
		}
	}
	for _, phase := range []string{"ATTACHING", "READY"} {
		if _, err := s.rooms.UpdateAttach(initial.RoomID, initial.PlaybackID, initial.Generation, 1, phase); err != nil {
			t.Fatal(err)
		}
	}
	_, _, err := s.rooms.ReportExecution(mediarooms.ExecutionReport{RoomID: initial.RoomID, PlaybackID: initial.PlaybackID, Generation: initial.Generation, ConnectionEpoch: 1, Participant: "gateway", ReportSequence: 1, State: "STREAMING", SessionID: 99})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = s.rooms.ReportExecution(mediarooms.ExecutionReport{RoomID: initial.RoomID, PlaybackID: initial.PlaybackID, Generation: initial.Generation, ConnectionEpoch: 1, Participant: "tv", ReportSequence: 1, State: "PLAYING", SessionID: 12})
	if err != nil {
		t.Fatal(err)
	}
	outcome, err := s.rooms.Seek(mediarooms.SeekRequest{RoomID: initial.RoomID, PlaybackID: initial.PlaybackID, Generation: initial.Generation, PositionMS: 30000, CommandID: "seek-1"})
	if err != nil {
		t.Fatal(err)
	}
	next := outcome.Snapshot
	for _, phase := range []string{"ATTACHING", "READY"} {
		if _, err := s.rooms.UpdateAttach(next.RoomID, next.PlaybackID, next.Generation, 2, phase); err != nil {
			t.Fatal(err)
		}
	}
	build := func(gen uint32) commands.Inbound {
		raw, _ := json.Marshal(map[string]any{"room_id": next.RoomID, "playback_id": next.PlaybackID, "generation": gen, "connection_epoch": 2, "source_position_ms": 28900})
		return commands.Inbound{Data: raw}
	}
	if _, err := s.mediaRoomSeekSource(context.Background(), build(next.Generation), commands.ClientContext{DeviceID: "tv-1", ClientType: "tv", SessionID: 12}); err == nil {
		t.Fatal("TV impersonated Gateway")
	}
	valid := commands.ClientContext{DeviceID: "homecloud-media-gateway", ClientType: "media_gateway", SessionID: 99}
	if _, err := s.mediaRoomSeekSource(context.Background(), build(initial.Generation), valid); !errors.Is(err, mediarooms.ErrStale) {
		t.Fatalf("old generation accepted: %v", err)
	}
	if _, err := s.mediaRoomSeekSource(context.Background(), build(next.Generation), valid); err != nil {
		t.Fatal(err)
	}
	updated, err := s.rooms.Get(next.RoomID)
	if err != nil || updated.SeekSourceMS == nil || *updated.SeekSourceMS != 28900 || updated.PlaybackState == "PLAYING" {
		t.Fatalf("seek source pretended TV decoded: %+v %v", updated, err)
	}
}
