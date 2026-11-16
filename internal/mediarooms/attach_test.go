package mediarooms

import (
	"errors"
	"testing"
)

func TestAttachLifecycleFencesStaleEpoch(t *testing.T) {
	m := New(2)
	q := PlayRequest{CommandID: "x", Target: Target{NodeID: "node", DeviceID: "tv"}, Source: Source{StorageNodeID: "nas", FileID: "film"}}
	o, e := m.Play(q, "node", "homecloud-media-gateway", "ws://192.168.1.5:8091/ws/stream")
	if e != nil {
		t.Fatal(e)
	}
	id, p := o.Snapshot.RoomID, o.Snapshot.PlaybackID
	if _, e = m.UpdateAttach(id, p, 1, 1, "READY"); !errors.Is(e, ErrInvalid) {
		t.Fatalf("missing ATTACHING: %v", e)
	}
	if _, e = m.UpdateAttach(id, p, 1, 1, "ATTACHING"); e != nil {
		t.Fatal(e)
	}
	s, e := m.UpdateAttach(id, p, 1, 1, "READY")
	if e != nil || s.TVAttach != "READY" || s.RoomState != "CONNECTING" {
		t.Fatalf("%+v %v", s, e)
	}
	_, e = m.UpdateAttach(id, p, 1, 2, "ATTACHING")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = m.UpdateAttach(id, p, 1, 1, "DISCONNECTED"); !errors.Is(e, ErrStale) {
		t.Fatalf("old socket mutated: %v", e)
	}
	if _, e = m.UpdateAttach(id, p, 1, 2, "READY"); e != nil {
		t.Fatal(e)
	}
	if _, e = m.UpdateAttach(id, p, 1, 2, "DISCONNECTED"); e != nil {
		t.Fatal(e)
	}
	q.CommandID = "y"
	q.RoomID = id
	q.Source.FileID = "film2"
	next, e := m.Play(q, "node", "homecloud-media-gateway", "ws://192.168.1.5:8091/ws/stream")
	if e != nil {
		t.Fatal(e)
	}
	if next.Snapshot.TVAttach != "PENDING" {
		t.Fatal("attach not reset on new playback")
	}
	if _, e = m.UpdateAttach(id, p, 1, 3, "ATTACHING"); !errors.Is(e, ErrStale) {
		t.Fatalf("old playback modified state: %v", e)
	}
}
