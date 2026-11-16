package mediarooms

import (
	"errors"
	"sync"
	"testing"
)

func TestPlaySwitchStopAndDedup(t *testing.T) {
	m := New(1)
	tv := Target{"node", "tv"}
	a := PlayRequest{CommandID: "cmd1", Target: tv, Source: Source{"nas", "film1"}}
	o, e := m.Play(a, "node", "gw", "ws://192.168.1.2:8091/ws/stream")
	if e != nil || !o.Fresh || o.Snapshot.RoomState != "CONNECTING" {
		t.Fatalf("create %+v %v", o, e)
	}
	again, e := m.Play(a, "node", "gw", "ws://192.168.1.2:8091/ws/stream")
	if e != nil || again.Fresh || again.Snapshot.RoomID != o.Snapshot.RoomID {
		t.Fatal("dedup failed")
	}
	if _, e = m.Play(PlayRequest{CommandID: "other", Target: tv, Source: a.Source}, "node", "gw", "ws://192.168.1.2:8091/ws/stream"); !errors.Is(e, ErrConflict) {
		t.Fatal(e)
	}
	if _, e = m.ParticipantAcknowledged(o.Snapshot.RoomID, o.Snapshot.PlaybackID, "gateway", "APPLIED"); e != nil {
		t.Fatal(e)
	}
	s, e := m.ParticipantAcknowledged(o.Snapshot.RoomID, o.Snapshot.PlaybackID, "tv", "APPLIED")
	if e != nil || s.RoomState != "ACTIVE" || s.PlaybackState != "PREPARING" {
		t.Fatal(s, e)
	}
	a = PlayRequest{CommandID: "cmd2", RoomID: s.RoomID, Target: tv, Source: Source{"nas", "film2"}}
	b, e := m.Play(a, "node", "gw", "ws://192.168.1.2:8091/ws/stream")
	if e != nil || b.Snapshot.RoomID != s.RoomID || b.Snapshot.PlaybackID == s.PlaybackID || b.Snapshot.Generation != 2 {
		t.Fatal(b, e)
	}
	if _, e = m.ParticipantAcknowledged(s.RoomID, s.PlaybackID, "tv", "APPLIED"); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	done, fresh, e := m.Stop("stop1", s.RoomID)
	if e != nil || !fresh || done.CloseReason != "STOP" {
		t.Fatal(done, e)
	}
	_, fresh, e = m.Stop("stop1", s.RoomID)
	if e != nil || fresh {
		t.Fatal(e)
	}
	if len(m.List()) != 0 {
		t.Fatal("not closed")
	}
}
func TestConcurrentAndLost(t *testing.T) {
	m := New(4)
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "cmd" + string(rune(i+100))
			_, _ = m.Play(PlayRequest{CommandID: id, Target: Target{"node", id}, Source: Source{"nas", "film"}}, "node", "gw", "ws://192.168.1.2:8091/ws/stream")
		}(i)
	}
	wg.Wait()
	if len(m.List()) != 4 {
		t.Fatal(len(m.List()))
	}
	lost := m.LostPeer("gw")
	if len(lost) != 4 || len(m.List()) != 0 {
		t.Fatal("lost cleanup")
	}
}
