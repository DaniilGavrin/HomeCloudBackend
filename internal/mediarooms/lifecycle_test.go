package mediarooms

import (
	"errors"
	"testing"
)

func TestKAN54EOFStopReplayAndFreshRoom(t *testing.T) {
	m, r := prepared(t)
	start := m.Snapshot()
	if len(start.Rooms) != 1 || start.Rooms[0].RoomID != r.RoomID {
		t.Fatal("active room missing")
	}
	tv := ExecutionReport{RoomID: r.RoomID, PlaybackID: r.PlaybackID, Generation: r.Generation,
		ConnectionEpoch: r.AttachEpoch, Participant: "tv", ReportSequence: 1, State: "ENDED", PositionMS: 87500, SessionID: 101}
	ended, changed, err := m.ReportExecution(tv)
	if err != nil || !changed || ended.RoomState != "ACTIVE" || ended.PlaybackState != "ENDED" || ended.PositionMS != 87500 {
		t.Fatalf("EOF closed room: %+v changed=%v err=%v", ended, changed, err)
	}
	if len(m.List()) != 1 {
		t.Fatal("EOF removed active room")
	}
	zombie := tv
	zombie.ReportSequence = 2
	zombie.State = "PLAYING"
	if _, _, err := m.ReportExecution(zombie); !errors.Is(err, ErrStale) {
		t.Fatalf("late TV PLAYING resurrected EOF: %v", err)
	}
	idle := tv
	idle.ReportSequence = 2
	idle.State = "IDLE"
	state, changed, err := m.ReportExecution(idle)
	if err != nil || !changed || state.PlaybackState != "IDLE" || state.RoomState != "ACTIVE" {
		t.Fatalf("idle %+v %v", state, err)
	}
	a, fresh, err := m.Stop("stop-1", r.RoomID)
	if err != nil || !fresh || a.RoomState != "CLOSED" || a.CloseReason != "STOP" {
		t.Fatalf("stop %+v %v", a, err)
	}
	b, fresh, err := m.Stop("stop-1", r.RoomID)
	if err != nil || fresh || a != b {
		t.Fatal("same command repeated stop not idempotent", err)
	}
	c, fresh, err := m.Stop("stop-from-second-panel", r.RoomID)
	if err != nil || fresh || a != c {
		t.Fatal("different command repeated stop not idempotent", err)
	}
	if len(m.List()) != 0 {
		t.Fatal("stopped room still active")
	}
	if _, _, err = m.ReportExecution(idle); !errors.Is(err, ErrNotFound) {
		t.Fatalf("late EOF changed closed room: %v", err)
	}
	next, err := m.Play(PlayRequest{CommandID: "play-next", Target: r.Target, Source: Source{StorageNodeID: "new-storage", FileID: "new-film"}}, r.Target.NodeID, r.GatewayID, r.GatewayWSURL)
	if err != nil || next.Snapshot.RoomID == r.RoomID {
		t.Fatalf("new viewing must use new room ID: %+v %v", next, err)
	}
	if _, _, err = m.Stop("stop-from-second-panel", next.Snapshot.RoomID); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused command ID accepted on another room: %v", err)
	}
}

func TestKAN54StopTombstonesAreBounded(t *testing.T) {
	m := New(1)
	defer m.Close()
	var first string
	for i := 0; i < stoppedHistoryLimit+1; i++ {
		p, err := m.Play(PlayRequest{CommandID: "play-" + string(rune(i+0x100)), Target: Target{NodeID: "node", DeviceID: "tv"}, Source: Source{StorageNodeID: "ss", FileID: "f"}}, "node", "gw", "ws://127.0.0.1:8091/ws/stream")
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = p.Snapshot.RoomID
		}
		if _, _, err = m.Stop("stop-"+string(rune(i+0x100)), p.Snapshot.RoomID); err != nil {
			t.Fatal(err)
		}
	}
	if len(m.stopped) != stoppedHistoryLimit {
		t.Fatalf("unbounded tombstones: %d", len(m.stopped))
	}
	if _, ok := m.ClosedRoom(first); ok {
		t.Fatal("old tombstone not evicted")
	}
}
