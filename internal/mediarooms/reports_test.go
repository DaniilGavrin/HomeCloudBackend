package mediarooms

import (
	"errors"
	"testing"
)

func prepared(t *testing.T) (*Manager, Snapshot) {
	t.Helper()
	m := New(2)
	t.Cleanup(m.Close)
	p, err := m.Play(PlayRequest{CommandID: "cmd-1", Target: Target{"node", "tv"}, Source: Source{"nas", "media"}}, "node", "homecloud-media-gateway", "ws://192.168.39.11:8091/ws/stream")
	if err != nil {
		t.Fatal(err)
	}
	s := p.Snapshot
	if _, err = m.ParticipantAcknowledged(s.RoomID, s.PlaybackID, "tv", "APPLIED"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.ParticipantAcknowledged(s.RoomID, s.PlaybackID, "gateway", "APPLIED"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.UpdateAttach(s.RoomID, s.PlaybackID, s.Generation, 1, "ATTACHING"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.UpdateAttach(s.RoomID, s.PlaybackID, s.Generation, 1, "READY"); err != nil {
		t.Fatal(err)
	}
	s, err = m.Get(s.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}

func TestTwoIndependentReportsGatePLAYINGAndProgress(t *testing.T) {
	m, s := prepared(t)
	base := ExecutionReport{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: 1, Participant: "gateway", ReportSequence: 1, State: "STREAMING", SessionID: 99}
	updated, fresh, err := m.ReportExecution(base)
	if err != nil || !fresh || updated.PlaybackState == "PLAYING" {
		t.Fatalf("early PLAYING: %+v %v", updated, err)
	}
	base.Participant = "tv"
	base.SessionID = 11
	base.State = "PLAYING"
	base.PositionMS = 1500
	updated, fresh, err = m.ReportExecution(base)
	if err != nil || !fresh || updated.PlaybackState != "PLAYING" || updated.PositionMS != 1500 {
		t.Fatalf("not PLAYING: %+v %v", updated, err)
	}
	if updated.CommandID != "cmd-1" || updated.Sequence == 0 {
		t.Fatal("missing version/command")
	}
	snap := m.Snapshot()
	if snap.Sequence != updated.Sequence || len(snap.Rooms) != 1 || snap.Rooms[0].Revision != updated.Revision {
		t.Fatalf("snapshot %+v", snap)
	}
	again, changed, err := m.ReportExecution(base)
	if err != nil || changed || again.Revision != updated.Revision {
		t.Fatal("non-idempotent duplicate", err)
	}
	base.ReportSequence = 2
	base.State = "PAUSED"
	base.PositionMS = 1700
	updated, _, err = m.ReportExecution(base)
	if err != nil || updated.PlaybackState != "PAUSED" || updated.PositionMS != 1700 {
		t.Fatalf("pause %+v %v", updated, err)
	}
	base.ReportSequence = 3
	base.State = "BUFFERING"
	base.PositionMS = 75000
	updated, _, err = m.ReportExecution(base)
	if err != nil || updated.PlaybackState != "BUFFERING" || updated.PositionMS != 75000 {
		t.Fatalf("seek progress %+v %v", updated, err)
	}
	base.ReportSequence = 2
	base.State = "PLAYING"
	if _, _, err = m.ReportExecution(base); !errors.Is(err, ErrStale) {
		t.Fatalf("old seek state applied: %v", err)
	}
	base.ReportSequence = 4
	base.State = "ENDED"
	base.PositionMS = 80000
	updated, _, err = m.ReportExecution(base)
	if err != nil || updated.PlaybackState != "ENDED" || updated.RoomState != "ACTIVE" {
		t.Fatalf("EOF closed room: %+v %v", updated, err)
	}
	base.ReportSequence = 5
	base.State = "IDLE"
	updated, _, err = m.ReportExecution(base)
	if err != nil || updated.PlaybackState != "IDLE" || len(m.List()) != 1 {
		t.Fatalf("IDLE closed room: %+v %v", updated, err)
	}
}

func TestGenerationEpochSessionAndErrorFences(t *testing.T) {
	m, s := prepared(t)
	q := ExecutionReport{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: 1, Participant: "tv", ReportSequence: 1, State: "PAUSED", SessionID: 100}
	if _, _, err := m.ReportExecution(q); err != nil {
		t.Fatal(err)
	}
	q.SessionID = 99
	q.ReportSequence = 100
	if _, _, err := m.ReportExecution(q); !errors.Is(err, ErrStale) {
		t.Fatal("old session accepted", err)
	}
	q.SessionID = 101
	q.ReportSequence = 1
	if _, _, err := m.ReportExecution(q); err != nil {
		t.Fatal("reconnect report failed", err)
	}
	q.ConnectionEpoch = 2
	if _, _, err := m.ReportExecution(q); !errors.Is(err, ErrStale) {
		t.Fatal("future attach accepted", err)
	}
	q.ConnectionEpoch = 1
	q.Generation++
	if _, _, err := m.ReportExecution(q); !errors.Is(err, ErrStale) {
		t.Fatal("old playback fence failed", err)
	}
	q.Generation = s.Generation
	q.ReportSequence = 2
	q.State = "ERROR"
	q.ErrorCode = "FILE_UNAVAILABLE"
	updated, _, err := m.ReportExecution(q)
	if err != nil || updated.PlaybackState != "ERROR" || updated.ErrorCode != "FILE_UNAVAILABLE" {
		t.Fatalf("ERROR report %+v %v", updated, err)
	}
	q.ReportSequence = 3
	q.ErrorCode = "../../secret"
	if _, _, err := m.ReportExecution(q); !errors.Is(err, ErrInvalid) {
		t.Fatalf("invalid error code: %v", err)
	}
	q.ErrorCode = "FILE_UNAVAILABLE"
	// Switch resets both confirmations and fences the old playback ID.
	changed, err := m.Play(PlayRequest{CommandID: "cmd-2", RoomID: s.RoomID, Target: s.Target, Source: Source{"nas", "next"}}, "node", "homecloud-media-gateway", "ws://192.168.39.11:8091/ws/stream")
	if err != nil {
		t.Fatal(err)
	}
	if changed.Snapshot.PlaybackState != "PREPARING" || changed.Snapshot.TVExecution != "" || changed.Snapshot.PositionMS != 0 {
		t.Fatal("switch leaked old state")
	}
	if _, _, err := m.ReportExecution(q); !errors.Is(err, ErrStale) {
		t.Fatal("old playback rewrote new", err)
	}
}

func TestSnapshotSequenceAcrossClose(t *testing.T) {
	m, s := prepared(t)
	a := m.Snapshot()
	if a.InstanceID == "" || a.SchemaVersion != 1 || len(a.Rooms) != 1 {
		t.Fatalf("invalid snapshot %+v", a)
	}
	closed, _, err := m.Stop("cmd-stop", s.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	b := m.Snapshot()
	if b.InstanceID != a.InstanceID || b.Sequence <= a.Sequence || len(b.Rooms) != 0 || closed.Sequence != b.Sequence {
		t.Fatalf("close sequence %+v %+v", a, b)
	}
}

func TestReconnectRevokesOldPlaybackEvidence(t *testing.T) {
	m, s := prepared(t)
	gw := ExecutionReport{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: 1, Participant: "gateway", ReportSequence: 1, State: "STREAMING", SessionID: 99}
	tv := gw
	tv.Participant = "tv"
	tv.State = "PLAYING"
	tv.SessionID = 11
	if _, _, err := m.ReportExecution(gw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := m.ReportExecution(tv); err != nil {
		t.Fatal(err)
	}
	gw.ReportSequence = 2
	gw.State = "ENDED"
	updated, _, err := m.ReportExecution(gw)
	if err != nil || updated.PlaybackState != "PLAYING" {
		t.Fatalf("EOF from sender interrupted decoding: %+v %v", updated, err)
	}
	updated, err = m.UpdateAttach(s.RoomID, s.PlaybackID, s.Generation, 2, "ATTACHING")
	if err != nil || updated.PlaybackState == "PLAYING" || updated.TVExecution != "" || updated.GatewayExecution != "" {
		t.Fatalf("new epoch inherited old proof: %+v %v", updated, err)
	}
	if _, _, err := m.ReportExecution(tv); !errors.Is(err, ErrStale) {
		t.Fatalf("old epoch reused: %v", err)
	}
	if _, err = m.UpdateAttach(s.RoomID, s.PlaybackID, s.Generation, 2, "READY"); err != nil {
		t.Fatal(err)
	}
	gw.ConnectionEpoch = 2
	gw.ReportSequence = 1
	gw.State = "STREAMING"
	updated, _, err = m.ReportExecution(gw)
	if err != nil || updated.PlaybackState == "PLAYING" {
		t.Fatal("new Gateway alone caused PLAYING", err)
	}
	tv.ConnectionEpoch = 2
	tv.ReportSequence = 1
	updated, _, err = m.ReportExecution(tv)
	if err != nil || updated.PlaybackState != "PLAYING" {
		t.Fatal("new TV+Gateway failed", err)
	}
}
