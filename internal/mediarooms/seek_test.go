package mediarooms

import (
	"errors"
	"testing"
)

func preparedForSeek(t *testing.T) (*Manager, Snapshot) {
	t.Helper()
	m, s := prepared(t)
	_, _, err := m.ReportExecution(ExecutionReport{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: 1, Participant: "gateway", ReportSequence: 1, State: "STREAMING", SessionID: 99})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = m.ReportExecution(ExecutionReport{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: 1, Participant: "tv", ReportSequence: 1, State: "PLAYING", PositionMS: 4500, SessionID: 11})
	if err != nil {
		t.Fatal(err)
	}
	s, err = m.Get(s.RoomID)
	if err != nil {
		t.Fatal(err)
	}
	return m, s
}

func TestSeekChangesGenerationWithoutChangingRoomOrPlayback(t *testing.T) {
	m, room := preparedForSeek(t)
	request := SeekRequest{CommandID: "seek-1", RoomID: room.RoomID, PlaybackID: room.PlaybackID, Generation: room.Generation, PositionMS: 91000}
	oldSequence := room.Sequence
	result, err := m.Seek(request)
	if err != nil || !result.Fresh {
		t.Fatalf("seek: %+v %v", result, err)
	}
	next := result.Snapshot
	if next.RoomID != room.RoomID || next.PlaybackID != room.PlaybackID || next.Generation != room.Generation+1 || next.Sequence <= oldSequence || next.SeekTargetMS == nil || *next.SeekTargetMS != 91000 || next.SeekSourceMS != nil || next.PlaybackState != "PREPARING" || next.TVAttach != "PENDING" {
		t.Fatalf("invalid seek %+v", next)
	}
	again, err := m.Seek(request)
	if err != nil || again.Fresh || again.Snapshot.Generation != next.Generation {
		t.Fatalf("not idempotent %+v %v", again, err)
	}
	request.PositionMS = 92000
	if _, err := m.Seek(request); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused command ID accepted: %v", err)
	}
	request.CommandID = "seek-2"
	request.PositionMS = 91000
	if _, err := m.Seek(request); !errors.Is(err, ErrStale) {
		t.Fatalf("stale generation accepted: %v", err)
	}
	if len(m.List()) != 1 {
		t.Fatal("seek deleted room")
	}
}
func TestSeekSourceAccuracyIsDistinctFromTVPosition(t *testing.T) {
	m, s := preparedForSeek(t)
	original := s.PositionMS
	q := SeekRequest{CommandID: "seek", RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, PositionMS: 12000}
	outcome, err := m.Seek(q)
	if err != nil {
		t.Fatal(err)
	}
	seek := outcome.Snapshot
	if _, err = m.UpdateAttach(seek.RoomID, seek.PlaybackID, seek.Generation, 2, "ATTACHING"); err != nil {
		t.Fatal(err)
	}
	if _, err = m.UpdateAttach(seek.RoomID, seek.PlaybackID, seek.Generation, 2, "READY"); err != nil {
		t.Fatal(err)
	}
	if _, _, err = m.ReportSeekSource(seek.RoomID, seek.PlaybackID, s.Generation, 1, 10000); !errors.Is(err, ErrStale) {
		t.Fatalf("old source accepted %v", err)
	}
	now, fresh, err := m.ReportSeekSource(seek.RoomID, seek.PlaybackID, seek.Generation, 2, 10400)
	if err != nil || !fresh || now.PositionMS != original || now.SeekSourceMS == nil || *now.SeekSourceMS != 10400 {
		t.Fatalf("source incorrectly confirmed TV position %+v %v", now, err)
	}
	dup, fresh, err := m.ReportSeekSource(seek.RoomID, seek.PlaybackID, seek.Generation, 2, 10400)
	if err != nil || fresh || dup.Revision != now.Revision {
		t.Fatal("duplicate source report mutated room", err)
	}
	if _, _, err = m.ReportSeekSource(seek.RoomID, seek.PlaybackID, seek.Generation, 2, 15000); !errors.Is(err, ErrStale) {
		t.Fatalf("conflicting source report accepted %v", err)
	}
}
func TestSeekRangeBounds(t *testing.T) {
	m, s := preparedForSeek(t)
	q := SeekRequest{CommandID: "x", RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, PositionMS: -1}
	if _, err := m.Seek(q); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
	q.PositionMS = MaxSeekMS + 1
	if _, err := m.Seek(q); !errors.Is(err, ErrInvalid) {
		t.Fatal(err)
	}
}
