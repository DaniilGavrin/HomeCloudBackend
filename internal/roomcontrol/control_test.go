package roomcontrol

import (
	"errors"
	"testing"
	"time"
)

func TestIndependentAckReplayOrderingAndRemote(t *testing.T) {
	m := New()
	now := time.Now()
	q := Command{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 2, CommandID: "c1", Action: "pause", Origin: "ui"}
	r, f, e := m.Issue(q, now)
	if e != nil || !f || r.Status != "PENDING" {
		t.Fatal(r, f, e)
	}
	dup, f, e := m.Issue(q, now)
	if e != nil || f || dup.Command.CommandSequence != r.Command.CommandSequence {
		t.Fatal(dup, f, e)
	}
	ack := Ack{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 2, CommandID: "c1", CommandSequence: r.Command.CommandSequence, Participant: "tv", Phase: "APPLIED"}
	r, changed, e := m.Ack(ack)
	if e != nil || !changed || r.Status == "EXECUTED" {
		t.Fatal(r, e)
	}
	_, changed, e = m.Ack(ack)
	if e != nil || changed {
		t.Fatal("duplicate applied", e)
	}
	ack.Participant = "gateway"
	r, changed, e = m.Ack(ack)
	if e != nil || !changed || r.Status != "EXECUTED" {
		t.Fatal(r, e)
	}
	r2, f, e := m.Issue(Command{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 2, CommandID: "c2", Action: "resume", Origin: "ui"}, now)
	if e != nil || !f || r2.Command.CommandSequence <= r.Command.CommandSequence {
		t.Fatal(r2, e)
	}
	if _, _, e = m.Ack(ack); !errors.Is(e, ErrStale) {
		t.Fatal("old ack accepted", e)
	}
	r3, f, e := m.Issue(Command{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 2, CommandID: "remote-volume", Action: "volume", Origin: "tv", Volume: ptr(45)}, now)
	if e != nil || !f || r3.Status != "EXECUTED" {
		t.Fatal(r3, e)
	}
	r4, f, e := m.Issue(Command{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 2, CommandID: "remote-pause", Action: "pause", Origin: "tv"}, now)
	if e != nil || !f || !r4.TVApplied || r4.Status == "EXECUTED" {
		t.Fatal(r4, e)
	}
}
func ptr(n int) *int { return &n }
func TestTimeoutAndValueValidation(t *testing.T) {
	m := New()
	now := time.Now()
	q := Command{RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 1, CommandID: "c", Action: "volume", Origin: "ui", Volume: ptr(101)}
	if _, _, e := m.Issue(q, now); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	q.Volume = ptr(25)
	rec, _, e := m.Issue(q, now)
	if e != nil {
		t.Fatal(e)
	}
	a := Ack{CommandID: "c", RoomID: "r", PlaybackID: "p", Generation: 1, ConnectionEpoch: 1, CommandSequence: rec.Command.CommandSequence, Participant: "gateway", Phase: "APPLIED"}
	if _, _, e = m.Ack(a); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	if got := m.Expire(now.Add(Timeout)); len(got) != 0 {
		t.Fatal("expired early")
	}
	if got := m.Expire(now.Add(Timeout + time.Nanosecond)); len(got) != 1 || got[0].ErrorCode != "CONTROL_TIMEOUT" {
		t.Fatal(got)
	}
}
