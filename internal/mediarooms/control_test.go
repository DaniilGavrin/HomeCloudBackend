package mediarooms

import (
	"HomeCloudBackend/internal/roomcontrol"
	"testing"
)

func TestControlProjectionRequiresExecutionAndNoPrematurePlaying(t *testing.T) {
	m, s := prepared(t)
	val := 37
	cmd := roomcontrol.Command{RoomID: s.RoomID, PlaybackID: s.PlaybackID, Generation: s.Generation, ConnectionEpoch: s.AttachEpoch, CommandID: "control-1", CommandSequence: 1, Action: "volume", Origin: "ui", Volume: &val}
	rec := roomcontrol.Record{Command: cmd, Status: "DELIVERED"}
	updated, changed, e := m.ApplyControl(rec)
	if e != nil || !changed || updated.Volume != nil || updated.PlaybackState == "PLAYING" {
		t.Fatal(updated, e)
	}
	rec.Status = "EXECUTED"
	updated, changed, e = m.ApplyControl(rec)
	if e != nil || !changed || updated.Volume == nil || *updated.Volume != 37 {
		t.Fatal(updated, e)
	}
	updated, changed, e = m.ApplyControl(rec)
	if e != nil || changed {
		t.Fatal("replay wasn't idempotent", e)
	}
	cmd.CommandSequence = 2
	cmd.CommandID = "control-2"
	cmd.Action = "pause"
	cmd.Volume = nil
	rec = roomcontrol.Record{Command: cmd, Status: "EXECUTED"}
	updated, _, e = m.ApplyControl(rec)
	if e != nil || updated.PlaybackState != "PAUSED" {
		t.Fatal(updated, e)
	}
	cmd.CommandSequence = 3
	cmd.CommandID = "control-3"
	cmd.Action = "resume"
	rec.Command = cmd
	updated, _, e = m.ApplyControl(rec)
	if e != nil || updated.PlaybackState == "PLAYING" {
		t.Fatal(updated, e)
	}
}
