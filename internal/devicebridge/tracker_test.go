package devicebridge

import (
	"errors"
	"testing"
)

func TestLifecycle(t *testing.T) {
	tracker := New()
	if got := tracker.Snapshot(); got.State != StatusUnavailable || got.Connected || got.Ready {
		t.Fatalf("unexpected initial state: %+v", got)
	}
	if err := tracker.Register(11); err != nil { t.Fatal(err) }
	if err := tracker.Ready(10); !errors.Is(err, ErrInvalidTransition) { t.Fatalf("expected stale session rejection, got %v", err) }
	if err := tracker.Ready(11); err != nil { t.Fatal(err) }
	if got := tracker.Snapshot(); got.State != StatusReady || !got.Ready { t.Fatalf("not ready: %+v", got) }
	tracker.Disconnect(10, "stale")
	if got := tracker.Snapshot(); !got.Connected { t.Fatalf("stale disconnect changed current session: %+v", got) }
	tracker.Disconnect(11, "connection_lost")
	if got := tracker.Snapshot(); got.State != StatusUnavailable || got.Connected || got.Ready { t.Fatalf("not unavailable: %+v", got) }
}

func TestHeartbeatRequiresCurrentSession(t *testing.T) {
	tracker := New()
	if err := tracker.Register(3); err != nil { t.Fatal(err) }
	if err := tracker.Heartbeat(2); !errors.Is(err, ErrInvalidTransition) { t.Fatalf("expected stale session rejection, got %v", err) }
	if err := tracker.Heartbeat(3); err != nil { t.Fatal(err) }
}
