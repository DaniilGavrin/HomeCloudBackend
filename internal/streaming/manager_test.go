package streaming

import (
	"errors"
	"sync"
	"testing"
)

func TestCreateListCancelAndLimits(t *testing.T) {
	online := map[string]bool{"pc": true, "tv": true, "phone": true}
	m := New(func(id string) bool { return online[id] }, nil)
	defer m.CloseAll()
	if _, err := m.Create("pc", CreateRequest{Target: "missing"}, "/ws/stream"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("offline target: %v", err)
	}
	if _, err := m.Create("pc", CreateRequest{Target: "pc"}, "/ws/stream"); err == nil {
		t.Fatal("self transfer accepted")
	}
	if _, err := m.Create("pc", CreateRequest{Target: "tv", SizeBytes: MaxStreamBytes + 1}, "/ws/stream"); err == nil {
		t.Fatal("oversize accepted")
	}
	ids := map[string]bool{}
	for i := 0; i < MaxStreamsPerDevice; i++ {
		info, err := m.Create("pc", CreateRequest{Target: "tv", SizeBytes: 100}, "/ws/stream")
		if err != nil {
			t.Fatal(err)
		}
		if info.Path != "/ws/stream" || info.From != "pc" || info.Target != "tv" || len(info.StreamID) != 32 || ids[info.StreamID] {
			t.Fatalf("bad offer: %+v", info)
		}
		ids[info.StreamID] = true
	}
	if _, err := m.Create("pc", CreateRequest{Target: "phone"}, "/ws/stream"); !errors.Is(err, ErrBusy) {
		t.Fatalf("per-device limit: %v", err)
	}
	if got := m.List("tv"); len(got) != MaxStreamsPerDevice {
		t.Fatalf("list: %+v", got)
	}
	for id := range ids {
		if err := m.Cancel("phone", id); !errors.Is(err, ErrNotParticipant) {
			t.Fatalf("nonparticipant cancel: %v", err)
		}
		if err := m.Cancel("tv", id); err != nil {
			t.Fatal(err)
		}
		if err := m.Cancel("tv", id); !errors.Is(err, ErrNotFound) {
			t.Fatalf("double cancel: %v", err)
		}
	}
	if got := m.List("pc"); len(got) != 0 {
		t.Fatalf("leaked streams: %+v", got)
	}
}

func TestConcurrentCreateCancelDevice(t *testing.T) {
	m := New(func(string) bool { return true }, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			info, err := m.Create("pc", CreateRequest{Target: "tv"}, "/ws/stream")
			if err == nil {
				_ = m.Cancel("pc", info.StreamID)
			}
			if err != nil && !errors.Is(err, ErrBusy) {
				t.Errorf("create: %v", err)
			}
		}()
	}
	wg.Wait()
	m.CancelDevice("pc")
	if got := m.List("pc"); len(got) != 0 {
		t.Fatalf("leaked: %+v", got)
	}
	m.CloseAll()
	if _, err := m.Create("pc", CreateRequest{Target: "tv"}, "/ws/stream"); !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("after shutdown: %v", err)
	}
}
