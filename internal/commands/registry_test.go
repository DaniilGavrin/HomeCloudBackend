package commands

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestRegistryNamesAndDuplicates(t *testing.T) {
	r := NewRegistry()
	noop := func(context.Context, Inbound, ClientContext) (any, error) { return true, nil }
	for _, name := range []string{"system.status", "clients.list", "media.play"} {
		if err := r.Register(name, noop); err != nil {
			t.Fatalf("register %s: %v", name, err)
		}
	}
	if err := r.Register("system.status", noop); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("expected duplicate registration error, got %v", err)
	}
	for _, invalid := range []string{"", "plain", "System.status", "system..bad", "device/execute", "system."} {
		if err := r.Register(invalid, noop); err == nil {
			t.Errorf("accepted invalid action %q", invalid)
		}
	}
	if err := r.Register("system.noop", nil); err == nil {
		t.Fatal("accepted nil handler")
	}
	want := []string{"clients.list", "media.play", "system.status"}
	if got := r.Actions(); !reflect.DeepEqual(got, want) {
		t.Fatalf("actions=%v want %v", got, want)
	}
}

func TestDispatchLegacyResponseShape(t *testing.T) {
	r := NewRegistry()
	r.MustRegister("test.ok", func(ctx context.Context, msg Inbound, client ClientContext) (any, error) {
		if client.DeviceID != "desktop" || msg.ID != "req-1" {
			t.Fatalf("lost request metadata: %+v / %+v", msg, client)
		}
		return map[string]any{"ok": true}, nil
	})
	r.MustRegister("test.error", func(context.Context, Inbound, ClientContext) (any, error) {
		return nil, errors.New("expected domain error")
	})
	sender := ClientContext{DeviceID: "desktop", ClientType: "pc"}
	resp := r.Dispatch(context.Background(), Inbound{V: 1, ID: "req-1", Type: "command", Action: "test.ok"}, sender)
	if resp.V != 1 || resp.ID != "req-1" || resp.Type != "result" || resp.Action != "test.ok" || resp.Error != "" {
		t.Fatalf("unexpected success response: %+v", resp)
	}
	for _, tc := range []struct{ action, expected string }{
		{"test.error", "expected domain error"},
		{"does.notexist", "unknown or unsupported action"},
	} {
		resp := r.Dispatch(context.Background(), Inbound{V: 1, ID: "req-2", Type: "command", Action: tc.action}, sender)
		if resp.V != 1 || resp.ID != "req-2" || resp.Type != "error" || resp.Error != tc.expected || resp.Action != "" {
			t.Fatalf("unexpected error response for %s: %+v", tc.action, resp)
		}
	}
}

func TestRegistryConcurrentDispatch(t *testing.T) {
	r := NewRegistry()
	r.MustRegister("test.concurrent", func(_ context.Context, _ Inbound, c ClientContext) (any, error) {
		return c.DeviceID, nil
	})
	const n = 100
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			msg := Inbound{V: 1, ID: "req", Type: "command", Action: "test.concurrent"}
			out := r.Dispatch(context.Background(), msg, ClientContext{DeviceID: "tv"})
			if out.Type != "result" || out.Data != "tv" {
				t.Errorf("unexpected concurrent response: %+v", out)
			}
		}()
	}
	wg.Wait()
}
