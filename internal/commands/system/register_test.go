package system

import (
	"context"
	"testing"

	"HomeCloudBackend/internal/commands"
)

func TestSystemHealthRequiresSubscription(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Online: func() int { return 3 }, RequireSubscription: true})
	msg := commands.Inbound{V: 1, ID: "health-1", Type: "command", Action: "system.health"}
	client := commands.ClientContext{DeviceID: "kitchen-tv"}
	out := r.Dispatch(context.Background(), msg, client)
	if out.Type != "error" || out.Error != "subscribe to system.health first" {
		t.Fatalf("expected subscription error, got %+v", out)
	}
	client.Subscriptions = map[string]struct{}{"system.health": {}}
	out = r.Dispatch(context.Background(), msg, client)
	if out.Type != "result" || out.Action != "system.health" {
		t.Fatalf("expected health result, got %+v", out)
	}
	data, ok := out.Data.(map[string]any)
	if !ok || data["status"] != "ok" || data["connections"] != 3 || data["server_time"] == "" {
		t.Fatalf("unexpected health: %#v", out.Data)
	}
}

func TestSystemHealthWithoutRequiredSubscription(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Online: func() int { return 1 }, RequireSubscription: false})
	out := r.Dispatch(context.Background(), commands.Inbound{V: 1, Type: "command", Action: "system.health"}, commands.ClientContext{})
	if out.Type != "result" {
		t.Fatalf("unexpected response %+v", out)
	}
}
