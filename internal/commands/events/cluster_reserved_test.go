package events

import (
	"context"
	"strings"
	"testing"

	"HomeCloudBackend/internal/commands"
	eventbus "HomeCloudBackend/internal/events"
)

func TestClusterEventsCannotBeSpoofedByWSClient(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Publish: func(context.Context, string, string, any) (eventbus.PublishResult, error) {
		return eventbus.PublishResult{}, nil
	}})
	m := commands.Inbound{V: 1, ID: "evt", Type: "command", Action: "events.publish", Data: []byte(`{"topic":"cluster.registry.changed","payload":{"revision":9999}}`)}
	resp := r.Dispatch(context.Background(), m, commands.ClientContext{DeviceID: "test"})
	if resp.Type != "error" || !strings.Contains(resp.Error, "reserved") {
		t.Fatalf("client spoofed cluster event: %+v", resp)
	}
}
