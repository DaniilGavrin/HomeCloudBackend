package events

import (
	"context"
	"encoding/json"
	"testing"

	"HomeCloudBackend/internal/commands"
	eventbus "HomeCloudBackend/internal/events"
)

func TestPublishCommandValidationAndSender(t *testing.T) {
	bus := eventbus.New()
	reg := commands.NewRegistry()
	Register(reg, Dependencies{Publish: bus.Publish})
	var received commands.Outbound
	listener := bus.Attach("receiver", func(ctx context.Context, out commands.Outbound) error { received = out; return nil })
	_, _ = bus.Subscribe(listener, "media.progress")
	ctx := context.Background()
	send := func(id, body string) commands.Outbound {
		return reg.Dispatch(ctx, commands.Inbound{V: 1, ID: id, Type: "command", Action: "events.publish", Data: json.RawMessage(body)}, commands.ClientContext{DeviceID: "desktop"})
	}
	result := send("good", `{"topic":"media.progress","payload":{"seconds":42}}`)
	if result.Type != "result" || received.Topic != "media.progress" {
		t.Fatalf("publish %+v, event %+v", result, received)
	}
	if received.Data.(map[string]any)["from"] != "desktop" {
		t.Fatalf("spoofed source %+v", received.Data)
	}
	for _, body := range []string{
		`{"topic":"clients.connected","payload":{}}`,
		`{"topic":"system.health","payload":{}}`,
		`{"topic":"backend.foo","payload":{}}`,
		`{"topic":"media.progress"}`,
		`{"topic":"media.progress","payload":0,"other":42}`,
		`null`,
	} {
		if got := send("bad", body); got.Type != "error" {
			t.Errorf("accepted %s: %+v", body, got)
		}
	}
}

func TestSubscriptionsCommand(t *testing.T) {
	reg := commands.NewRegistry()
	Register(reg, Dependencies{})
	result := reg.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "1", Type: "command", Action: "events.subscriptions"}, commands.ClientContext{Subscriptions: map[string]struct{}{"device.changed": {}, "system.health": {}}})
	if result.Type != "result" {
		t.Fatalf("subscriptions %+v", result)
	}
	topics := result.Data.(map[string]any)["topics"].([]string)
	if len(topics) != 2 || topics[0] != "device.changed" || topics[1] != "system.health" {
		t.Fatalf("sorted topics %v", topics)
	}
}
