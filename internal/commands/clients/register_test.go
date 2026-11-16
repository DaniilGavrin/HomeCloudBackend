package clients

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
)

func TestClientsList(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Connected: func() []map[string]string {
		return []map[string]string{{"device_id": "tv", "client_type": "television"}}
	}})
	out := r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "r", Type: "command", Action: "clients.list"}, commands.ClientContext{DeviceID: "pc"})
	if out.Type != "result" || out.Action != "clients.list" || out.ID != "r" {
		t.Fatalf("unexpected: %+v", out)
	}
	devices, ok := out.Data.([]map[string]string)
	if !ok || len(devices) != 1 || devices[0]["device_id"] != "tv" {
		t.Fatalf("devices: %#v", out.Data)
	}
}

func TestClientsSend(t *testing.T) {
	r := commands.NewRegistry()
	var target string
	var event commands.Outbound
	Register(r, Dependencies{
		Connected: func() []map[string]string { return nil },
		QueueToDevice: func(ctx context.Context, id string, m commands.Outbound) error {
			target = id
			event = m
			return nil
		},
	})
	request := commands.Inbound{V: 1, ID: "r-5", Type: "command", Action: "clients.send", Data: json.RawMessage(`{"target":"kitchen-tv","payload":{"action":"media.pause"}}`)}
	out := r.Dispatch(context.Background(), request, commands.ClientContext{DeviceID: "desktop"})
	if out.Type != "result" || target != "kitchen-tv" || event.Topic != "clients.message" || event.Type != "event" {
		t.Fatalf("out=%+v event=%+v target=%s", out, event, target)
	}
	data, err := json.Marshal(event.Data)
	if err != nil {
		t.Fatal(err)
	}
	var fields struct {
		From      string            `json:"from"`
		RequestID string            `json:"request_id"`
		Payload   map[string]string `json:"payload"`
	}
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if fields.From != "desktop" || fields.RequestID != "r-5" || fields.Payload["action"] != "media.pause" {
		t.Fatalf("fields=%+v", fields)
	}
}

func TestClientsSendInvalidAndOffline(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Connected: func() []map[string]string { return nil },
		QueueToDevice: func(context.Context, string, commands.Outbound) error { return errors.New("device not connected") },
	})
	for _, body := range []string{`{}`, `{"target":"tv"}`, `{"target":"tv","payload":null}`, `{"target":"tv","payload":[]}`, `{"target":"tv","payload":{},"unexpected":true}`} {
		out := r.Dispatch(context.Background(), commands.Inbound{V: 1, Type: "command", Action: "clients.send", Data: json.RawMessage(body)}, commands.ClientContext{DeviceID: "pc"})
		if out.Type != "error" {
			t.Fatalf("expected error for %s, got %+v", body, out)
		}
	}
	out := r.Dispatch(context.Background(), commands.Inbound{V: 1, Type: "command", Action: "clients.send", Data: json.RawMessage(`{"target":"offline","payload":{"x":1}}`)}, commands.ClientContext{DeviceID: "pc"})
	if out.Type != "error" || out.Error != "device not connected" {
		t.Fatalf("unexpected %+v", out)
	}
}
