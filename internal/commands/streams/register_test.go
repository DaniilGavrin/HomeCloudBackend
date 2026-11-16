package streams

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/streaming"
)

func TestStreamsCommandRegistration(t *testing.T) {
	online := func(id string) bool { return id == "pc" || id == "tv" }
	m := streaming.New(online, nil)
	defer m.CloseAll()
	r := commands.NewRegistry()
	var offered string
	Register(r, Dependencies{
		Create: m.Create, Cancel: m.Cancel, List: m.List, Path: "/ws/stream",
		QueueToDevice: func(_ context.Context, to string, msg commands.Outbound) error {
			if to != "tv" || msg.Topic != "streams.offer" {
				t.Fatalf("bad offer %+v", msg)
			}
			offered = msg.Data.(streaming.Info).StreamID
			return nil
		},
	})
	client := commands.ClientContext{DeviceID: "pc"}
	call := func(id, action, data string) commands.Outbound {
		return r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: id, Type: "command", Action: action, Data: json.RawMessage(data)}, client)
	}
	if out := call("bad", "streams.create", `{"target":"tv","evil":true}`); out.Type != "error" {
		t.Fatalf("unknown field %+v", out)
	}
	out := call("create", "streams.create", `{"target":"tv","name":"x.pdf","mime_type":"application/pdf","size_bytes":3}`)
	if out.Type != "result" {
		t.Fatalf("create %+v", out)
	}
	info := out.Data.(streaming.Info)
	if info.StreamID != offered || info.SizeBytes != 3 {
		t.Fatalf("offer mismatch %+v", info)
	}
	if got := call("list", "streams.list", `{}`); got.Type != "result" {
		t.Fatalf("list %+v", got)
	}
	if out := call("cancel", "streams.cancel", `{"stream_id":"`+info.StreamID+`"}`); out.Type != "result" {
		t.Fatalf("cancel %+v", out)
	}
	if out := call("again", "streams.cancel", `{"stream_id":"`+info.StreamID+`"}`); out.Type != "error" {
		t.Fatalf("double cancel %+v", out)
	}
}

func TestCreateRollsBackWhenOfferQueueFails(t *testing.T) {
	m := streaming.New(func(string) bool { return true }, nil)
	defer m.CloseAll()
	r := commands.NewRegistry()
	Register(r, Dependencies{Create: m.Create, Cancel: m.Cancel, List: m.List, Path: "/ws/stream", QueueToDevice: func(context.Context, string, commands.Outbound) error { return errors.New("queue full") }})
	out := r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "x", Type: "command", Action: "streams.create", Data: json.RawMessage(`{"target":"tv"}`)}, commands.ClientContext{DeviceID: "pc"})
	if out.Type != "error" || len(m.List("pc")) != 0 {
		t.Fatalf("rollback %+v", out)
	}
}
