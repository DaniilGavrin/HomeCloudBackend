// Package clients exposes messaging between active devices via the registry.
package clients

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"HomeCloudBackend/internal/commands"
)

type Dependencies struct {
	Connected     func() []map[string]string
	QueueToDevice func(context.Context, string, commands.Outbound) error
}

type sendRequest struct {
	Target  string          `json:"target"`
	Payload json.RawMessage `json:"payload"`
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("clients.list", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		return d.Connected(), nil
	})
	r.MustRegister("clients.send", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if d.QueueToDevice == nil {
			return nil, errors.New("client messaging unavailable")
		}
		var req sendRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return nil, errors.New("invalid clients.send data")
		}
		// Explicitly require a JSON object, not a null or arbitrary scalar.
		payload := bytes.TrimSpace(req.Payload)
		if req.Target == "" || len(payload) == 0 || payload[0] != '{' || !json.Valid(payload) {
			return nil, errors.New("target and JSON object payload are required")
		}
		// Reject misspelled/extra fields instead of silently ignoring them.
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(msg.Data, &fields); err != nil {
			return nil, errors.New("invalid clients.send data")
		}
		for k := range fields {
			if k != "target" && k != "payload" {
				return nil, fmt.Errorf("unknown clients.send field %q", k)
			}
		}
		event := commands.Outbound{V: 1, Type: "event", Topic: "clients.message", Data: map[string]any{
			"from": peer.DeviceID, "request_id": msg.ID, "payload": json.RawMessage(payload),
		}}
		if err := d.QueueToDevice(ctx, req.Target, event); err != nil {
			return nil, err
		}
		return map[string]string{"target": req.Target, "status": "queued"}, nil
	})
}
