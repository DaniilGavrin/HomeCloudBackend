// Package streams registers control-plane commands for binary WS sessions.
// File bytes are never carried in these JSON commands.
package streams

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/streaming"
)

type Dependencies struct {
	Create        func(string, streaming.CreateRequest, string) (streaming.Info, error)
	Cancel        func(string, string) error
	List          func(string) []streaming.Info
	QueueToDevice func(context.Context, string, commands.Outbound) error
	Path          string
}

func decodeStrict(raw json.RawMessage, target any) error {
	if len(raw) == 0 {
		return errors.New("data object required")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return errors.New("data must be a JSON object")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(target); err != nil {
		return fmt.Errorf("invalid stream data: %w", err)
	}
	return nil
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("streams.create", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if d.Create == nil || d.QueueToDevice == nil {
			return nil, errors.New("streaming unavailable")
		}
		var req streaming.CreateRequest
		if err := decodeStrict(msg.Data, &req); err != nil {
			return nil, err
		}
		info, err := d.Create(peer.DeviceID, req, d.Path)
		if err != nil {
			return nil, err
		}
		offer := commands.Outbound{V: 1, Type: "event", Topic: "streams.offer", Data: info}
		if err := d.QueueToDevice(ctx, req.Target, offer); err != nil {
			_ = d.Cancel(peer.DeviceID, info.StreamID)
			return nil, fmt.Errorf("target cannot receive stream offer: %w", err)
		}
		return info, nil
	})
	r.MustRegister("streams.cancel", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if d.Cancel == nil {
			return nil, errors.New("streaming unavailable")
		}
		var req struct {
			StreamID string `json:"stream_id"`
		}
		if err := decodeStrict(msg.Data, &req); err != nil {
			return nil, err
		}
		if req.StreamID == "" {
			return nil, errors.New("stream_id required")
		}
		if err := d.Cancel(peer.DeviceID, req.StreamID); err != nil {
			return nil, err
		}
		return map[string]string{"stream_id": req.StreamID, "status": "cancelled"}, nil
	})
	r.MustRegister("streams.list", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if d.List == nil {
			return nil, errors.New("streaming unavailable")
		}
		return map[string]any{"streams": d.List(peer.DeviceID)}, nil
	})
}
