// Package devices exposes persistent device metadata to HomeCloud clients.
package devices

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"HomeCloudBackend/internal/commands"
	devicestore "HomeCloudBackend/internal/devices"
)

type Dependencies struct {
	List    func(context.Context) ([]devicestore.Record, error)
	Rename  func(context.Context, string, string) (devicestore.Record, error)
	Publish func(context.Context, string, any) error
}

type renameRequest struct {
	DeviceID string `json:"device_id"`
	Name     string `json:"name"`
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("devices.list", func(ctx context.Context, msg commands.Inbound, client commands.ClientContext) (any, error) {
		if d.List == nil { return nil, errors.New("device registry unavailable") }
		return d.List(ctx)
	})
	r.MustRegister("devices.rename", func(ctx context.Context, msg commands.Inbound, client commands.ClientContext) (any, error) {
		if d.Rename == nil { return nil, errors.New("device registry unavailable") }
		var req renameRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil { return nil, errors.New("invalid devices.rename data") }
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(msg.Data, &fields); err != nil || fields == nil { return nil, errors.New("devices.rename data must be an object") }
		for key := range fields {
			if key != "device_id" && key != "name" { return nil, fmt.Errorf("unknown devices.rename field %q", key) }
		}
		if req.DeviceID == "" { return nil, errors.New("device_id is required") }
		item, err := d.Rename(ctx, req.DeviceID, req.Name)
		if err != nil { return nil, err }
		if d.Publish != nil {
			if err := d.Publish(ctx, "devices.updated", item); err != nil {
				// The durable rename succeeded. A later devices.list resync repairs
				// a transient event-delivery failure, so do not report it as a failed rename.
			}
		}
		return item, nil
	})
}
