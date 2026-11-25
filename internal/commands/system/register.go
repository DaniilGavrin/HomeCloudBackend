package system

import (
	"context"
	"errors"
	"time"

	"HomeCloudBackend/internal/commands"
)

// Dependencies contains only capabilities the system domain needs from core.
// Domain code does not import wsserver, avoiding dependency cycles.
type Dependencies struct {
	Online              func() int
	NodeID              func() string
	RequireSubscription bool
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("system.health", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if d.RequireSubscription && !peer.Subscribed("system.health") {
			return nil, errors.New("subscribe to system.health first")
		}
		return map[string]any{
			"status":      "ok",
			"connections": d.Online(),
			"server_time": time.Now().UTC().Format(time.RFC3339),
		}, nil
	})
	r.MustRegister("system.status", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		status := Status(ctx, d.Online)
		if d.NodeID != nil {
			if nodeID := d.NodeID(); nodeID != "" {
				status["node_id"] = nodeID
			}
		}
		return status, nil
	})
	RegisterProcessList(r, d.RequireSubscription)
}
