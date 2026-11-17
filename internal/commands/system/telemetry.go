package system

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/telemetry"
)

type metricsQueryRequest struct {
	NodeID       string `json:"node_id"`
	Metric       string `json:"metric"`
	From         string `json:"from"`
	To           string `json:"to"`
	ResolutionMS int64  `json:"resolution_ms,omitempty"`
	Limit        int    `json:"limit,omitempty"`
}

// RegisterTelemetry exposes the core's latest cache and historical time-series
// through the existing command protocol. Streaming subscriptions are added by
// the transport integration in a later phase.
func RegisterTelemetry(r *commands.Registry, core *telemetry.Core, requireSubscription bool) error {
	if core == nil {
		return errors.New("telemetry core is unavailable")
	}
	if err := r.Register("system.metrics.latest", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if requireSubscription && !peer.Subscribed("system.metrics") {
			return nil, errors.New("subscribe to system.metrics first")
		}
		var request struct {
			NodeID string `json:"node_id"`
			Metric string `json:"metric"`
		}
		if len(msg.Data) > 0 {
			if err := json.Unmarshal(msg.Data, &request); err != nil {
				return nil, errors.New("invalid metrics.latest data")
			}
		}
		return map[string]any{
			"samples": core.Latest(request.NodeID, request.Metric),
			"stats": core.Stats(),
			"server_time": time.Now().UTC().Format(time.RFC3339Nano),
		}, nil
	}); err != nil {
		return err
	}
	return r.Register("system.metrics.query", func(ctx context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if requireSubscription && !peer.Subscribed("system.metrics") {
			return nil, errors.New("subscribe to system.metrics first")
		}
		var request metricsQueryRequest
		if err := json.Unmarshal(msg.Data, &request); err != nil {
			return nil, errors.New("invalid metrics.query data")
		}
		from, err := time.Parse(time.RFC3339Nano, request.From)
		if err != nil {
			return nil, errors.New("from must be RFC3339 timestamp")
		}
		to, err := time.Parse(time.RFC3339Nano, request.To)
		if err != nil {
			return nil, errors.New("to must be RFC3339 timestamp")
		}
		if to.Sub(from) > 14*24*time.Hour || to.Sub(from) <= 0 {
			return nil, errors.New("metrics query range must be between 0 and 14 days")
		}
		points, err := core.Query(ctx, telemetry.Query{
			NodeID: request.NodeID, Metric: request.Metric, From: from, To: to,
			Resolution: time.Duration(request.ResolutionMS) * time.Millisecond,
			Limit: request.Limit,
		})
		if err != nil {
			return nil, err
		}
		return map[string]any{
			"node_id": request.NodeID, "metric": request.Metric,
			"from": from.UTC().Format(time.RFC3339Nano),
			"to": to.UTC().Format(time.RFC3339Nano),
			"points": points,
		}, nil
	})
}
