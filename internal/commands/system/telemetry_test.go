package system

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/telemetry"
)

type metricsTestHistory struct{}

func (metricsTestHistory) Append(context.Context, []telemetry.Sample) error { return nil }
func (metricsTestHistory) Query(context.Context, telemetry.Query) ([]telemetry.Point, error) {
	return []telemetry.Point{{
		NodeID: "node-a", Name: "cpu.usage_percent",
		Timestamp: time.Date(2026, 11, 17, 10, 0, 0, 0, time.UTC),
		Value: 22.5,
	}}, nil
}
func (metricsTestHistory) Close() error { return nil }

func TestMetricsLatestRequiresSubscription(t *testing.T) {
	core, err := telemetry.NewCore("node-a", metricsTestHistory{})
	if err != nil {
		t.Fatal(err)
	}
	r := commands.NewRegistry()
	if err := RegisterTelemetry(r, core, true); err != nil {
		t.Fatal(err)
	}
	msg := commands.Inbound{V: 1, Type: "command", Action: "system.metrics.latest"}
	out := r.Dispatch(context.Background(), msg, commands.ClientContext{})
	if out.Type != "error" || out.Error != "subscribe to system.metrics first" {
		t.Fatalf("expected subscription error, got %+v", out)
	}
}

func TestMetricsQueryReturnsHistoricalPoints(t *testing.T) {
	core, err := telemetry.NewCore("node-a", metricsTestHistory{})
	if err != nil {
		t.Fatal(err)
	}
	r := commands.NewRegistry()
	if err := RegisterTelemetry(r, core, false); err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(map[string]any{
		"node_id": "node-a",
		"metric": "cpu.usage_percent",
		"from": "2026-11-17T09:00:00Z",
		"to": "2026-11-17T11:00:00Z",
		"resolution_ms": 10000,
	})
	out := r.Dispatch(context.Background(), commands.Inbound{
		V: 1, Type: "command", Action: "system.metrics.query", Data: data,
	}, commands.ClientContext{})
	if out.Type != "result" {
		t.Fatalf("expected metrics result, got %+v", out)
	}
	result, ok := out.Data.(map[string]any)
	if !ok {
		t.Fatalf("unexpected response data: %#v", out.Data)
	}
	points, ok := result["points"].([]telemetry.Point)
	if !ok || len(points) != 1 || points[0].Value != 22.5 {
		t.Fatalf("unexpected historical points: %#v", result["points"])
	}
}
