package telemetry

import (
	"context"
	"testing"
)

func TestServiceCollectorEmitsAvailabilityGauges(t *testing.T) {
	collector := NewServiceCollector(func() map[string]bool {
		return map[string]bool{"backend":true, "media_gateway":false}
	})
	samples, err := collector.Collect(context.Background())
	if err != nil { t.Fatal(err) }
	if len(samples) != 2 { t.Fatalf("expected 2 service samples, got %d", len(samples)) }
	values := make(map[string]float64)
	for _, sample := range samples {
		sample.NodeID = "node-a"
		if err := sample.Validate(); err != nil {
			t.Fatalf("invalid service metric: %v", err)
		}
		values[sample.Labels["service"]] = sample.Value
	}
	if values["backend"] != 1 || values["media_gateway"] != 0 {
		t.Fatalf("unexpected service availability values: %#v", values)
	}
}
