package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestProcessCollectorEmitsBoundedResourceSeries(t *testing.T) {
	collector := NewProcessCollector(5 * time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	samples, err := collector.Collect(ctx)
	if err != nil { t.Fatal(err) }
	if len(samples) == 0 { t.Fatal("expected at least process count metric") }
	foundCount := false
	processSeries := 0
	for _, sample := range samples {
		sample.NodeID = "node-test" // Core attaches the owning node identity during ingestion.
		if err := sample.Validate(); err != nil { t.Fatalf("invalid sample %#v: %v", sample, err) }
		if sample.Name == "processes.count" { foundCount = true }
		if sample.Name == "process.cpu.usage_percent" { processSeries++ }
	}
	if !foundCount { t.Fatal("process count metric is missing") }
	if processSeries > processMetricLimit*2 { t.Fatalf("process series exceeded bounded top-N: %d", processSeries) }
}
