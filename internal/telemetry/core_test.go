package telemetry

import (
	"context"
	"sync"
	"testing"
	"time"
)

type testCollector struct {
	samples []Sample
}

func (c testCollector) Name() string { return "test" }
func (c testCollector) Interval() time.Duration { return time.Second }
func (c testCollector) Collect(context.Context) ([]Sample, error) {
	return c.samples, nil
}

type testHistory struct {
	mu sync.Mutex
	samples []Sample
}

func (h *testHistory) Append(_ context.Context, samples []Sample) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.samples = append(h.samples, samples...)
	return nil
}
func (h *testHistory) Count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.samples)
}
func (h *testHistory) Query(context.Context, Query) ([]Point, error) { return nil, nil }
func (h *testHistory) Close() error { return nil }

func TestCoreCollectsAndCachesSamples(t *testing.T) {
	history := &testHistory{}
	core, err := NewCore("node-a", history)
	if err != nil {
		t.Fatal(err)
	}
	sample := Sample{Name: "cpu.usage", Value: 12.5, Timestamp: time.Now().UTC()}
	if err := core.Register(testCollector{samples: []Sample{sample}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := core.Start(ctx); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for (len(core.Latest("node-a", "cpu.usage")) == 0 || history.Count() == 0) && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	latest := core.Latest("node-a", "cpu.usage")
	if len(latest) != 1 {
		t.Fatalf("expected one latest sample, got %d", len(latest))
	}
	if latest[0].NodeID != "node-a" || latest[0].Value != 12.5 {
		t.Fatalf("unexpected sample: %#v", latest[0])
	}
	if history.Count() != 1 {
		t.Fatalf("expected one persisted sample, got %d", history.Count())
	}
	if err := core.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestSampleValidationRejectsHighCardinalityLabels(t *testing.T) {
	sample := Sample{NodeID: "node-a", Name: "network.rx", Timestamp: time.Now(), Labels: map[string]string{}}
	for i := 0; i < 17; i++ {
		sample.Labels[string(rune('a'+i))] = "x"
	}
	if err := sample.Validate(); err == nil {
		t.Fatal("expected label limit validation error")
	}
}

func TestCoreObserverReceivesCollectedSamples(t *testing.T) {
	history := &testHistory{}
	core, err := NewCore("node-a", history)
	if err != nil { t.Fatal(err) }
	observed := make(chan []Sample, 1)
	if err := core.SetObserver(func(samples []Sample) { observed <- samples }); err != nil {
		t.Fatal(err)
	}
	if err := core.Register(testCollector{samples: []Sample{{
		Name:"cpu.usage_percent", Value:42, Timestamp:time.Now().UTC(),
	}}}); err != nil { t.Fatal(err) }
	ctx, cancel := context.WithCancel(context.Background())
	if err := core.Start(ctx); err != nil { cancel(); t.Fatal(err) }
	select {
	case samples := <-observed:
		if len(samples) != 1 || samples[0].NodeID != "node-a" || samples[0].Value != 42 {
			t.Fatalf("unexpected observed samples: %#v", samples)
		}
	case <-time.After(time.Second):
		t.Fatal("observer did not receive the collected sample")
	}
	cancel()
	if err := core.Close(); err != nil { t.Fatal(err) }
}

func TestCoreExpiresStaleLatestSeries(t *testing.T) {
	core, err := NewCore("node-a", &testHistory{})
	if err != nil { t.Fatal(err) }
	core.latest["stale"] = Sample{
		NodeID:"node-a", Name:"network.receive_bytes_per_second",
		Timestamp:time.Now().Add(-11*time.Minute),
	}
	if got := core.Latest("node-a", "network.receive_bytes_per_second"); len(got) != 0 {
		t.Fatalf("stale metric was returned by Latest: %#v", got)
	}
	core.collect(context.Background(), testCollector{samples:[]Sample{{
		Name:"cpu.usage_percent", Value:1, Timestamp:time.Now().UTC(),
	}}}, time.Second)
	if _, exists := core.latest["stale"]; exists {
		t.Fatal("stale metric remained in latest cache")
	}
	if err := core.Close(); err != nil { t.Fatal(err) }
}
