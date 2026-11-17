package telemetry

import (
	"context"
	"testing"
	"time"
)

func TestRegisterHostCollectorsProducesCPUAndMemorySamples(t *testing.T) {
	history := &testHistory{}
	core, err := NewCore("node-test", history)
	if err != nil { t.Fatal(err) }
	if err := RegisterHostCollectors(core, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := core.Start(ctx); err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() {
		cancel()
		if err := core.Close(); err != nil { t.Errorf("close telemetry core: %v", err) }
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		cpuSamples := core.Latest("node-test", "cpu.usage_percent")
		memorySamples := core.Latest("node-test", "memory.used_bytes")
		backendSamples := core.Latest("node-test", "backend.goroutines")
		processSamples := core.Latest("node-test", "processes.count")
		if len(cpuSamples) > 0 && len(memorySamples) > 0 && len(backendSamples) > 0 && len(processSamples) > 0 {
			if err := cpuSamples[0].Validate(); err != nil { t.Fatalf("invalid CPU sample: %v", err) }
			if err := memorySamples[0].Validate(); err != nil { t.Fatalf("invalid memory sample: %v", err) }
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("host collectors did not publish CPU and memory samples within 5 seconds")
}

func TestHostCollectorIntervalsAreBounded(t *testing.T) {
	if got := NewCPUCollector(0).Interval(); got != time.Second {
		t.Fatalf("CPU interval = %s, want 1s minimum", got)
	}
	if got := NewMemoryCollector(0).Interval(); got != time.Second {
		t.Fatalf("memory interval = %s, want 1s minimum", got)
	}
	if got := NewNetworkCollector(0).Interval(); got != time.Second {
		t.Fatalf("network interval = %s, want 1s minimum", got)
	}
	if got := NewDiskCollector(0).Interval(); got != 5*time.Second {
		t.Fatalf("disk interval = %s, want 5s minimum", got)
	}
}
