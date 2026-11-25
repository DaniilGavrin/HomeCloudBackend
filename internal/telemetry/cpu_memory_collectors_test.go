package telemetry

import (
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
)

func TestCPUModeSamplesUseCounterDeltas(t *testing.T) {
	now := time.Now().UTC()
	out := make([]Sample, 0)
	appendCPUModeSamples(&out,
		cpu.TimesStat{User:10, System:5, Idle:80},
		cpu.TimesStat{User:20, System:10, Idle:170},
		now,
	)
	values := make(map[string]float64)
	for _, sample := range out {
		sample.NodeID = "node-a"
		if err := sample.Validate(); err != nil { t.Fatalf("invalid CPU mode sample: %v", err) }
		values[sample.Name] = sample.Value
	}
	if len(values) != 8 {
		t.Fatalf("expected 8 CPU mode series, got %d", len(values))
	}
	if values["cpu.mode.user_percent"] < 9.52 || values["cpu.mode.user_percent"] > 9.53 {
		t.Fatalf("unexpected user percentage: %v", values["cpu.mode.user_percent"])
	}
	if values["cpu.mode.idle_percent"] < 85.71 || values["cpu.mode.idle_percent"] > 85.72 {
		t.Fatalf("unexpected idle percentage: %v", values["cpu.mode.idle_percent"])
	}
}

func TestCPUModeSamplesIgnoreCounterReset(t *testing.T) {
	out := make([]Sample, 0)
	appendCPUModeSamples(&out,
		cpu.TimesStat{User:10, System:5, Idle:80},
		cpu.TimesStat{User:2, System:7, Idle:90},
		time.Now().UTC(),
	)
	if len(out) != 0 {
		t.Fatalf("counter reset should not produce bogus percentages: %#v", out)
	}
}
