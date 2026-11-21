package cpu

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestReadLinuxCStates(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cpuidle sysfs only")
	}
	root := t.TempDir()
	for _, item := range []struct{ cpu, state, name, micros string }{
		{"cpu0", "state0", "POLL", "125"},
		{"cpu0", "state1", "C1", "2500"},
		{"cpu1", "state1", "C1", "5000"},
	} {
		dir := filepath.Join(root, item.cpu, "cpuidle", item.state)
		writeSysfsTestFile(t, filepath.Join(dir, "name"), item.name)
		writeSysfsTestFile(t, filepath.Join(dir, "time"), item.micros)
	}
	items, err := readLinuxCStates(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 {
		t.Fatalf("C-state items = %d, want 3: %#v", len(items), items)
	}
	if items[0].MetricID != "system.cpu.idle.cstate.poll" || items[0].ObjectID != "cpu0/state0" || items[0].Value != int64(125000) || items[0].Unit != "ns" {
		t.Fatalf("unexpected first C-state item: %#v", items[0])
	}
}

func TestReadLinuxCStatesUnsupportedWhenNoResidencySource(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cpuidle sysfs only")
	}
	items, err := readLinuxCStates(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("read C-states: %v", err)
	}
	if len(items) != 1 || items[0].MetricID != MetricCStateTarget || items[0].Status != StatusUnsupported {
		t.Fatalf("unsupported C-state source must return an explicit status item: %#v", items)
	}
}

func TestReadLinuxThrottlingPreservesPerCPUCountersAndDeduplicatesPackages(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux thermal throttle sysfs only")
	}
	root := t.TempDir()
	writeSysfsTestFile(t, filepath.Join(root, "present"), "0-3")
	for id, core := range map[int]string{0:"0", 1:"0", 2:"1", 3:"1"} {
		base := filepath.Join(root, "cpu"+itoa(id))
		topology := filepath.Join(base, "topology")
		writeSysfsTestFile(t, filepath.Join(topology, "physical_package_id"), "0")
		writeSysfsTestFile(t, filepath.Join(topology, "core_id"), core)
		throttle := filepath.Join(base, "thermal_throttle")
		writeSysfsTestFile(t, filepath.Join(throttle, "core_throttle_count"), "7")
		writeSysfsTestFile(t, filepath.Join(throttle, "core_throttle_total_time_ms"), "3")
		writeSysfsTestFile(t, filepath.Join(throttle, "core_power_limit_count"), "4")
		writeSysfsTestFile(t, filepath.Join(throttle, "package_throttle_count"), "11")
		writeSysfsTestFile(t, filepath.Join(throttle, "package_throttle_total_time_ms"), "5")
		writeSysfsTestFile(t, filepath.Join(throttle, "package_power_limit_count"), "6")
	}
	items, err := readLinuxThrottling(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 15 {
		t.Fatalf("throttling items = %d, want 15 (per-CPU core counters and one package copy): %#v", len(items), items)
	}
	if !hasThrottleObject(items, MetricThrottleThermalCoreEvents, "cpu0") ||
		!hasThrottleObject(items, MetricThrottleThermalCoreEvents, "cpu1") ||
		!hasThrottleObject(items, MetricThrottleThermalPackageEvents, "package0") ||
		!hasThrottleObject(items, MetricThrottlePowerCoreEvents, "cpu0") ||
		!hasThrottleObject(items, MetricThrottlePowerPackageEvents, "package0") {
		t.Fatalf("missing per-CPU or deduplicated package counters: %#v", items)
	}
}

func TestReadLinuxSystemCounters(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc/stat format is Linux-specific")
	}
	path := filepath.Join(t.TempDir(), "stat")
	content := "cpu 1 2 3 4\n intr 12345 1 2 3\n ctxt 6789\n btime 1\n processes 54321\n procs_running 4\n procs_blocked 2\n"
	if err := writeFileForTest(path, content); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxSystemCounters(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 6 {
		t.Fatalf("system counters = %d, want 6 including runqueue: %#v", len(items), items)
	}
	want := []struct {
		metricID string
		value    uint64
	}{
		{MetricInterrupts, 12345},
		{MetricContextSwitches, 6789},
		{MetricForks, 54321},
		{MetricProcessesRunning, 4},
		{MetricProcessesBlocked, 2},
		{MetricRunQueue, 4},
	}
	for i, expected := range want {
		if items[i].MetricID != expected.metricID || items[i].Value != expected.value || items[i].Unit != "count" {
			t.Errorf("counter[%d] = %#v, want metric=%q value=%d unit=count", i, items[i], expected.metricID, expected.value)
		}
	}
}

func writeFileForTest(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return err }
	return os.WriteFile(path, []byte(value), 0o644)
}
