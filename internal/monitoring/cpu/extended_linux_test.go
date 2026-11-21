package cpu

import (
	"context"
	"errors"
	"path/filepath"
	"os"
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
	_, err := readLinuxCStates(context.Background(), t.TempDir())
	if !errors.Is(err, ErrMetricUnsupported) {
		t.Fatalf("error = %v, want ErrMetricUnsupported", err)
	}
}

func TestReadLinuxThrottlingDeduplicatesSMTSiblingsAndPackages(t *testing.T) {
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
		writeSysfsTestFile(t, filepath.Join(throttle, "package_throttle_count"), "11")
		writeSysfsTestFile(t, filepath.Join(throttle, "package_throttle_total_time_ms"), "5")
	}
	items, err := readLinuxThrottling(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 6 {
		t.Fatalf("throttling items = %d, want 6 (2 cores + 1 package, count and duration): %#v", len(items), items)
	}
	if !hasThrottleObject(items, MetricThrottleThermalCoreEvents, "package0/core0") ||
		!hasThrottleObject(items, MetricThrottleThermalCoreEvents, "package0/core1") ||
		!hasThrottleObject(items, MetricThrottleThermalPackageEvents, "package0") {
		t.Fatalf("missing deduplicated core/package objects: %#v", items)
	}
}

func TestReadLinuxSystemCounters(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("/proc/stat format is Linux-specific")
	}
	path := filepath.Join(t.TempDir(), "stat")
	content := "cpu 1 2 3 4\n intr 12345 1 2 3\n ctxt 6789\n btime 1\n"
	if err := writeFileForTest(path, content); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxSystemCounters(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("system counters = %d, want 2: %#v", len(items), items)
	}
	if items[0].MetricID != MetricInterrupts || items[0].Value != uint64(12345) || items[0].Unit != "count" {
		t.Fatalf("interrupt counter = %#v", items[0])
	}
	if items[1].MetricID != MetricContextSwitches || items[1].Value != uint64(6789) || items[1].Unit != "count" {
		t.Fatalf("context switch counter = %#v", items[1])
	}
}

func writeFileForTest(path, value string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil { return err }
	return os.WriteFile(path, []byte(value), 0o644)
}
