package cpu

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestParseCPUList(t *testing.T) {
	got, err := parseCPUList("0-2,4,6-7")
	if err != nil {
		t.Fatal(err)
	}
	want := []int{0, 1, 2, 4, 6, 7}
	if len(got) != len(want) {
		t.Fatalf("parsed CPU list = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("parsed CPU list = %v, want %v", got, want)
		}
	}
	for _, invalid := range []string{"", "2-1", "a", "0-99999", "1,,2"} {
		if _, err := parseCPUList(invalid); err == nil {
			t.Errorf("parseCPUList(%q) succeeded, want error", invalid)
		}
	}
}

func TestReadLinuxFrequencyPolicies(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cpufreq sysfs only")
	}
	root := t.TempDir()
	policy := filepath.Join(root, "cpufreq", "policy0")
	writeSysfsTestFile(t, filepath.Join(policy, "scaling_cur_freq"), "1800000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_min_freq"), "800000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_max_freq"), "3200000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "base_frequency"), "2400000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "scaling_governor"), "performance\n")

	items, err := readLinuxFrequency(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 5 {
		t.Fatalf("frequency items = %d, want 5: %#v", len(items), items)
	}
	expected := map[string]any{
		MetricFrequencyCurrent: int64(1800000),
		MetricFrequencyMin: int64(800000),
		MetricFrequencyMax: int64(3200000),
		MetricFrequencyBase: int64(2400000),
		MetricFrequencyGovernor: "performance",
	}
	for metricID, value := range expected {
		item, ok := findMetric(items, metricID, "policy0")
		if !ok {
			t.Errorf("missing %s for policy0", metricID)
			continue
		}
		if item.Value != value {
			t.Errorf("%s value = %#v, want %#v", metricID, item.Value, value)
		}
	}
}

func TestReadLinuxFrequencyUsesBaseFrequencyFallback(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux cpufreq sysfs only")
	}
	root := t.TempDir()
	policy := filepath.Join(root, "cpufreq", "policy2")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_base_freq"), "2100000")
	items, err := readLinuxFrequency(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := findMetric(items, MetricFrequencyBase, "policy2")
	if !ok || item.Value != int64(2100000) {
		t.Fatalf("base frequency = %#v, found=%v; want 2100000 kHz", item, ok)
	}
}

func TestReadLinuxTopologyFromSysfs(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Linux CPU topology sysfs only")
	}
	root := t.TempDir()
	writeSysfsTestFile(t, filepath.Join(root, "present"), "0-3")
	writeSysfsTestFile(t, filepath.Join(root, "online"), "0-2")

	for id, coreID := range map[int]string{0: "0", 1: "0", 2: "1", 3: "1"} {
		cpuPath := filepath.Join(root, "cpu"+itoa(id))
		topology := filepath.Join(cpuPath, "topology")
		writeSysfsTestFile(t, filepath.Join(topology, "core_id"), coreID)
		writeSysfsTestFile(t, filepath.Join(topology, "physical_package_id"), "0")
		writeSysfsTestFile(t, filepath.Join(topology, "die_id"), "0")
	}
	for _, id := range []int{0, 1} {
		if err := os.MkdirAll(filepath.Join(root, "cpu"+itoa(id), "node0"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "cpu2", "node1"), 0o755); err != nil {
		t.Fatal(err)
	}

	items, err := readLinuxTopology(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	assertTopologyValue(t, items, MetricTopologyLogicalCount, 4)
	assertTopologyValue(t, items, MetricTopologyOnlineCount, 3)
	assertTopologyValue(t, items, MetricTopologyOfflineCount, 1)
	assertTopologyValue(t, items, MetricTopologyCoreCount, 2)
	assertTopologyValue(t, items, MetricTopologyPackageCount, 1)
	assertTopologyValue(t, items, MetricTopologyDieCount, 1)
	assertTopologyValue(t, items, MetricTopologyNUMACount, 2)

	onlineCPU3, ok := findMetric(items, MetricTopologyOnline, "cpu3")
	if !ok || onlineCPU3.Value != false {
		t.Fatalf("cpu3 online metric = %#v, found=%v; want false", onlineCPU3, ok)
	}
	coreCPU2, ok := findMetric(items, MetricTopologyCoreID, "cpu2")
	if !ok || coreCPU2.Value != 1 {
		t.Fatalf("cpu2 core ID = %#v, found=%v; want 1", coreCPU2, ok)
	}
}

func assertTopologyValue(t *testing.T, items []MetricItem, metricID string, want int) {
	t.Helper()
	item, ok := findMetric(items, metricID, "")
	if !ok || item.Value != want {
		t.Fatalf("%s = %#v, found=%v; want %d", metricID, item, ok, want)
	}
}

func writeSysfsTestFile(t *testing.T, path, value string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(value), 0o644); err != nil {
		t.Fatal(err)
	}
}

func itoa(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for value > 0 {
		i--
		buf[i] = digits[value%10]
		value /= 10
	}
	return string(buf[i:])
}
