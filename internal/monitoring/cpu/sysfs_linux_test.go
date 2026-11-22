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
	whitespace, err := parseCPUList("0 1 3-4")
	if err != nil || len(whitespace) != 4 || whitespace[0] != 0 || whitespace[3] != 4 {
		t.Fatalf("whitespace-separated CPU list = %v, err=%v", whitespace, err)
	}
	for _, invalid := range []string{"", "2-1", "a", "0-99999", "1,,2", "0,0"} {
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
	writeSysfsTestFile(t, filepath.Join(policy, "affected_cpus"), "0 1")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_min_freq"), "800000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_max_freq"), "3200000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "base_frequency"), "2400000\n")
	writeSysfsTestFile(t, filepath.Join(policy, "scaling_governor"), "performance\n")

	items, err := readLinuxFrequency(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 8 {
		t.Fatalf("frequency items = %d, want 8 including canonical/current aliases and two per-core values: %#v", len(items), items)
	}
	expected := map[string]any{
		MetricFrequencyTarget: int64(1800000000),
		MetricFrequencyCurrent: int64(1800000000),
		MetricFrequencyMin: int64(800000000),
		MetricFrequencyMax: int64(3200000000),
		MetricFrequencyBase: int64(2400000000),
		MetricFrequencyGovernor: "performance",
	}
	for metricID, value := range expected {
		item, ok := findMetric(items, metricID, "policy0")
		if !ok {
			t.Errorf("missing %s for policy0", metricID)
			continue
		}
		wantUnit := "Hz"
		if metricID == MetricFrequencyGovernor {
			wantUnit = "string"
		}
		if item.Value != value || item.Unit != wantUnit {
			t.Errorf("%s value/unit = %#v/%q, want %#v/%s", metricID, item.Value, item.Unit, value, wantUnit)
		}
	}
	for _, cpuID := range []string{"cpu0", "cpu1"} {
		coreFrequency, ok := findMetric(items, MetricCoreFrequency, cpuID)
		if !ok || coreFrequency.Value != nil || coreFrequency.Unit != "" || coreFrequency.Status != StatusUnavailable || coreFrequency.Reason != "shared_policy_not_per_core" {
			t.Fatalf("shared policy must not be reported as independent core frequency for %s: %#v, found=%v", cpuID, coreFrequency, ok)
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
	if !ok || item.Value != int64(2100000000) {
		t.Fatalf("base frequency = %#v, found=%v; want 2100000000 Hz", item, ok)
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

func TestFrequencyPolicyCPUResolution(t *testing.T) {
	root := t.TempDir()
	policy0 := filepath.Join(root, "cpufreq", "policy0")
	policy1 := filepath.Join(root, "cpufreq", "policy1")
	policy2 := filepath.Join(root, "cpufreq", "policy2")
	writeSysfsTestFile(t, filepath.Join(policy0, "affected_cpus"), "0 1")
	writeSysfsTestFile(t, filepath.Join(policy1, "related_cpus"), "2-3")
	writeSysfsTestFile(t, filepath.Join(policy2, "affected_cpus"), "")
	writeSysfsTestFile(t, filepath.Join(policy2, "related_cpus"), "4")

	if got := frequencyPoliciesForCPU(root, 1); len(got) != 1 || got[0] != "policy0" {
		t.Fatalf("policies for cpu1 = %v, want [policy0]", got)
	}
	if got := cpusForFrequencyPolicy(root, "policy1"); len(got) != 2 || got[0] != 2 || got[1] != 3 {
		t.Fatalf("CPUs for policy1 = %v, want [2 3]", got)
	}
	if got := frequencyPoliciesForCPU(root, 4); len(got) != 1 || got[0] != "policy2" {
		t.Fatalf("policies for cpu4 = %v, want [policy2] via related_cpus fallback", got)
	}
	if got := cpusForFrequencyPolicy(root, "policy2"); len(got) != 1 || got[0] != 4 {
		t.Fatalf("CPUs for policy2 = %v, want [4] via related_cpus fallback", got)
	}
	if isFrequencyDomainID("cpu1") || !isFrequencyDomainID("policy1") {
		t.Fatal("frequency-domain validation accepted invalid policy ID or rejected policy1")
	}
}

func TestReadLinuxFrequencyReportsExactCurrentFrequencySource(t *testing.T) {
	root := t.TempDir()
	policy := filepath.Join(root, "cpufreq", "policy0")
	writeSysfsTestFile(t, filepath.Join(policy, "affected_cpus"), "0-1")
	writeSysfsTestFile(t, filepath.Join(policy, "scaling_cur_freq"), "2500000")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_min_freq"), "800000")
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_max_freq"), "4000000")
	writeSysfsTestFile(t, filepath.Join(policy, "base_frequency"), "3000000")
	writeSysfsTestFile(t, filepath.Join(policy, "scaling_governor"), "schedutil")

	items, err := readLinuxFrequency(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	current, ok := findMetric(items, MetricFrequencyCurrent, "policy0")
	if !ok || current.Value != int64(2_500_000_000) || current.SourceID != "linux.host.sysfs.cpufreq.scaling_cur_freq" {
		t.Fatalf("current policy frequency = %#v (found=%v)", current, ok)
	}
	core, ok := findMetric(items, MetricCoreFrequency, "cpu0")
	if !ok || core.Value != nil || core.Status != StatusUnavailable || core.Reason != "shared_policy_not_per_core" {
		t.Fatalf("shared policy must not be reported as a per-core frequency: %#v (found=%v)", core, ok)
	}

	if err := os.Remove(filepath.Join(policy, "scaling_cur_freq")); err != nil {
		t.Fatal(err)
	}
	writeSysfsTestFile(t, filepath.Join(policy, "cpuinfo_cur_freq"), "2600000")
	items, err = readLinuxFrequency(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	current, ok = findMetric(items, MetricFrequencyCurrent, "policy0")
	if !ok || current.Value != int64(2_600_000_000) || current.SourceID != "linux.host.sysfs.cpufreq.cpuinfo_cur_freq" {
		t.Fatalf("fallback current policy frequency = %#v (found=%v)", current, ok)
	}
	core, ok = findMetric(items, MetricCoreFrequency, "cpu0")
	if !ok || core.Value != nil || core.Status != StatusUnavailable || core.Reason != "shared_policy_not_per_core" {
		t.Fatalf("fallback shared policy must not be reported as a per-core frequency: %#v (found=%v)", core, ok)
	}
}
