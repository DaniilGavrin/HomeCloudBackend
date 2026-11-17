package wsserver

import (
	"testing"

	"HomeCloudBackend/internal/connections"
)

func TestDeviceIDValidation(t *testing.T) {
	for _, tc := range []struct {
		value string
		valid bool
	}{
		{"kitchen-tv", true}, {"pc-main-postman", true}, {"audio.zone-01", true},
		{"", false}, {"../../config", false}, {"bad ID", false}, {"\n", false},
	} {
		if got := connections.ValidDeviceID(tc.value); got != tc.valid {
			t.Errorf("device id %q got %v want %v", tc.value, got, tc.valid)
		}
	}
}

func TestDuplicateDeviceIDRejected(t *testing.T) {
	manager := connections.NewManager(10, 10)
	first := connections.NewPeer("kitchen-tv", "tv", 1, 0, nil, nil)
	second := connections.NewPeer("kitchen-tv", "tv", 1, 0, nil, nil)
	if err := manager.Register(first); err != nil {
		t.Fatal(err)
	}
	if err := manager.Register(second); err != connections.ErrDuplicateID {
		t.Fatalf("expected duplicate, got %v", err)
	}
	manager.Unregister(second)
	if got := manager.Online(); got != 1 {
		t.Fatalf("wrong cleanup: %d", got)
	}
	manager.Unregister(first)
	if got := manager.Online(); got != 0 {
		t.Fatalf("still online: %d", got)
	}
}


func TestTelemetryMetricGroup(t *testing.T) {
	cases := map[string]string{
		"cpu.usage_percent": "cpu",
		"cpu_frequency.current_hz": "cpu",
		"memory.used_percent": "memory",
		"disk.read_bytes_per_second": "disks",
		"disk_inventory.capacity_bytes": "disks",
		"smart.temperature_celsius": "disks",
		"network.receive_bytes_per_second": "network",
		"process.count": "processes",
		"services.available": "services",
		"gpu.utilization_percent": "hardware",
		"sensors.temperature_celsius": "hardware",
		"fans.rpm": "hardware",
		"pressure.memory.some": "diagnostics",
		"linux_kernel.context_switches": "diagnostics",
		"backend.goroutines": "diagnostics",
		"host.uptime_seconds": "diagnostics",
	}
	for name, want := range cases {
		if got := telemetryMetricGroup(name); got != want {
			t.Errorf("telemetryMetricGroup(%q) = %q, want %q", name, got, want)
		}
	}
	if got := telemetryMetricGroup("unknown.metric"); got != "" {
		t.Errorf("unknown metric group = %q, want empty", got)
	}
}

func TestTelemetryOverviewMetric(t *testing.T) {
	for _, name := range []string{"cpu.usage_percent", "memory.used_percent", "host.uptime_seconds", "services.available", "disk.root.used_percent"} {
		if !telemetryOverviewMetric(name) {
			t.Errorf("expected %q in overview", name)
		}
	}
	for _, name := range []string{"cpu.core.0.usage_percent", "network.receive_bytes_per_second", "process.count"} {
		if telemetryOverviewMetric(name) {
			t.Errorf("unexpected %q in overview", name)
		}
	}
}
