//go:build linux

package cpu

import (
	"context"
	"path/filepath"
	"testing"
)

func TestReadLinuxCPUTemperaturesFromHwmon(t *testing.T) {
	root := t.TempDir()
	hwmon := filepath.Join(root, "hwmon0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "name"), "coretemp")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_label"), "Package id 0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_input"), "45000")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp2_label"), "Core 0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp2_input"), "42000")

	items, err := readLinuxCPUTemperatures(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("CPU temperature items = %d, want 2: %#v", len(items), items)
	}
	packageItem, ok := findMetric(items, MetricTemperature, "package0")
	if !ok || packageItem.Value != 45.0 || packageItem.Unit != "°C" || packageItem.Status != StatusValid {
		t.Fatalf("package temperature = %#v, found=%v", packageItem, ok)
	}
	coreItem, ok := findMetric(items, MetricTemperature, "cpu0")
	if !ok || coreItem.Value != 42.0 || coreItem.Unit != "°C" || coreItem.Status != StatusValid {
		t.Fatalf("core temperature = %#v, found=%v", coreItem, ok)
	}
}

func TestReadLinuxCPUTemperaturesDistinguishesTctlAndTdie(t *testing.T) {
	root := t.TempDir()
	hwmon := filepath.Join(root, "hwmon0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "name"), "k10temp")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_label"), "Tctl")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_input"), "51000")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp2_label"), "Tdie")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp2_input"), "49000")

	items, err := readLinuxCPUTemperatures(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ObjectID == items[1].ObjectID {
		t.Fatalf("Tctl and Tdie must have distinct object IDs: %#v", items)
	}
}

func TestReadLinuxCPUTemperaturesRejectsOutOfRangeValue(t *testing.T) {
	root := t.TempDir()
	hwmon := filepath.Join(root, "hwmon0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "name"), "coretemp")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_label"), "Package id 0")
	writeSysfsTestFile(t, filepath.Join(hwmon, "temp1_input"), "250000")

	items, err := readLinuxCPUTemperatures(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Status != StatusInvalid || items[0].Reason != "out_of_range" {
		t.Fatalf("out-of-range CPU temperature must be invalid: %#v", items)
	}
}

func TestReadLinuxCPUTemperaturesReportsUnsupportedWhenNoCPUHwmon(t *testing.T) {
	items, err := readLinuxCPUTemperatures(context.Background(), t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].MetricID != MetricTemperature || items[0].Status != StatusUnsupported {
		t.Fatalf("missing CPU hwmon must be unsupported, not zero: %#v", items)
	}
}

