//go:build linux

package cpu

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestReadLinuxCPUPressurePreservesAllSomeWindowsAndUnsupportedFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	if err := os.WriteFile(path, []byte("some avg10=1.25 avg60=0.80 avg300=0.20 total=12345\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxCPUPressure(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 8 {
		t.Fatalf("pressure items = %d, want four dimensions for some and full: %#v", len(items), items)
	}
	for _, test := range []struct {
		window int64
		value float64
	}{
		{window: 10_000, value: 1.25},
		{window: 60_000, value: 0.80},
		{window: 300_000, value: 0.20},
	} {
		item, ok := findPressureItem(items, MetricPressureSome, test.window)
		if !ok || item.Status != StatusValid || item.Unit != "percent" || item.Value != test.value {
			t.Fatalf("unexpected some PSI window %d: %#v (found=%v)", test.window, item, ok)
		}
	}
	total, ok := findPressureItem(items, MetricPressureSomeTotal, 0)
	if !ok || total.Status != StatusValid || total.Unit != "ns" || total.Value != int64(12_345_000) {
		t.Fatalf("unexpected some PSI total: %#v (found=%v)", total, ok)
	}
	for _, item := range items {
		if item.MetricID == MetricPressureFull && item.Status != StatusUnsupported {
			t.Fatalf("missing full pressure must be unsupported, not zero: %#v", item)
		}
	}
}

func TestReadLinuxCPUPressureReadsFullWhenProvided(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	content := "some avg10=2.5 avg60=1.5 avg300=1.0 total=12345\nfull avg10=0.5 avg60=0.2 avg300=0.1 total=123\n"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxCPUPressure(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 8 {
		t.Fatalf("pressure items = %d, want 8: %#v", len(items), items)
	}
	item, ok := findPressureItem(items, MetricPressureFull, 10_000)
	if !ok || item.Value != 0.5 || item.Status != StatusValid {
		t.Fatalf("unexpected full pressure avg10 result: %#v", item)
	}
	total, ok := findPressureItem(items, MetricPressureFullTotal, 0)
	if !ok || total.Value != int64(123_000) || total.Unit != "ns" {
		t.Fatalf("unexpected full pressure total: %#v", total)
	}
}

func TestReadLinuxCPUPressureRejectsOutOfRangeValue(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	if err := os.WriteFile(path, []byte("some avg10=101 avg60=1 avg300=1 total=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxCPUPressure(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	item, ok := findPressureItem(items, MetricPressureSome, 10_000)
	if !ok || item.Status != StatusInvalid || item.Reason != "invalid_value" {
		t.Fatalf("out-of-range PSI value must be invalid: %#v", item)
	}
}

func findPressureItem(items []MetricItem, metricID string, windowMS int64) (MetricItem, bool) {
	for _, item := range items {
		if item.MetricID == metricID && item.WindowMS == windowMS {
			return item, true
		}
	}
	return MetricItem{}, false
}
