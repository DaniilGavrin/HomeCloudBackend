//go:build linux

package cpu

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestReadLinuxCPUPressurePreservesSomeAndUnsupportedFull(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cpu")
	if err := os.WriteFile(path, []byte("some avg10=1.25 avg60=0.80 avg300=0.20 total=12345\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := readLinuxCPUPressure(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatalf("pressure items = %d, want some and full: %#v", len(items), items)
	}
	if items[0].MetricID != MetricPressureSome || items[0].Value != 1.25 || items[0].Unit != "percent" || items[0].WindowMS != 10_000 || items[0].Status != StatusValid {
		t.Fatalf("unexpected CPU some pressure item: %#v", items[0])
	}
	if items[1].MetricID != MetricPressureFull || items[1].Status != StatusUnsupported {
		t.Fatalf("missing full pressure must be unsupported, not zero: %#v", items[1])
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
	if len(items) != 2 || items[1].MetricID != MetricPressureFull || items[1].Value != 0.5 || items[1].Status != StatusValid {
		t.Fatalf("unexpected full pressure result: %#v", items)
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
	if items[0].Status != StatusInvalid || items[0].Reason != "invalid_value" {
		t.Fatalf("out-of-range PSI value must be invalid: %#v", items[0])
	}
}
