//go:build linux

package cpu

import (
	"reflect"
	"testing"
)

func TestDerivedPerfValueEnforcesMetricUnitsAndRanges(t *testing.T) {
	if value, unit, ok := derivedPerfValue(MetricPerfIPC, 2, 1); !ok || value != 2 || unit != "ratio" {
		t.Fatalf("IPC = (%v, %q, %v), want (2, ratio, true)", value, unit, ok)
	}
	if value, unit, ok := derivedPerfValue(MetricPerfBranchMissRate, 1, 4); !ok || value != 25 || unit != "percent" {
		t.Fatalf("branch miss rate = (%v, %q, %v), want (25, percent, true)", value, unit, ok)
	}
	if _, _, ok := derivedPerfValue(MetricPerfBranchMissRate, 2, 1); ok {
		t.Fatal("derived PMU percentage above 100 must be invalid")
	}
	if _, _, ok := derivedPerfValue(MetricPerfIPC, 1, 0); ok {
		t.Fatal("derived PMU ratio with zero denominator must be invalid")
	}
}

func TestSelectPerfEventIDsUsesSynchronizedDependencies(t *testing.T) {
	tests := []struct {
		name string
		targets []string
		want []string
	}{
		{
			name: "IPC",
			targets: []string{MetricPerfIPC},
			want: []string{MetricPerfCycles, MetricPerfInstructions},
		},
		{
			name: "branch miss rate",
			targets: []string{MetricPerfBranchMissRate},
			want: []string{MetricPerfBranches, MetricPerfBranchMisses},
		},
		{
			name: "cache miss rate",
			targets: []string{MetricPerfCacheMissRate},
			want: []string{MetricPerfCacheReferences, MetricPerfCacheMisses},
		},
		{
			name: "one raw event",
			targets: []string{MetricPerfCycles},
			want: []string{MetricPerfCycles},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := selectPerfEventIDs(test.targets); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("selected events = %#v, want %#v", got, test.want)
			}
		})
	}
}

func TestSelectPerfGroupIncludesAllRegisteredEventsAndDerivedMetrics(t *testing.T) {
	metrics := selectPerfMetricIDs([]string{MetricPerfTarget})
	if len(metrics) != len(allPerfMetricIDs()) || len(metrics) == 0 {
		t.Fatalf("PMU group metrics = %d, want all %d", len(metrics), len(allPerfMetricIDs()))
	}
	events := selectPerfEventIDs([]string{MetricPerfTarget})
	if len(events) == 0 || len(events) >= len(metrics) {
		t.Fatalf("PMU group event dependencies = %d, metrics = %d", len(events), len(metrics))
	}
}
