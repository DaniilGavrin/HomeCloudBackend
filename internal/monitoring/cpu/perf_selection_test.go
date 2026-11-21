package cpu

import (
	"reflect"
	"testing"
)

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
