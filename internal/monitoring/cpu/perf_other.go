//go:build !linux

package cpu

import (
	"context"
	"time"
)

type perfReader struct{}

func (c *SystemCollector) ReadPerfMetrics(ctx context.Context) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	items := make([]MetricItem, 0, len(genericPerfMetricIDs))
	for _, metricID := range genericPerfMetricIDs {
		items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.perf_event", now))
	}
	return items, nil
}

func (c *SystemCollector) StopPerfMetrics() {}

var genericPerfMetricIDs = []string{
	MetricPerfCycles, MetricPerfRefCycles, MetricPerfInstructions, MetricPerfIPC,
	MetricPerfBranches, MetricPerfBranchMisses, MetricPerfCacheReferences,
	MetricPerfCacheMisses, MetricPerfBusCycles, MetricPerfStalledFrontend,
	MetricPerfStalledBackend, MetricPerfBranchMissRate, MetricPerfCacheMissRate,
}
