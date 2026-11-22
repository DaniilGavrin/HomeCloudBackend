//go:build !linux

package cpu

import (
	"context"
	"time"
)

type perfReader struct{}

func (c *SystemCollector) ReadPerfMetrics(ctx context.Context, targets ...string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	metricIDs := selectPerfMetricIDs(targets)
	items := make([]MetricItem, 0, len(metricIDs))
	for _, metricID := range metricIDs {
		items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.perf_event", now))
	}
	return items, nil
}

func (c *SystemCollector) StopPerfMetrics() {}
