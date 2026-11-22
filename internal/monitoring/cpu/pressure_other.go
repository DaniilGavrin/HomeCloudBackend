//go:build !linux

package cpu

import (
	"context"
	"time"
)

func (c *SystemCollector) ReadPressureMetrics(ctx context.Context) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	items := make([]MetricItem, 0, 8)
	for _, metricID := range []string{MetricPressureSome, MetricPressureFull} {
		for _, window := range []int64{10_000, 60_000, 300_000} {
			item := statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.pressure", now)
			item.WindowMS = window
			items = append(items, item)
		}
		items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.pressure", now))
	}
	return items, nil
}
