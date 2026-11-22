//go:build !linux

package cpu

import (
	"context"
	"time"
)

func (c *SystemCollector) ReadSystemCounters(ctx context.Context) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	items := make([]MetricItem, 0, 6)
	for _, metricID := range []string{
		MetricInterrupts, MetricContextSwitches, MetricForks,
		MetricProcessesRunning, MetricProcessesBlocked, MetricRunQueue,
	} {
		items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.scheduler", now))
	}
	return items, nil
}
