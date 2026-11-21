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
	return []MetricItem{statusItem(MetricRunQueue, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.scheduler", now)}, nil
}
