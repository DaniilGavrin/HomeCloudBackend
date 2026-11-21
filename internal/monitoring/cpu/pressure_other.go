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
	return []MetricItem{
		statusItem(MetricPressureSome, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.pressure", now),
		statusItem(MetricPressureFull, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.pressure", now),
	}, nil
}
