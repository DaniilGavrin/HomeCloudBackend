//go:build !linux

package cpu

import (
	"context"
	"time"
)

func (c *SystemCollector) ReadTemperatureMetrics(ctx context.Context) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return []MetricItem{statusItem(MetricTemperature, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.temperature", time.Now().UTC())}, nil
}
