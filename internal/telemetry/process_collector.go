package telemetry

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

type ProcessCollector struct{ interval time.Duration }

func NewProcessCollector(interval time.Duration) *ProcessCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &ProcessCollector{interval:interval}
}
func (c *ProcessCollector) Name() string { return "processes" }
func (c *ProcessCollector) Interval() time.Duration { return c.interval }
func (c *ProcessCollector) Collect(ctx context.Context) ([]Sample, error) {
	pids, err := process.PidsWithContext(ctx)
	if err != nil { return nil, err }
	return []Sample{{
		Name:"processes.count", Value:float64(len(pids)),
		Unit:"processes", Timestamp:time.Now().UTC(),
	}}, nil
}
