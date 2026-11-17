package telemetry

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v4/host"
)

type HostCollector struct{ interval time.Duration }

func NewHostCollector(interval time.Duration) *HostCollector {
	if interval < 30*time.Second { interval = 30*time.Second }
	return &HostCollector{interval:interval}
}
func (c *HostCollector) Name() string { return "host" }
func (c *HostCollector) Interval() time.Duration { return c.interval }
func (c *HostCollector) Collect(ctx context.Context) ([]Sample, error) {
	info, err := host.InfoWithContext(ctx)
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := []Sample{
		{Name:"system.uptime_seconds", Value:float64(info.Uptime), Unit:"seconds", Timestamp:now},
	}
	if info.BootTime > 0 {
		out = append(out, Sample{Name:"system.boot_time_unix", Value:float64(info.BootTime), Unit:"unix_seconds", Timestamp:now})
	}
	return out, nil
}
