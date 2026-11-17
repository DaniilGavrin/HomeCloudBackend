package telemetry

import (
	"context"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
)

type CPUCollector struct {
	interval time.Duration
	previous cpu.TimesStat
	hasPrevious bool
}

func NewCPUCollector(interval time.Duration) *CPUCollector {
	if interval < time.Second { interval = time.Second }
	return &CPUCollector{interval: interval}
}
func (c *CPUCollector) Name() string { return "cpu" }
func (c *CPUCollector) Interval() time.Duration { return c.interval }
func (c *CPUCollector) Collect(ctx context.Context) ([]Sample, error) {
	// One bounded measurement gives both aggregate and per-core utilization.
	values, err := cpu.PercentWithContext(ctx, 200*time.Millisecond, true)
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(values)+1)
	if len(values) > 0 {
		total := 0.0
		for i, value := range values {
			total += value
			out = append(out, Sample{Name:"cpu.core.usage_percent", Labels:map[string]string{"core": itoa(i)}, Value:value, Unit:"percent", Timestamp:now})
		}
		out = append(out, Sample{Name:"cpu.usage_percent", Value:total/float64(len(values)), Unit:"percent", Timestamp:now})
	}
	if times, err := cpu.TimesWithContext(ctx, false); err == nil && len(times) > 0 {
		current := times[0]
		if c.hasPrevious {
			appendCPUModeSamples(&out, c.previous, current, now)
		}
		c.previous = current
		c.hasPrevious = true
	}
	if runtime.GOOS == "linux" {
		if avg, err := load.AvgWithContext(ctx); err == nil && avg != nil {
			out = append(out,
				Sample{Name:"cpu.load.one_minute", Value:avg.Load1, Unit:"load", Timestamp:now},
				Sample{Name:"cpu.load.five_minutes", Value:avg.Load5, Unit:"load", Timestamp:now},
				Sample{Name:"cpu.load.fifteen_minutes", Value:avg.Load15, Unit:"load", Timestamp:now},
			)
		}
	}
	return out, nil
}

type MemoryCollector struct{ interval time.Duration }

func NewMemoryCollector(interval time.Duration) *MemoryCollector {
	if interval < time.Second { interval = time.Second }
	return &MemoryCollector{interval: interval}
}
func (c *MemoryCollector) Name() string { return "memory" }
func (c *MemoryCollector) Interval() time.Duration { return c.interval }
func (c *MemoryCollector) Collect(ctx context.Context) ([]Sample, error) {
	now := time.Now().UTC()
	vm, err := mem.VirtualMemoryWithContext(ctx)
	if err != nil { return nil, err }
	out := []Sample{
		{Name:"memory.total_bytes", Value:float64(vm.Total), Unit:"bytes", Timestamp:now},
		{Name:"memory.used_bytes", Value:float64(vm.Used), Unit:"bytes", Timestamp:now},
		{Name:"memory.available_bytes", Value:float64(vm.Available), Unit:"bytes", Timestamp:now},
		{Name:"memory.free_bytes", Value:float64(vm.Free), Unit:"bytes", Timestamp:now},
		{Name:"memory.used_percent", Value:vm.UsedPercent, Unit:"percent", Timestamp:now},
	}
	if vm.Cached > 0 { out = append(out, Sample{Name:"memory.cached_bytes", Value:float64(vm.Cached), Unit:"bytes", Timestamp:now}) }
	if vm.Buffers > 0 { out = append(out, Sample{Name:"memory.buffers_bytes", Value:float64(vm.Buffers), Unit:"bytes", Timestamp:now}) }
	swap, err := mem.SwapMemoryWithContext(ctx)
	if err == nil && swap != nil {
		out = append(out,
			Sample{Name:"memory.swap.total_bytes", Value:float64(swap.Total), Unit:"bytes", Timestamp:now},
			Sample{Name:"memory.swap.used_bytes", Value:float64(swap.Used), Unit:"bytes", Timestamp:now},
			Sample{Name:"memory.swap.free_bytes", Value:float64(swap.Free), Unit:"bytes", Timestamp:now},
			Sample{Name:"memory.swap.used_percent", Value:swap.UsedPercent, Unit:"percent", Timestamp:now},
		)
	}
	return out, nil
}

// itoa avoids fmt allocations in the per-core hot path.
func itoa(v int) string {
	if v == 0 { return "0" }
	var buf [20]byte
	i := len(buf)
	for v > 0 { i--; buf[i] = byte('0' + v%10); v /= 10 }
	return string(buf[i:])
}

func appendCPUModeSamples(out *[]Sample, previous, current cpu.TimesStat, now time.Time) {
	deltas := map[string]float64{
		"user": current.User - previous.User,
		"system": current.System - previous.System,
		"idle": current.Idle - previous.Idle,
		"nice": current.Nice - previous.Nice,
		"iowait": current.Iowait - previous.Iowait,
		"irq": current.Irq - previous.Irq,
		"softirq": current.Softirq - previous.Softirq,
		"steal": current.Steal - previous.Steal,
	}
	total := 0.0
	for _, delta := range deltas {
		if delta < 0 { return }
		total += delta
	}
	if total <= 0 { return }
	for mode, delta := range deltas {
		*out = append(*out, Sample{
			Name:"cpu.mode."+mode+"_percent", Value:delta/total*100,
			Unit:"percent", Timestamp:now,
		})
	}
}
