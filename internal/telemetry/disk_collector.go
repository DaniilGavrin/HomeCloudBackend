package telemetry

import (
	"context"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
)

type diskPrevious struct {
	at time.Time
	readBytes, writeBytes uint64
	readOps, writeOps uint64
}

type DiskCollector struct {
	interval time.Duration
	previous map[string]diskPrevious
	lastUsage time.Time
}

func NewDiskCollector(interval time.Duration) *DiskCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &DiskCollector{interval:interval, previous:make(map[string]diskPrevious)}
}
func (c *DiskCollector) Name() string { return "disk" }
func (c *DiskCollector) Interval() time.Duration { return c.interval }
func (c *DiskCollector) Collect(ctx context.Context) ([]Sample, error) {
	now := time.Now().UTC()
	out := make([]Sample, 0, 64)
	stats, err := disk.IOCountersWithContext(ctx)
	if err == nil {
		for name, current := range stats {
			prev, ok := c.previous[name]
			if ok && current.ReadBytes >= prev.readBytes && current.WriteBytes >= prev.writeBytes &&
				current.ReadCount >= prev.readOps && current.WriteCount >= prev.writeOps {
				seconds := now.Sub(prev.at).Seconds()
				if seconds > 0 {
					labels := map[string]string{"device":name}
					out = append(out,
						Sample{Name:"disk.read_bytes_per_second", Labels:labels, Value:float64(current.ReadBytes-prev.readBytes)/seconds, Unit:"bytes_per_second", Timestamp:now},
						Sample{Name:"disk.write_bytes_per_second", Labels:labels, Value:float64(current.WriteBytes-prev.writeBytes)/seconds, Unit:"bytes_per_second", Timestamp:now},
						Sample{Name:"disk.read_operations_per_second", Labels:labels, Value:float64(current.ReadCount-prev.readOps)/seconds, Unit:"operations_per_second", Timestamp:now},
						Sample{Name:"disk.write_operations_per_second", Labels:labels, Value:float64(current.WriteCount-prev.writeOps)/seconds, Unit:"operations_per_second", Timestamp:now},
					)
				}
			}
			c.previous[name] = diskPrevious{at:now, readBytes:current.ReadBytes, writeBytes:current.WriteBytes, readOps:current.ReadCount, writeOps:current.WriteCount}
		}
	}
	// Capacity and inode scans are intentionally less frequent than IO sampling.
	if c.lastUsage.IsZero() || now.Sub(c.lastUsage) >= 30*time.Second {
		partitions, partErr := disk.PartitionsWithContext(ctx, false)
		if partErr == nil {
			for _, p := range partitions {
				usage, usageErr := disk.UsageWithContext(ctx, p.Mountpoint)
				if usageErr != nil || usage == nil { continue }
				labels := map[string]string{"device":p.Device, "mountpoint":p.Mountpoint}
				out = append(out,
					Sample{Name:"disk.capacity.total_bytes", Labels:labels, Value:float64(usage.Total), Unit:"bytes", Timestamp:now},
					Sample{Name:"disk.capacity.used_bytes", Labels:labels, Value:float64(usage.Used), Unit:"bytes", Timestamp:now},
					Sample{Name:"disk.capacity.free_bytes", Labels:labels, Value:float64(usage.Free), Unit:"bytes", Timestamp:now},
					Sample{Name:"disk.capacity.used_percent", Labels:labels, Value:usage.UsedPercent, Unit:"percent", Timestamp:now},
				)
				if usage.InodesTotal > 0 {
					out = append(out,
						Sample{Name:"disk.inodes.total", Labels:labels, Value:float64(usage.InodesTotal), Unit:"inodes", Timestamp:now},
						Sample{Name:"disk.inodes.used", Labels:labels, Value:float64(usage.InodesUsed), Unit:"inodes", Timestamp:now},
					)
				}
			}
			c.lastUsage = now
		}
	}
	if err != nil && len(out) == 0 { return nil, err }
	return out, nil
}
