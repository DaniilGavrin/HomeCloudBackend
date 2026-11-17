package telemetry

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// DiskKernelCollector reads the complete block layer counters exposed by
// Linux /proc/diskstats. It complements gopsutil's portable IO collector.
type DiskKernelCollector struct {
	interval time.Duration
	mu sync.Mutex
	previous map[string]diskKernelSnapshot
}

type diskKernelSnapshot struct {
	at time.Time
	values []uint64
}

func NewDiskKernelCollector(interval time.Duration) *DiskKernelCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &DiskKernelCollector{interval: interval, previous: make(map[string]diskKernelSnapshot)}
}
func (c *DiskKernelCollector) Name() string { return "disk_kernel" }
func (c *DiskKernelCollector) Interval() time.Duration { return c.interval }

func parseDiskStatsLine(line string) (string, []uint64, bool) {
	fields := strings.Fields(line)
	if len(fields) < 14 { return "", nil, false }
	name := fields[2]
	values := make([]uint64, 0, len(fields)-3)
	for _, field := range fields[3:] {
		value, err := strconv.ParseUint(field, 10, 64)
		if err != nil { return "", nil, false }
		values = append(values, value)
	}
	return name, values, true
}

func (c *DiskKernelCollector) Collect(ctx context.Context) ([]Sample, error) {
	file, err := os.Open("/proc/diskstats")
	if err != nil { return nil, err }
	defer file.Close()
	now := time.Now().UTC()
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Sample, 0, 128)
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil { return out, err }
		name, values, ok := parseDiskStatsLine(scanner.Text())
		if !ok { continue }
		// Keep physical devices and partitions: both are useful for attribution.
		labels := map[string]string{"device": name}
		prev, exists := c.previous[name]
		if exists && len(prev.values) == len(values) {
			seconds := now.Sub(prev.at).Seconds()
			if seconds > 0 {
				delta := func(index int) (uint64, bool) {
					if index >= len(values) || values[index] < prev.values[index] { return 0, false }
					return values[index]-prev.values[index], true
				}
				addRate := func(metric string, index int, scale float64, unit string) {
					if d, ok := delta(index); ok { out = append(out, Sample{Name:metric, Labels:labels, Value:float64(d)*scale/seconds, Unit:unit, Timestamp:now}) }
				}
				addGauge := func(metric string, index int, scale float64, unit string) {
					if index < len(values) { out = append(out, Sample{Name:metric, Labels:labels, Value:float64(values[index])*scale, Unit:unit, Timestamp:now}) }
				}
				addRate("disk.kernel.read_operations_per_second", 0, 1, "operations_per_second")
				addRate("disk.kernel.merged_reads_per_second", 1, 1, "operations_per_second")
				addRate("disk.kernel.read_bytes_per_second", 2, 512, "bytes_per_second")
				addRate("disk.kernel.read_time_ms_per_second", 3, 1, "milliseconds_per_second")
				addRate("disk.kernel.write_operations_per_second", 4, 1, "operations_per_second")
				addRate("disk.kernel.merged_writes_per_second", 5, 1, "operations_per_second")
				addRate("disk.kernel.write_bytes_per_second", 6, 512, "bytes_per_second")
				addRate("disk.kernel.write_time_ms_per_second", 7, 1, "milliseconds_per_second")
				addGauge("disk.kernel.io_in_progress", 8, 1, "operations")
				addRate("disk.kernel.io_busy_ms_per_second", 9, 1, "milliseconds_per_second")
				addRate("disk.kernel.weighted_io_ms_per_second", 10, 1, "milliseconds_per_second")
				if d, ok := delta(9); ok { out = append(out, Sample{Name:"disk.kernel.utilization_percent", Labels:labels, Value:float64(d)/(seconds*10), Unit:"percent", Timestamp:now}) }
				if d, ok := delta(10); ok { out = append(out, Sample{Name:"disk.kernel.average_queue_depth", Labels:labels, Value:float64(d)/(seconds*1000), Unit:"operations", Timestamp:now}) }
				if d, ok := delta(0); ok && d > 0 { if ms, valid := delta(3); valid { out = append(out, Sample{Name:"disk.kernel.read_await_ms", Labels:labels, Value:float64(ms)/float64(d), Unit:"milliseconds", Timestamp:now}) } }
				if d, ok := delta(4); ok && d > 0 { if ms, valid := delta(7); valid { out = append(out, Sample{Name:"disk.kernel.write_await_ms", Labels:labels, Value:float64(ms)/float64(d), Unit:"milliseconds", Timestamp:now}) } }
				// Linux 4.18+ appends discard counters; Linux 5.5+ appends flush counters.
				if len(values) >= 15 {
					addRate("disk.kernel.discard_operations_per_second", 11, 1, "operations_per_second")
					addRate("disk.kernel.merged_discards_per_second", 12, 1, "operations_per_second")
					addRate("disk.kernel.discard_bytes_per_second", 13, 512, "bytes_per_second")
					addRate("disk.kernel.discard_time_ms_per_second", 14, 1, "milliseconds_per_second")
				}
				if len(values) >= 17 {
					addRate("disk.kernel.flush_operations_per_second", 15, 1, "operations_per_second")
					addRate("disk.kernel.flush_time_ms_per_second", 16, 1, "milliseconds_per_second")
				}
			}
		}
		c.previous[name] = diskKernelSnapshot{at:now, values:append([]uint64(nil), values...)}
	}
	if err := scanner.Err(); err != nil { return out, err }
	return out, nil
}
