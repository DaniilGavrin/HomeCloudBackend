package telemetry

import (
	"context"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v4/process"
)

type BackendCollector struct{ interval time.Duration }

func NewBackendCollector(interval time.Duration) *BackendCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &BackendCollector{interval:interval}
}
func (c *BackendCollector) Name() string { return "backend" }
func (c *BackendCollector) Interval() time.Duration { return c.interval }
func (c *BackendCollector) Collect(ctx context.Context) ([]Sample, error) {
	now := time.Now().UTC()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	out := []Sample{
		{Name:"backend.goroutines", Value:float64(runtime.NumGoroutine()), Unit:"goroutines", Timestamp:now},
		{Name:"backend.heap_alloc_bytes", Value:float64(stats.Alloc), Unit:"bytes", Timestamp:now},
		{Name:"backend.heap_inuse_bytes", Value:float64(stats.HeapInuse), Unit:"bytes", Timestamp:now},
		{Name:"backend.heap_objects", Value:float64(stats.HeapObjects), Unit:"objects", Timestamp:now},
		{Name:"backend.gc_cycles", Value:float64(stats.NumGC), Unit:"cycles", Timestamp:now},
		{Name:"backend.total_allocated_bytes", Value:float64(stats.TotalAlloc), Unit:"bytes", Timestamp:now},
	}
	p, err := process.NewProcessWithContext(ctx, int32(os.Getpid()))
	if err != nil { return out, nil }
	if memory, err := p.MemoryInfoWithContext(ctx); err == nil && memory != nil {
		out = append(out,
			Sample{Name:"backend.process.rss_bytes", Value:float64(memory.RSS), Unit:"bytes", Timestamp:now},
			Sample{Name:"backend.process.vms_bytes", Value:float64(memory.VMS), Unit:"bytes", Timestamp:now},
		)
	}
	if threads, err := p.NumThreadsWithContext(ctx); err == nil {
		out = append(out, Sample{Name:"backend.process.threads", Value:float64(threads), Unit:"threads", Timestamp:now})
	}
	return out, nil
}
