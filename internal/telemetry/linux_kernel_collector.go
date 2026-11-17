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

type kernelCounterSnapshot struct {
	at time.Time
	values map[string]uint64
}

type LinuxKernelCollector struct {
	interval time.Duration
	mu sync.Mutex
	previous kernelCounterSnapshot
}

func NewLinuxKernelCollector(interval time.Duration) *LinuxKernelCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &LinuxKernelCollector{interval:interval}
}
func (c *LinuxKernelCollector) Name() string { return "linux_kernel" }
func (c *LinuxKernelCollector) Interval() time.Duration { return c.interval }

func (c *LinuxKernelCollector) Collect(ctx context.Context) ([]Sample, error) {
	now := time.Now().UTC()
	values := make(map[string]uint64, 16)
	if err := readKernelStat(ctx, "/proc/stat", values); err != nil { return nil, err }
	if err := readKernelStat(ctx, "/proc/vmstat", values); err != nil { return nil, err }
	out := make([]Sample, 0, len(values)+2)
	for _, key := range []string{"procs_running","procs_blocked"} {
		if value, ok := values[key]; ok {
			out = append(out, Sample{Name:"system."+key, Value:float64(value), Unit:"processes", Timestamp:now})
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.previous.at.IsZero() && now.After(c.previous.at) {
		seconds := now.Sub(c.previous.at).Seconds()
		metrics := map[string]string{
			"ctxt":"system.context_switches_per_second",
			"intr":"system.interrupts_per_second",
			"softirq":"system.softirqs_per_second",
			"processes":"system.forks_per_second",
			"pgfault":"memory.page_faults_per_second",
			"pgmajfault":"memory.major_page_faults_per_second",
			"pswpin":"memory.swap_in_pages_per_second",
			"pswpout":"memory.swap_out_pages_per_second",
			"pgscan_kswapd":"memory.reclaim_scan_kswapd_pages_per_second",
			"pgscan_direct":"memory.reclaim_scan_direct_pages_per_second",
			"pgsteal_kswapd":"memory.reclaim_steal_kswapd_pages_per_second",
			"pgsteal_direct":"memory.reclaim_steal_direct_pages_per_second",
		}
		for key, metric := range metrics {
			current, ok := values[key]; if !ok { continue }
			previous, ok := c.previous.values[key]
			if !ok || current < previous { continue }
			out = append(out, Sample{Name:metric, Value:float64(current-previous)/seconds, Unit:"per_second", Timestamp:now})
		}
	}
	c.previous = kernelCounterSnapshot{at:now, values:values}
	return out, nil
}

func readKernelStat(ctx context.Context, path string, values map[string]uint64) error {
	file, err := os.Open(path)
	if err != nil { return nil } // Optional on non-Linux or restricted environments.
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil { return err }
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 { continue }
		key := strings.TrimSuffix(fields[0], ":")
		switch key {
		case "ctxt","intr","softirq","processes","procs_running","procs_blocked",
			"pgfault","pgmajfault","pswpin","pswpout","pgscan_kswapd","pgscan_direct",
			"pgsteal_kswapd","pgsteal_direct":
			value, parseErr := strconv.ParseUint(fields[1],10,64)
			if parseErr == nil { values[key]=value }
		}
	}
	return scanner.Err()
}
