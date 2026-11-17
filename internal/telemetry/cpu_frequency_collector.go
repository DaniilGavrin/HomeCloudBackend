package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type CPUFrequencyCollector struct{ interval time.Duration }

func NewCPUFrequencyCollector(interval time.Duration) *CPUFrequencyCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &CPUFrequencyCollector{interval:interval}
}
func (c *CPUFrequencyCollector) Name() string { return "cpu_frequency" }
func (c *CPUFrequencyCollector) Interval() time.Duration { return c.interval }

func (c *CPUFrequencyCollector) Collect(ctx context.Context) ([]Sample, error) {
	paths, err := filepath.Glob("/sys/devices/system/cpu/cpu[0-9]*/cpufreq")
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample,0,len(paths)*2)
	for _, dir := range paths {
		if err := ctx.Err(); err != nil { return out, err }
		core := strings.TrimPrefix(filepath.Base(filepath.Dir(dir)),"cpu")
		labels := map[string]string{"core":core}
		if raw, e := os.ReadFile(filepath.Join(dir,"scaling_governor")); e == nil {
			if governor := strings.TrimSpace(string(raw)); governor != "" { labels["governor"]=governor }
		}
		var current float64
		for _, file := range []string{"cpuinfo_cur_freq","scaling_cur_freq"} {
			raw, e := os.ReadFile(filepath.Join(dir,file))
			if e != nil { continue }
			value, e := strconv.ParseFloat(strings.TrimSpace(string(raw)),64)
			if e == nil && value > 0 { current=value/1000; break }
		}
		if current > 0 {
			out=append(out,Sample{Name:"cpu.frequency.current_mhz",Labels:labels,Value:current,Unit:"megahertz",Timestamp:now})
		}
		if raw, e := os.ReadFile(filepath.Join(dir,"cpuinfo_max_freq")); e == nil {
			if value, e := strconv.ParseFloat(strings.TrimSpace(string(raw)),64); e == nil && value > 0 {
				out=append(out,Sample{Name:"cpu.frequency.max_mhz",Labels:labels,Value:value/1000,Unit:"megahertz",Timestamp:now})
			}
		}
	}
	return out,nil
}
