package telemetry

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

const processMetricLimit = 25

type processPrevious struct {
	cpuSeconds float64
	at time.Time
	createTime int64
}

type processMetricRecord struct {
	pid int32
	name string
	rss uint64
	memoryPercent float64
	cpuPercent float64
	threads int32
}

type ProcessCollector struct {
	interval time.Duration
	mu sync.Mutex
	previous map[int32]processPrevious
}

func NewProcessCollector(interval time.Duration) *ProcessCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &ProcessCollector{interval:interval, previous:make(map[int32]processPrevious)}
}
func (c *ProcessCollector) Name() string { return "processes" }
func (c *ProcessCollector) Interval() time.Duration { return c.interval }

// Collect records process count and bounded top-process time series. It never
// collects command lines, environment variables, file descriptors or remote endpoints.
func (c *ProcessCollector) Collect(ctx context.Context) ([]Sample, error) {
	all, err := process.ProcessesWithContext(ctx)
	if err != nil { return nil, err }
	vm, _ := mem.VirtualMemoryWithContext(ctx)
	totalMemory := uint64(0)
	if vm != nil { totalMemory = vm.Total }
	now := time.Now().UTC()
	records := make([]processMetricRecord, 0, len(all))
	nextPrevious := make(map[int32]processPrevious, len(all))
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, p := range all {
		if err := ctx.Err(); err != nil { break }
		name, nameErr := p.NameWithContext(ctx)
		if nameErr != nil || name == "" { continue }
		record := processMetricRecord{pid:p.Pid, name:name}
		if len(record.name) > 120 { record.name = record.name[:120] }
		if info, e := p.MemoryInfoWithContext(ctx); e == nil && info != nil {
			record.rss = info.RSS
			if totalMemory > 0 { record.memoryPercent = float64(info.RSS)/float64(totalMemory)*100 }
		}
		if threads, e := p.NumThreadsWithContext(ctx); e == nil { record.threads = threads }
		created, _ := p.CreateTimeWithContext(ctx)
		times, timesErr := p.TimesWithContext(ctx)
		if timesErr == nil && times != nil {
			cpuSeconds := times.User + times.System
			if previous, ok := c.previous[p.Pid]; ok && previous.createTime == created && now.After(previous.at) && cpuSeconds >= previous.cpuSeconds {
				elapsed := now.Sub(previous.at).Seconds()
				if elapsed > 0 { record.cpuPercent = (cpuSeconds-previous.cpuSeconds)/elapsed*100 }
			}
			nextPrevious[p.Pid] = processPrevious{cpuSeconds:cpuSeconds, at:now, createTime:created}
		}
		records = append(records, record)
	}
	c.previous = nextPrevious
	if len(records) == 0 {
		return []Sample{{Name:"processes.count", Value:float64(len(all)), Unit:"processes", Timestamp:now}}, nil
	}
	byCPU := append([]processMetricRecord(nil), records...)
	byMemory := append([]processMetricRecord(nil), records...)
	sort.Slice(byCPU, func(i,j int) bool {
		if byCPU[i].cpuPercent == byCPU[j].cpuPercent { return byCPU[i].pid < byCPU[j].pid }
		return byCPU[i].cpuPercent > byCPU[j].cpuPercent
	})
	sort.Slice(byMemory, func(i,j int) bool {
		if byMemory[i].rss == byMemory[j].rss { return byMemory[i].pid < byMemory[j].pid }
		return byMemory[i].rss > byMemory[j].rss
	})
	selected := make(map[int32]processMetricRecord, processMetricLimit*2)
	for i:=0; i<len(byCPU) && i<processMetricLimit; i++ { selected[byCPU[i].pid]=byCPU[i] }
	for i:=0; i<len(byMemory) && i<processMetricLimit; i++ { selected[byMemory[i].pid]=byMemory[i] }
	out := make([]Sample, 0, 1+len(selected)*4)
	out = append(out, Sample{Name:"processes.count", Value:float64(len(all)), Unit:"processes", Timestamp:now})
	for _, record := range selected {
		labels := map[string]string{"pid":strconv.Itoa(int(record.pid)), "process":record.name}
		out = append(out,
			Sample{Name:"process.cpu.usage_percent", Labels:labels, Value:record.cpuPercent, Unit:"percent", Timestamp:now},
			Sample{Name:"process.memory.rss_bytes", Labels:labels, Value:float64(record.rss), Unit:"bytes", Timestamp:now},
			Sample{Name:"process.memory.used_percent", Labels:labels, Value:record.memoryPercent, Unit:"percent", Timestamp:now},
			Sample{Name:"process.threads", Labels:labels, Value:float64(record.threads), Unit:"threads", Timestamp:now},
		)
	}
	return out, nil
}
