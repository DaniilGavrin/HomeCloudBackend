package system

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"HomeCloudBackend/internal/commands"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/process"
)

type ProcessRecord struct {
	PID           int32    `json:"pid"`
	Name          string   `json:"name"`
	Status        []string `json:"status,omitempty"`
	RSSBytes      uint64   `json:"rss_bytes"`
	MemoryPercent float64  `json:"memory_percent"`
	Threads       int32    `json:"threads,omitempty"`
	StartedAt     string   `json:"started_at,omitempty"`
}

type processListRequest struct {
	Limit  int    `json:"limit,omitempty"`
	SortBy string `json:"sort_by,omitempty"`
}

// RegisterProcessList adds a bounded, on-demand process inventory. It never
// exposes command lines, environment variables, or process file descriptors.
func RegisterProcessList(r *commands.Registry, requireSubscription bool) error {
	return r.Register("system.processes", func(parent context.Context, msg commands.Inbound, peer commands.ClientContext) (any, error) {
		if requireSubscription && !peer.Subscribed("system.metrics") {
			return nil, errors.New("subscribe to system.metrics first")
		}
		var request processListRequest
		if len(msg.Data) > 0 {
			if err := json.Unmarshal(msg.Data, &request); err != nil {
				return nil, errors.New("invalid system.processes data")
			}
		}
		if request.Limit == 0 { request.Limit = 50 }
		if request.Limit < 1 || request.Limit > 200 {
			return nil, errors.New("process limit must be between 1 and 200")
		}
		if request.SortBy == "" { request.SortBy = "memory" }
		if request.SortBy != "memory" && request.SortBy != "pid" && request.SortBy != "name" {
			return nil, errors.New("sort_by must be memory, pid, or name")
		}

		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		defer cancel()
		all, err := process.ProcessesWithContext(ctx)
		if err != nil { return nil, err }
		var totalMemory uint64
		if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil && vm != nil {
			totalMemory = vm.Total
		}
		items := make([]ProcessRecord, 0, len(all))
		for _, p := range all {
			if ctx.Err() != nil { break }
			name, nameErr := p.NameWithContext(ctx)
			if nameErr != nil || name == "" { continue }
			record := ProcessRecord{PID:p.Pid, Name:name}
			if info, err := p.MemoryInfoWithContext(ctx); err == nil && info != nil {
				record.RSSBytes = info.RSS
				if totalMemory > 0 {
					record.MemoryPercent = float64(info.RSS) / float64(totalMemory) * 100
				}
			}
			if status, err := p.StatusWithContext(ctx); err == nil {
				record.Status = status
			}
			if threads, err := p.NumThreadsWithContext(ctx); err == nil {
				record.Threads = threads
			}
			if created, err := p.CreateTimeWithContext(ctx); err == nil && created > 0 {
				record.StartedAt = time.UnixMilli(created).UTC().Format(time.RFC3339)
			}
			items = append(items, record)
		}
		if err := ctx.Err(); err != nil && len(items) == 0 {
			return nil, err
		}
		sort.Slice(items, func(i, j int) bool {
			switch request.SortBy {
			case "pid":
				return items[i].PID < items[j].PID
			case "name":
				if items[i].Name == items[j].Name { return items[i].PID < items[j].PID }
				return items[i].Name < items[j].Name
			default:
				if items[i].RSSBytes == items[j].RSSBytes { return items[i].PID < items[j].PID }
				return items[i].RSSBytes > items[j].RSSBytes
			}
		})
		total := len(items)
		truncated := total > request.Limit
		if truncated { items = items[:request.Limit] }
		return map[string]any{
			"process_count": len(all),
			"returned": len(items),
			"truncated": truncated,
			"sort_by": request.SortBy,
			"processes": items,
			"collected_at": time.Now().UTC().Format(time.RFC3339Nano),
		}, nil
	})
}
