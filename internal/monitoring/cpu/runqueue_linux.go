//go:build linux

package cpu

import (
	"bufio"
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"time"
)

var defaultSchedDebugPaths = []string{
	"/proc/sched_debug",
	"/sys/kernel/debug/sched/debug",
}

func (c *SystemCollector) ReadSystemCounters(ctx context.Context) ([]MetricItem, error) {
	items, err := readLinuxSystemCounters(ctx, defaultProcStatPath)
	if err != nil {
		return nil, err
	}
	runQueue := readLinuxRunQueue(ctx, defaultSchedDebugPaths)
	replaced := false
	for i := range items {
		if items[i].MetricID == MetricRunQueue {
			items[i] = runQueue
			replaced = true
			break
		}
	}
	if !replaced {
		items = append(items, runQueue)
	}
	return items, nil
}

// readLinuxRunQueue uses only scheduler data whose semantics match the
// run-queue metric: runnable tasks waiting to be scheduled. /proc/stat's
// procs_running also includes currently executing tasks, so it is not a
// compatible fallback and must never be reported as system.cpu.runqueue.
func readLinuxRunQueue(ctx context.Context, schedDebugPaths []string) MetricItem {
	now := time.Now().UTC()
	var lastReadErr error
	for _, path := range schedDebugPaths {
		if err := ctx.Err(); err != nil {
			return statusItem(MetricRunQueue, "", StatusCancelled, "cancelled", "REQUEST_CANCELLED", "linux.host.proc.sched_debug", now)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				lastReadErr = err
			}
			continue
		}
		count, found, parseErr := parseSchedDebugRunQueue(data)
		if parseErr != nil {
			return statusItem(MetricRunQueue, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.host.proc.sched_debug", now)
		}
		if found {
			return MetricItem{
				MetricID: MetricRunQueue, Value: count, Unit: "count",
				ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
				SourceID: "linux.host.proc.sched_debug.nr_running",
			}
		}
	}

	if lastReadErr != nil {
		status, reason, code := classifySourceError(lastReadErr)
		return statusItem(MetricRunQueue, "", status, reason, code, "linux.host.proc.sched_debug", now)
	}
	return statusItem(MetricRunQueue, "", StatusUnavailable, "no_semantically_compatible_source", "SOURCE_UNAVAILABLE", "linux.host.proc.sched_debug", now)
}

func parseSchedDebugRunQueue(data []byte) (uint64, bool, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var total uint64
	found := false
	for scanner.Scan() {
		rawLine := scanner.Text()
		indent := len(rawLine) - len(strings.TrimLeft(rawLine, " \t"))
		// Count only the runqueue-level nr_running entry. Nested cfs_rq
		// entries describe sub-queues and would otherwise be double-counted.
		if indent > 2 {
			continue
		}
		line := strings.TrimSpace(rawLine)
		if !strings.Contains(line, "nr_running") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}
		raw := strings.Trim(parts[len(parts)-1], ",")
		value, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return 0, false, err
		}
		if ^uint64(0)-total < value {
			return 0, false, errors.New("scheduler runqueue counter overflow")
		}
		total += value
		found = true
	}
	if err := scanner.Err(); err != nil {
		return 0, false, err
	}
	return total, found, nil
}
