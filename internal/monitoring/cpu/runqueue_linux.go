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
	runQueue := readLinuxRunQueue(ctx, defaultProcStatPath, defaultSchedDebugPaths)
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

// readLinuxRunQueue prefers scheduler debug data, where nr_running represents
// queued runnable tasks. /proc/stat procs_running is a documented fallback,
// but includes currently running tasks and is therefore marked partial.
func readLinuxRunQueue(ctx context.Context, procStatPath string, schedDebugPaths []string) MetricItem {
	now := time.Now().UTC()
	var lastReadErr error
	for _, path := range schedDebugPaths {
		if err := ctx.Err(); err != nil {
			return statusItem(MetricRunQueue, "", StatusCancelled, "cancelled", "REQUEST_CANCELLED", "linux.proc.sched_debug", now)
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
			return statusItem(MetricRunQueue, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.proc.sched_debug", now)
		}
		if found {
			return MetricItem{
				MetricID: MetricRunQueue, Value: count, Unit: "count",
				ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
				SourceID: "linux.proc.sched_debug",
			}
		}
	}

	data, err := os.ReadFile(procStatPath)
	if err != nil {
		if lastReadErr != nil {
			err = lastReadErr
		}
		status, reason, code := classifySourceError(err)
		return statusItem(MetricRunQueue, "", status, reason, code, "linux.proc.stat", now)
	}
	value, found, parseErr := parseProcStatCounter(data, "procs_running")
	if parseErr != nil {
		return statusItem(MetricRunQueue, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.proc.stat", now)
	}
	if !found {
		return statusItem(MetricRunQueue, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "linux.proc.stat", now)
	}
	return MetricItem{
		MetricID: MetricRunQueue, Value: value, Unit: "count",
		ObservedAt: now.Format(time.RFC3339Nano), Status: StatusPartial,
		Reason: "includes_running_tasks", ErrorCode: "PARTIAL_RESULT",
		SourceID: "linux.proc.stat.procs_running",
	}
}

func parseSchedDebugRunQueue(data []byte) (uint64, bool, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var total uint64
	found := false
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
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

func parseProcStatCounter(data []byte, key string) (uint64, bool, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 || fields[0] != key {
			continue
		}
		value, err := strconv.ParseUint(fields[1], 10, 64)
		return value, err == nil, err
	}
	if err := scanner.Err(); err != nil {
		return 0, false, err
	}
	return 0, false, nil
}
