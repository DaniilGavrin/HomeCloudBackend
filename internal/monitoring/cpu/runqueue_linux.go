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

// readLinuxRunQueue reports runnable tasks waiting for a CPU. Linux rq.nr_running
// includes the task currently executing on that CPU, so the parser subtracts
// one for each non-idle current task. /proc/stat procs_running includes both
// running and runnable tasks and is not a compatible fallback.
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
				SourceID: "linux.host.proc.sched_debug.waiting_tasks",
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
	type cpuRunQueue struct {
		nrRunning uint64
		currentPID uint64
		hasNRRunning bool
		hasCurrentPID bool
	}

	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	var total uint64
	var current cpuRunQueue
	foundCPU := false

	flushCPU := func() error {
		if !foundCPU || !current.hasNRRunning || !current.hasCurrentPID {
			return errors.New("scheduler debug CPU block lacks nr_running or curr->pid")
		}
		waiting := current.nrRunning
		if current.currentPID != 0 {
			if waiting == 0 {
				return errors.New("scheduler debug running task exceeds nr_running")
			}
			waiting--
		}
		if ^uint64(0)-total < waiting {
			return errors.New("scheduler runqueue aggregate overflow")
		}
		total += waiting
		return nil
	}

	for scanner.Scan() {
		rawLine := scanner.Text()
		line := strings.TrimSpace(rawLine)
		if strings.HasPrefix(line, "cpu#") {
			if foundCPU {
				if err := flushCPU(); err != nil {
					return 0, false, nil
				}
			}
			current = cpuRunQueue{}
			foundCPU = true
			continue
		}

		indent := len(rawLine) - len(strings.TrimLeft(rawLine, " \t"))
		// Only per-CPU runqueue fields are relevant. Nested cfs_rq entries
		// are not independent CPU runqueues and must not be double-counted.
		if indent > 2 {
			continue
		}
		if strings.Contains(line, "nr_running") {
			value, err := parseSchedDebugUint(line)
			if err != nil {
				return 0, false, err
			}
			current.nrRunning = value
			current.hasNRRunning = true
		} else if strings.Contains(line, "curr->pid") {
			value, err := parseSchedDebugUint(line)
			if err != nil {
				return 0, false, err
			}
			current.currentPID = value
			current.hasCurrentPID = true
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, false, err
	}
	if !foundCPU {
		return 0, false, nil
	}
	if err := flushCPU(); err != nil {
		return 0, false, nil
	}
	return total, true, nil
}

func parseSchedDebugUint(line string) (uint64, error) {
	_, raw, ok := strings.Cut(line, ":")
	if !ok {
		return 0, errors.New("invalid scheduler debug field")
	}
	raw = strings.TrimSpace(raw)
	return strconv.ParseUint(raw, 10, 64)
}
