//go:build linux

package cpu

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

type perfEventSpec struct {
	metricID string
	config uint64
}

var genericPerfEvents = []perfEventSpec{
	{MetricPerfCycles, unix.PERF_COUNT_HW_CPU_CYCLES},
	{MetricPerfRefCycles, unix.PERF_COUNT_HW_REF_CPU_CYCLES},
	{MetricPerfInstructions, unix.PERF_COUNT_HW_INSTRUCTIONS},
	{MetricPerfBranches, unix.PERF_COUNT_HW_BRANCH_INSTRUCTIONS},
	{MetricPerfBranchMisses, unix.PERF_COUNT_HW_BRANCH_MISSES},
	{MetricPerfCacheReferences, unix.PERF_COUNT_HW_CACHE_REFERENCES},
	{MetricPerfCacheMisses, unix.PERF_COUNT_HW_CACHE_MISSES},
	{MetricPerfBusCycles, unix.PERF_COUNT_HW_BUS_CYCLES},
	{MetricPerfStalledFrontend, unix.PERF_COUNT_HW_STALLED_CYCLES_FRONTEND},
	{MetricPerfStalledBackend, unix.PERF_COUNT_HW_STALLED_CYCLES_BACKEND},
}

type perfReader struct {
	cpus []int
	fds map[int]map[string]int
	openErrors map[string]error
	last map[string]uint64
	lastAt time.Time
}

func (c *SystemCollector) ReadPerfMetrics(ctx context.Context) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	c.perfMu.Lock()
	defer c.perfMu.Unlock()

	cpus, err := onlineCPUList()
	if err != nil {
		if c.perf == nil {
			c.perf = newPerfReader(nil, err)
		}
		return c.perf.read(ctx)
	}
	if c.perf == nil || !sameCPUList(c.perf.cpus, cpus) {
		if c.perf != nil {
			c.perf.close()
		}
		c.perf = newPerfReader(cpus, nil)
	}
	return c.perf.read(ctx)
}

func (c *SystemCollector) StopPerfMetrics() {
	c.perfMu.Lock()
	defer c.perfMu.Unlock()
	if c.perf != nil {
		c.perf.close()
		c.perf = nil
	}
}

func onlineCPUList() ([]int, error) {
	value, err := readSysfsValue(defaultSysfsCPUPath + "/online")
	if err != nil {
		return nil, fmt.Errorf("read online CPU list: %w", err)
	}
	cpus, err := parseCPUList(value)
	if err != nil {
		return nil, fmt.Errorf("parse online CPU list: %w", err)
	}
	sort.Ints(cpus)
	return cpus, nil
}

func sameCPUList(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func newPerfReader(cpus []int, cpuListErr error) *perfReader {
	reader := &perfReader{
		cpus: append([]int(nil), cpus...),
		fds: make(map[int]map[string]int),
		openErrors: make(map[string]error),
		last: make(map[string]uint64),
	}
	if cpuListErr != nil {
		for _, event := range genericPerfEvents {
			reader.openErrors[event.metricID] = cpuListErr
		}
		return reader
	}
	for _, cpuID := range cpus {
		reader.fds[cpuID] = make(map[string]int)
		for _, event := range genericPerfEvents {
			attr := unix.PerfEventAttr{
				Type: uint32(unix.PERF_TYPE_HARDWARE),
				Size: uint32(unsafe.Sizeof(unix.PerfEventAttr{})),
				Config: event.config,
				Read_format: unix.PERF_FORMAT_TOTAL_TIME_ENABLED | unix.PERF_FORMAT_TOTAL_TIME_RUNNING,
				Bits: unix.PerfBitDisabled,
			}
			fd, err := unix.PerfEventOpen(&attr, -1, cpuID, -1, 0)
			if err == nil {
				err = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_RESET, 0)
			}
			if err == nil {
				err = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_ENABLE, 0)
			}
			if err != nil {
				if fd >= 0 {
					_ = unix.Close(fd)
				}
				if reader.openErrors[event.metricID] == nil {
					reader.openErrors[event.metricID] = err
				}
				continue
			}
			reader.fds[cpuID][event.metricID] = fd
		}
	}
	return reader
}

func (r *perfReader) close() {
	for _, cpuFDs := range r.fds {
		for _, fd := range cpuFDs {
			_ = unix.IoctlSetInt(fd, unix.PERF_EVENT_IOC_DISABLE, 0)
			_ = unix.Close(fd)
		}
	}
	r.fds = nil
	r.cpus = nil
	r.last = nil
}

func (r *perfReader) read(ctx context.Context) ([]MetricItem, error) {
	now := time.Now().UTC()
	values := make(map[string]uint64, len(genericPerfEvents))
	statuses := make(map[string]MetricItem, len(genericPerfEvents))
	for _, event := range genericPerfEvents {
		var total uint64
		successes, failures := 0, 0
		var firstErr error
		for cpuID, cpuFDs := range r.fds {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			fd, ok := cpuFDs[event.metricID]
			if !ok {
				failures++
				if firstErr == nil {
					firstErr = r.openErrors[event.metricID]
				}
				continue
			}
			value, readErr := readPerfEvent(fd)
			if readErr != nil {
				failures++
				if firstErr == nil {
					firstErr = readErr
				}
				continue
			}
			if math.MaxUint64-total < value {
				firstErr = errors.New("PMU counter aggregate overflow")
				failures++
				continue
			}
			total += value
			successes++
			_ = cpuID
		}
		if successes == 0 {
			if firstErr == nil {
				firstErr = errors.New("no CPU PMU event descriptor is available")
			}
			status, reason, code := classifyPerfError(firstErr)
			statuses[event.metricID] = statusItem(event.metricID, "", status, reason, code, "linux.perf_event_open", now)
			continue
		}
		values[event.metricID] = total
		status := StatusValid
		reason, code := "", ""
		if failures > 0 {
			status, reason, code = StatusPartial, "partial_source", "PARTIAL_RESULT"
		}
		statuses[event.metricID] = MetricItem{
			MetricID: event.metricID, Value: total, Unit: "count",
			ObservedAt: now.Format(time.RFC3339Nano), Status: status,
			Reason: reason, ErrorCode: code, SourceID: "linux.perf_event_open",
		}
	}

	items := make([]MetricItem, 0, len(genericPerfEvents)+3)
	for _, event := range genericPerfEvents {
		items = append(items, statuses[event.metricID])
	}

	windowMS := int64(0)
	if !r.lastAt.IsZero() {
		windowMS = now.Sub(r.lastAt).Milliseconds()
	}
	derive := func(metricID, numeratorID, denominatorID string, percent bool) MetricItem {
		numerator, numeratorOK := values[numeratorID]
		denominator, denominatorOK := values[denominatorID]
		previousNumerator, previousNumeratorOK := r.last[numeratorID]
		previousDenominator, previousDenominatorOK := r.last[denominatorID]
		if !numeratorOK || !denominatorOK {
			return statusItem(metricID, "", StatusUnavailable, "source_unavailable", "PARTIAL_RESULT", "linux.perf_event_open", now)
		}
		if !previousNumeratorOK || !previousDenominatorOK {
			return statusItem(metricID, "", StatusUnknown, "no_baseline", "", "linux.perf_event_open", now)
		}
		if numerator < previousNumerator || denominator < previousDenominator {
			return statusItem(metricID, "", StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "linux.perf_event_open", now)
		}
		deltaNumerator := numerator - previousNumerator
		deltaDenominator := denominator - previousDenominator
		if deltaDenominator == 0 {
			return statusItem(metricID, "", StatusUnavailable, "zero_denominator", "SOURCE_INVALID_DATA", "linux.perf_event_open", now)
		}
		value := float64(deltaNumerator) / float64(deltaDenominator)
		unit := "ratio"
		if percent {
			value *= 100
			unit = "percent"
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			return statusItem(metricID, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.perf_event_open", now)
		}
		return MetricItem{
			MetricID: metricID, Value: value, Unit: unit,
			ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
			SourceID: "linux.perf_event_open", WindowMS: windowMS,
		}
	}
	items = append(items,
		derive(MetricPerfIPC, MetricPerfInstructions, MetricPerfCycles, false),
		derive(MetricPerfBranchMissRate, MetricPerfBranchMisses, MetricPerfBranches, true),
		derive(MetricPerfCacheMissRate, MetricPerfCacheMisses, MetricPerfCacheReferences, true),
	)
	r.last = values
	r.lastAt = now
	return items, nil
}

func readPerfEvent(fd int) (uint64, error) {
	var data [24]byte
	n, err := unix.Read(fd, data[:])
	if err != nil {
		return 0, fmt.Errorf("read PMU event: %w", err)
	}
	if n != len(data) {
		return 0, fmt.Errorf("short PMU event read: got %d bytes, want %d", n, len(data))
	}
	value := binary.LittleEndian.Uint64(data[0:8])
	timeEnabled := binary.LittleEndian.Uint64(data[8:16])
	timeRunning := binary.LittleEndian.Uint64(data[16:24])
	if timeRunning == 0 {
		return 0, errors.New("PMU event was not scheduled")
	}
	if timeRunning >= timeEnabled {
		return value, nil
	}
	scaled := float64(value) * float64(timeEnabled) / float64(timeRunning)
	if math.IsNaN(scaled) || math.IsInf(scaled, 0) || scaled < 0 || scaled > float64(math.MaxUint64) {
		return 0, errors.New("scaled PMU event value is out of range")
	}
	return uint64(math.Round(scaled)), nil
}

func classifyPerfError(err error) (status, reason, code string) {
	if errors.Is(err, unix.EACCES) || errors.Is(err, unix.EPERM) || errors.Is(err, os.ErrPermission) {
		return StatusPermissionDenied, "permission_denied", "SOURCE_PERMISSION_DENIED"
	}
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENODEV) {
		return StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
	}
	return classifySourceError(err)
}

var _ = strings.Builder{}
