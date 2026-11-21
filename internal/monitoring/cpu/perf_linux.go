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
	"time"
	"unsafe"

	"golang.org/x/sys/unix"
)

const perfFlagFDCloseOnExec = 1 << 3

type perfEventSpec struct {
	metricID string
	eventType uint32
	config uint64
}

func hardwareEvent(metricID string, config uint64) perfEventSpec {
	return perfEventSpec{metricID: metricID, eventType: uint32(unix.PERF_TYPE_HARDWARE), config: config}
}

func cacheEvent(metricID string, cache, operation, result uint64) perfEventSpec {
	config := cache | operation<<8 | result<<16
	return perfEventSpec{metricID: metricID, eventType: uint32(unix.PERF_TYPE_HW_CACHE), config: config}
}

var genericPerfEvents = []perfEventSpec{
	hardwareEvent(MetricPerfCycles, uint64(unix.PERF_COUNT_HW_CPU_CYCLES)),
	hardwareEvent(MetricPerfRefCycles, uint64(unix.PERF_COUNT_HW_REF_CPU_CYCLES)),
	hardwareEvent(MetricPerfInstructions, uint64(unix.PERF_COUNT_HW_INSTRUCTIONS)),
	hardwareEvent(MetricPerfBranches, uint64(unix.PERF_COUNT_HW_BRANCH_INSTRUCTIONS)),
	hardwareEvent(MetricPerfBranchMisses, uint64(unix.PERF_COUNT_HW_BRANCH_MISSES)),
	hardwareEvent(MetricPerfCacheReferences, uint64(unix.PERF_COUNT_HW_CACHE_REFERENCES)),
	hardwareEvent(MetricPerfCacheMisses, uint64(unix.PERF_COUNT_HW_CACHE_MISSES)),
	hardwareEvent(MetricPerfBusCycles, uint64(unix.PERF_COUNT_HW_BUS_CYCLES)),
	hardwareEvent(MetricPerfStalledFrontend, uint64(unix.PERF_COUNT_HW_STALLED_CYCLES_FRONTEND)),
	hardwareEvent(MetricPerfStalledBackend, uint64(unix.PERF_COUNT_HW_STALLED_CYCLES_BACKEND)),
	cacheEvent(MetricPerfL1DReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_L1D), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfL1DReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_L1D), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
	cacheEvent(MetricPerfL1IReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_L1I), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfL1IReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_L1I), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
	cacheEvent(MetricPerfL2ReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_L2), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfL2ReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_L2), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
	cacheEvent(MetricPerfLLCReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_LL), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfLLCReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_LL), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
	cacheEvent(MetricPerfDTLBReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_DTLB), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfDTLBReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_DTLB), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
	cacheEvent(MetricPerfITLBReadAccess, uint64(unix.PERF_COUNT_HW_CACHE_ITLB), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_ACCESS)),
	cacheEvent(MetricPerfITLBReadMiss, uint64(unix.PERF_COUNT_HW_CACHE_ITLB), uint64(unix.PERF_COUNT_HW_CACHE_OP_READ), uint64(unix.PERF_COUNT_HW_CACHE_RESULT_MISS)),
}

type perfReader struct {
	cpus []int
	events []perfEventSpec
	metricIDs []string
	derivedIDs []string
	fds map[int]map[string]int
	openErrors map[string]error
	last map[string]uint64
	lastStatus map[string]string
	lastAt time.Time
}

func (c *SystemCollector) ReadPerfMetrics(ctx context.Context, targets ...string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	metricIDs := selectPerfMetricIDs(targets)
	eventIDs := selectPerfEventIDs(targets)
	selectedEvents := make([]perfEventSpec, 0, len(eventIDs))
	eventByID := make(map[string]perfEventSpec, len(genericPerfEvents))
	for _, event := range genericPerfEvents {
		eventByID[event.metricID] = event
	}
	for _, id := range eventIDs {
		if event, ok := eventByID[id]; ok {
			selectedEvents = append(selectedEvents, event)
		}
	}
	derivedIDs := make([]string, 0)
	for _, id := range metricIDs {
		if numerator, denominator := isPerfDerivedMetric(id); numerator != "" && denominator != "" {
			derivedIDs = append(derivedIDs, id)
		}
	}

	c.perfMu.Lock()
	defer c.perfMu.Unlock()

	cpus, err := onlineCPUList()
	if err != nil {
		if c.perf != nil {
			c.perf.close()
		}
		c.perf = newPerfReader(nil, err, selectedEvents, metricIDs, derivedIDs)
		return c.perf.read(ctx)
	}
	if c.perf == nil || !sameCPUList(c.perf.cpus, cpus) ||
		!samePerfEventList(c.perf.events, selectedEvents) ||
		!sameStringList(c.perf.metricIDs, metricIDs) {
		if c.perf != nil {
			c.perf.close()
		}
		c.perf = newPerfReader(cpus, nil, selectedEvents, metricIDs, derivedIDs)
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

func samePerfEventList(a, b []perfEventSpec) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].metricID != b[i].metricID || a[i].eventType != b[i].eventType || a[i].config != b[i].config {
			return false
		}
	}
	return true
}

func sameStringList(a, b []string) bool {
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

func newPerfReader(cpus []int, cpuListErr error, events []perfEventSpec, metricIDs, derivedIDs []string) *perfReader {
	reader := &perfReader{
		cpus: append([]int(nil), cpus...),
		events: append([]perfEventSpec(nil), events...),
		metricIDs: append([]string(nil), metricIDs...),
		derivedIDs: append([]string(nil), derivedIDs...),
		fds: make(map[int]map[string]int),
		openErrors: make(map[string]error),
		last: make(map[string]uint64),
		lastStatus: make(map[string]string),
	}
	if cpuListErr != nil {
		for _, event := range events {
			reader.openErrors[event.metricID] = cpuListErr
		}
		return reader
	}
	for _, cpuID := range cpus {
		reader.fds[cpuID] = make(map[string]int)
		for _, event := range events {
			attr := unix.PerfEventAttr{
				Type: event.eventType,
				Size: uint32(unsafe.Sizeof(unix.PerfEventAttr{})),
				Config: event.config,
				Read_format: unix.PERF_FORMAT_TOTAL_TIME_ENABLED | unix.PERF_FORMAT_TOTAL_TIME_RUNNING,
				Bits: unix.PerfBitDisabled,
			}
			fd, err := unix.PerfEventOpen(&attr, -1, cpuID, -1, perfFlagFDCloseOnExec)
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
	r.lastStatus = nil
}

func (r *perfReader) read(ctx context.Context) ([]MetricItem, error) {
	now := time.Now().UTC()
	values := make(map[string]uint64, len(r.events))
	statuses := make(map[string]MetricItem, len(r.events))
	for _, event := range r.events {
		var total uint64
		successes, failures := 0, 0
		var firstErr error
		for _, cpuFDs := range r.fds {
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
			if ^uint64(0)-total < value {
				firstErr = errors.New("PMU counter aggregate overflow")
				failures++
				continue
			}
			total += value
			successes++
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

	items := make([]MetricItem, 0, len(r.metricIDs))
	for _, metricID := range r.metricIDs {
		if _, derived := isPerfDerivedMetric(metricID); derived == "" {
			if item, ok := statuses[metricID]; ok {
				items = append(items, item)
			}
		}
	}
	windowMS := int64(0)
	if !r.lastAt.IsZero() {
		windowMS = now.Sub(r.lastAt).Milliseconds()
	}
	for _, metricID := range r.derivedIDs {
		numeratorID, denominatorID := isPerfDerivedMetric(metricID)
		numerator, numeratorOK := values[numeratorID]
		denominator, denominatorOK := values[denominatorID]
		currentNumeratorStatus := statuses[numeratorID].Status
		currentDenominatorStatus := statuses[denominatorID].Status
		previousNumerator, previousNumeratorOK := r.last[numeratorID]
		previousDenominator, previousDenominatorOK := r.last[denominatorID]
		if !numeratorOK || !denominatorOK {
			items = append(items, statusItem(metricID, "", StatusUnavailable, "source_unavailable", "PARTIAL_RESULT", "linux.perf_event_open", now))
			continue
		}
		if currentNumeratorStatus != StatusValid || currentDenominatorStatus != StatusValid ||
			r.lastStatus[numeratorID] != StatusValid || r.lastStatus[denominatorID] != StatusValid {
			items = append(items, statusItem(metricID, "", StatusPartial, "source_counter_partial", "PARTIAL_RESULT", "linux.perf_event_open", now))
			continue
		}
		if !previousNumeratorOK || !previousDenominatorOK {
			items = append(items, statusItem(metricID, "", StatusUnknown, "no_baseline", "", "linux.perf_event_open", now))
			continue
		}
		if numerator < previousNumerator || denominator < previousDenominator {
			items = append(items, statusItem(metricID, "", StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "linux.perf_event_open", now))
			continue
		}
		deltaNumerator := numerator - previousNumerator
		deltaDenominator := denominator - previousDenominator
		if deltaDenominator == 0 {
			items = append(items, statusItem(metricID, "", StatusUnavailable, "zero_denominator", "SOURCE_INVALID_DATA", "linux.perf_event_open", now))
			continue
		}
		value := float64(deltaNumerator) / float64(deltaDenominator)
		unit := "ratio"
		if metricID != MetricPerfIPC {
			value *= 100
			unit = "percent"
		}
		if math.IsNaN(value) || math.IsInf(value, 0) {
			items = append(items, statusItem(metricID, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.perf_event_open", now))
			continue
		}
		items = append(items, MetricItem{
			MetricID: metricID, Value: value, Unit: unit,
			ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
			SourceID: "linux.perf_event_open", WindowMS: windowMS,
		})
	}
	r.last = values
	r.lastStatus = make(map[string]string, len(statuses))
	for metricID, item := range statuses {
		r.lastStatus[metricID] = item.Status
	}
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
	if math.IsNaN(scaled) || math.IsInf(scaled, 0) || scaled < 0 || scaled > float64(^uint64(0)) {
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
