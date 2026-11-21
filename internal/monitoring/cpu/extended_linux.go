package cpu

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	defaultProcStatPath = "/proc/stat"
)

func (c *SystemCollector) ReadCStates(ctx context.Context) ([]MetricItem, error) {
	return readLinuxCStates(ctx, defaultSysfsCPUPath)
}

func (c *SystemCollector) ReadThrottling(ctx context.Context) ([]MetricItem, error) {
	return readLinuxThrottling(ctx, defaultSysfsCPUPath)
}

func (c *SystemCollector) ReadSystemCounters(ctx context.Context) ([]MetricItem, error) {
	return readLinuxSystemCounters(ctx, defaultProcStatPath)
}

// readLinuxCStates reads cpuidle residency counters. Linux exports time in
// microseconds; HomeCloud's canonical duration unit is nanoseconds.
func readLinuxCStates(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return nil, ErrMetricUnsupported
	}
	pattern := filepath.Join(cpuSysfsPath, "cpu[0-9]*", "cpuidle", "state[0-9]*")
	states, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("list CPU idle states: %w", err)
	}
	sort.Strings(states)
	items := make([]MetricItem, 0, len(states))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for _, statePath := range states {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, err := readSysfsValue(filepath.Join(statePath, "name"))
		if err != nil || strings.TrimSpace(name) == "" {
			continue
		}
		timeText, err := readSysfsValue(filepath.Join(statePath, "time"))
		if err != nil {
			continue
		}
		microseconds, err := strconv.ParseUint(timeText, 10, 64)
		if err != nil || microseconds > math.MaxInt64/uint64(time.Microsecond) {
			continue
		}
		cpuDir := filepath.Dir(filepath.Dir(statePath))
		cpuID := filepath.Base(cpuDir)
		stateID := filepath.Base(statePath)
		suffix := metricSlug(name)
		if suffix == "" {
			suffix = stateID
		}
		items = append(items, MetricItem{
			MetricID: MetricCStateTarget + "." + suffix,
			ObjectID: cpuID + "/" + stateID,
			Value: int64(microseconds * uint64(time.Microsecond)),
			Unit: "ns",
			ObservedAt: now,
		})
	}
	if len(items) == 0 {
		return nil, ErrMetricUnsupported
	}
	return items, nil
}

func metricSlug(value string) string {
	var b strings.Builder
	lastUnderscore := false
	for _, r := range strings.ToLower(strings.TrimSpace(value)) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
			lastUnderscore = false
		default:
			if !lastUnderscore && b.Len() > 0 {
				b.WriteByte('_')
				lastUnderscore = true
			}
		}
	}
	return strings.Trim(b.String(), "_")
}

// readLinuxThrottling exposes Intel thermal_throttle counters where the kernel
// exports them. Per-core counters are deduplicated across SMT siblings; package
// counters are emitted once per physical package.
func readLinuxThrottling(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return nil, ErrMetricUnsupported
	}
	presentText, err := readSysfsValue(filepath.Join(cpuSysfsPath, "present"))
	if err != nil {
		return nil, fmt.Errorf("read present CPU list for throttling: %w", err)
	}
	cpuIDs, err := parseCPUList(presentText)
	if err != nil {
		return nil, fmt.Errorf("parse present CPU list for throttling: %w", err)
	}
	sort.Ints(cpuIDs)

	seenCoreEvents := make(map[string]bool)
	seenCoreDuration := make(map[string]bool)
	seenPackageEvents := make(map[string]bool)
	seenPackageDuration := make(map[string]bool)
	items := make([]MetricItem, 0, len(cpuIDs)*4)
	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, id := range cpuIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cpuName := fmt.Sprintf("cpu%d", id)
		cpuPath := filepath.Join(cpuSysfsPath, cpuName)
		topologyPath := filepath.Join(cpuPath, "topology")
		packageID, packageErr := readSysfsValue(filepath.Join(topologyPath, "physical_package_id"))
		coreID, coreErr := readSysfsValue(filepath.Join(topologyPath, "core_id"))
		throttlePath := filepath.Join(cpuPath, "thermal_throttle")

		// Never label a logical-CPU counter as core/package data when sysfs
		// does not expose the topology needed to establish that scope.
		if packageErr == nil && packageID != "" && coreErr == nil && coreID != "" {
			coreKey := packageID + ":" + coreID
			coreObjectID := "package" + packageID + "/core" + coreID
			if !seenCoreEvents[coreKey] {
				appendThrottleCounter(&items, throttlePath, "core_throttle_count",
					MetricThrottleThermalCoreEvents, coreObjectID, "count", 1, now)
				seenCoreEvents[coreKey] = hasThrottleObject(items, MetricThrottleThermalCoreEvents, coreObjectID)
			}
			if !seenCoreDuration[coreKey] {
				appendThrottleCounter(&items, throttlePath, "core_throttle_total_time_ms",
					MetricThrottleThermalCoreDuration, coreObjectID, "ns", uint64(time.Millisecond), now)
				seenCoreDuration[coreKey] = hasThrottleObject(items, MetricThrottleThermalCoreDuration, coreObjectID)
			}
		}

		if packageErr == nil && packageID != "" {
			packageObjectID := "package" + packageID
			if !seenPackageEvents[packageID] {
				appendThrottleCounter(&items, throttlePath, "package_throttle_count",
					MetricThrottleThermalPackageEvents, packageObjectID, "count", 1, now)
				seenPackageEvents[packageID] = hasThrottleObject(items, MetricThrottleThermalPackageEvents, packageObjectID)
			}
			if !seenPackageDuration[packageID] {
				appendThrottleCounter(&items, throttlePath, "package_throttle_total_time_ms",
					MetricThrottleThermalPackageDuration, packageObjectID, "ns", uint64(time.Millisecond), now)
				seenPackageDuration[packageID] = hasThrottleObject(items, MetricThrottleThermalPackageDuration, packageObjectID)
			}
		}
	}
	if len(items) == 0 {
		return nil, ErrMetricUnsupported
	}
	return items, nil
}

func appendThrottleCounter(items *[]MetricItem, dir, filename, metricID, objectID, unit string, scale uint64, observedAt string) {
	valueText, err := readSysfsValue(filepath.Join(dir, filename))
	if err != nil {
		return
	}
	value, err := strconv.ParseUint(valueText, 10, 64)
	if err != nil || value > math.MaxInt64/scale {
		return
	}
	*items = append(*items, MetricItem{
		MetricID: metricID, ObjectID: objectID,
		Value: int64(value * scale), Unit: unit, ObservedAt: observedAt,
	})
}

func hasThrottleObject(items []MetricItem, metricID, objectID string) bool {
	for _, item := range items {
		if item.MetricID == metricID && item.ObjectID == objectID {
			return true
		}
	}
	return false
}

// readLinuxSystemCounters reads the host-wide cumulative interrupt and context
// switch counters from /proc/stat. Rates are intentionally not calculated here.
func readLinuxSystemCounters(ctx context.Context, procStatPath string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return nil, ErrMetricUnsupported
	}
	file, err := os.Open(procStatPath)
	if err != nil {
		return nil, fmt.Errorf("open proc stat: %w", err)
	}
	defer file.Close()

	values := make(map[string]uint64, 2)
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) < 2 {
			continue
		}
		switch fields[0] {
		case "intr", "ctxt":
			value, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil {
				return nil, fmt.Errorf("parse /proc/stat %s counter: %w", fields[0], parseErr)
			}
			values[fields[0]] = value
		}
		if len(values) == 2 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read proc stat: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, 2)
	if value, ok := values["intr"]; ok {
		items = append(items, MetricItem{MetricID: MetricInterrupts, Value: value, Unit: "count", ObservedAt: now})
	}
	if value, ok := values["ctxt"]; ok {
		items = append(items, MetricItem{MetricID: MetricContextSwitches, Value: value, Unit: "count", ObservedAt: now})
	}
	if len(items) == 0 {
		return nil, errors.Join(ErrMetricUnsupported, errors.New("/proc/stat does not expose intr or ctxt"))
	}
	return items, nil
}
