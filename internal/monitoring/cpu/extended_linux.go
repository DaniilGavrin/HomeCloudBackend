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

// readLinuxCStates reads cpuidle residency counters. Linux exports time in
// microseconds; HomeCloud's canonical duration unit is nanoseconds.
func readLinuxCStates(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	now := time.Now().UTC()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return []MetricItem{statusItem(MetricCStateTarget, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpuidle", now)}, nil
	}
	availability, availabilityKnown := readLinuxCoreAvailability(cpuSysfsPath)
	pattern := filepath.Join(cpuSysfsPath, "cpu[0-9]*", "cpuidle", "state[0-9]*")
	states, err := filepath.Glob(pattern)
	if err != nil {
		status, reason, code := classifySourceError(err)
		return []MetricItem{statusItem(MetricCStateTarget, "", status, reason, code, "linux.host.sysfs.cpuidle", now)}, nil
	}
	sort.Strings(states)
	if len(states) == 0 {
		items := []MetricItem{statusItem(MetricCStateTarget, "", StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.cpuidle", now)}
		if availabilityKnown {
			for _, cpuName := range sortedCoreAvailabilityIDs(availability) {
				state := availability[cpuName]
				status, reason, code, sourceID := StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.cpuidle"
				if !state.Online {
					status, reason, code, sourceID = StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu"
				}
				items = append(items, statusItem(MetricCStateTarget, cpuName, status, reason, code, sourceID, now))
			}
		}
		return items, nil
	}
	items := make([]MetricItem, 0, len(states))
	seenCPUs := make(map[string]bool)
	for _, statePath := range states {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cpuDir := filepath.Dir(filepath.Dir(statePath))
		cpuID := filepath.Base(cpuDir)
		stateID := filepath.Base(statePath)
		objectID := cpuID + "/" + stateID
		seenCPUs[cpuID] = true
		name, nameErr := readSysfsValue(filepath.Join(statePath, "name"))
		if nameErr != nil || strings.TrimSpace(name) == "" {
			if availabilityKnown {
				if state, exists := availability[cpuID]; !exists || !state.Present {
					items = append(items, statusItem(MetricCStateTarget, objectID, StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu", now))
					continue
				} else if !state.Online {
					items = append(items, statusItem(MetricCStateTarget, objectID, StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu", now))
					continue
				}
			}
			if nameErr == nil {
				nameErr = errors.New("cpuidle state name is empty")
			}
			status, reason, code := classifySourceError(nameErr)
			items = append(items, statusItem(MetricCStateTarget, objectID, status, reason, code, "linux.host.sysfs.cpuidle", now))
			continue
		}
		metricID := MetricCStateTarget + "." + metricSlug(name)
		if metricID == MetricCStateTarget+"." {
			metricID = MetricCStateTarget + "." + stateID
		}
		if availabilityKnown {
			state, exists := availability[cpuID]
			if !exists || !state.Present {
				items = append(items, statusItem(metricID, objectID, StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu", now))
				continue
			}
			if !state.Online {
				items = append(items, statusItem(metricID, objectID, StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu", now))
				continue
			}
		}
		timeText, readErr := readSysfsValue(filepath.Join(statePath, "time"))
		if readErr != nil {
			status, reason, code := classifySourceError(readErr)
			if errors.Is(readErr, os.ErrNotExist) {
				status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
			}
			items = append(items, statusItem(metricID, objectID, status, reason, code, "linux.host.sysfs.cpuidle", now))
			continue
		}
		microseconds, parseErr := strconv.ParseUint(timeText, 10, 64)
		if parseErr != nil || microseconds > math.MaxInt64/uint64(time.Microsecond) {
			items = append(items, statusItem(metricID, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.host.sysfs.cpuidle", now))
			continue
		}
		items = append(items, MetricItem{
			MetricID: metricID, ObjectID: objectID,
			Value: int64(microseconds * uint64(time.Microsecond)),
			Unit: "ns", ObservedAt: now.Format(time.RFC3339Nano),
			Status: StatusValid, SourceID: "linux.host.sysfs.cpuidle",
		})
	}
	if presentText, presentErr := readSysfsValue(filepath.Join(cpuSysfsPath, "present")); presentErr == nil {
		if cpuIDs, parseErr := parseCPUList(presentText); parseErr == nil {
			for _, cpuID := range cpuIDs {
				cpuName := fmt.Sprintf("cpu%d", cpuID)
				if !seenCPUs[cpuName] {
					status, reason, code, sourceID := StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.cpuidle"
					if availabilityKnown {
						if state, exists := availability[cpuName]; !exists || !state.Present {
							status, reason, code, sourceID = StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu"
						} else if !state.Online {
							status, reason, code, sourceID = StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu"
						}
					}
					items = append(items, statusItem(MetricCStateTarget, cpuName, status, reason, code, sourceID, now))
				}
			}
		}
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
// exports them. Core-level counters remain scoped to each logical CPU; package
// counters are emitted once per physical package because they are broadcast.
func readLinuxThrottling(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	now := time.Now().UTC()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return []MetricItem{statusItem(MetricThrottlingTarget, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.throttling", now)}, nil
	}
	presentText, err := readSysfsValue(filepath.Join(cpuSysfsPath, "present"))
	if err != nil {
		status, reason, code := classifySourceError(err)
		return []MetricItem{statusItem(MetricThrottlingTarget, "", status, reason, code, "linux.host.sysfs.thermal_throttle", now)}, nil
	}
	cpuIDs, err := parseCPUList(presentText)
	if err != nil {
		return []MetricItem{statusItem(MetricThrottlingTarget, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.host.sysfs.thermal_throttle", now)}, nil
	}
	if len(cpuIDs) == 0 {
		return []MetricItem{statusItem(MetricThrottlingTarget, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "linux.host.sysfs.thermal_throttle", now)}, nil
	}
	sort.Ints(cpuIDs)

	packagePaths := make(map[string]string)
	packageIDs := make(map[string]bool)
	items := make([]MetricItem, 0, len(cpuIDs)*5+len(cpuIDs)*2+8)
	for _, id := range cpuIDs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cpuName := fmt.Sprintf("cpu%d", id)
		cpuPath := filepath.Join(cpuSysfsPath, cpuName)
		topologyPath := filepath.Join(cpuPath, "topology")
		packageID, packageErr := readSysfsValue(filepath.Join(topologyPath, "physical_package_id"))
		packageNumber, packageParseErr := strconv.Atoi(packageID)
		packageKnown := packageErr == nil && packageParseErr == nil && packageNumber >= 0
		if packageKnown {
			packageID = strconv.Itoa(packageNumber)
			packageIDs[packageID] = true
		}

		throttlePath := filepath.Join(cpuPath, "thermal_throttle")
		if _, statErr := os.Stat(throttlePath); statErr != nil {
			status, reason, code := classifySourceError(statErr)
			if errors.Is(statErr, os.ErrNotExist) {
				status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
			}
			items = append(items,
				statusItem(MetricThrottleThermalCoreEvents, cpuName, status, reason, code, "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottleThermalCoreDuration, cpuName, status, reason, code, "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottlePowerCoreEvents, cpuName, status, reason, code, "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottlePowerCoreDuration, cpuName, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottleActive, cpuName, StatusUnsupported, "active_state_not_exposed", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
			)
			continue
		}
		items = append(items,
			appendThrottleCounter(throttlePath, "core_throttle_count", MetricThrottleThermalCoreEvents, cpuName, "count", 1, now),
			appendThrottleCounter(throttlePath, "core_throttle_total_time_ms", MetricThrottleThermalCoreDuration, cpuName, "ns", uint64(time.Millisecond), now),
			appendThrottleCounter(throttlePath, "core_power_limit_count", MetricThrottlePowerCoreEvents, cpuName, "count", 1, now),
			statusItem(MetricThrottlePowerCoreDuration, cpuName, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
			statusItem(MetricThrottleActive, cpuName, StatusUnsupported, "active_state_not_exposed", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
		)
		if packageKnown {
			if _, exists := packagePaths[packageID]; !exists {
				packagePaths[packageID] = throttlePath
			}
		}
	}

	packageKeys := make([]string, 0, len(packageIDs))
	for packageID := range packageIDs {
		packageKeys = append(packageKeys, packageID)
	}
	sort.Strings(packageKeys)
	for _, packageID := range packageKeys {
		objectID := "package" + packageID
		throttlePath, ok := packagePaths[packageID]
		if !ok {
			items = append(items,
				statusItem(MetricThrottleThermalPackageEvents, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottleThermalPackageDuration, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottlePowerPackageEvents, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
				statusItem(MetricThrottlePowerPackageDuration, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
			)
			continue
		}
		items = append(items,
			appendThrottleCounter(throttlePath, "package_throttle_count", MetricThrottleThermalPackageEvents, objectID, "count", 1, now),
			appendThrottleCounter(throttlePath, "package_throttle_total_time_ms", MetricThrottleThermalPackageDuration, objectID, "ns", uint64(time.Millisecond), now),
			appendThrottleCounter(throttlePath, "package_power_limit_count", MetricThrottlePowerPackageEvents, objectID, "count", 1, now),
			statusItem(MetricThrottlePowerPackageDuration, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now),
		)
	}
	if len(packageKeys) == 0 {
		for _, metricID := range []string{MetricThrottleThermalPackageEvents, MetricThrottleThermalPackageDuration, MetricThrottlePowerPackageEvents, MetricThrottlePowerPackageDuration} {
			items = append(items, statusItem(metricID, "", StatusUnsupported, "package_topology_unavailable", "METRIC_UNSUPPORTED", "linux.host.sysfs.thermal_throttle", now))
		}
	}
	return items, nil
}

func appendThrottleCounter(dir, filename, metricID, objectID, unit string, scale uint64, observedAt time.Time) MetricItem {
	sourceID := "linux.host.sysfs.thermal_throttle"
	valueText, err := readSysfsValue(filepath.Join(dir, filename))
	if err != nil {
		status, reason, code := classifySourceError(err)
		if errors.Is(err, os.ErrNotExist) {
			status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
		}
		return statusItem(metricID, objectID, status, reason, code, sourceID, observedAt)
	}
	value, err := strconv.ParseUint(valueText, 10, 64)
	if err != nil {
		return statusItem(metricID, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", sourceID, observedAt)
	}
	var normalized any = value
	if unit != "count" {
		if scale == 0 || value > math.MaxInt64/scale {
			return statusItem(metricID, objectID, StatusInvalid, "overflow", "SOURCE_INVALID_DATA", sourceID, observedAt)
		}
		normalized = int64(value * scale)
	}
	return MetricItem{
		MetricID: metricID, ObjectID: objectID, Value: normalized, Unit: unit,
		ObservedAt: observedAt.Format(time.RFC3339Nano), Status: StatusValid, SourceID: sourceID,
	}
}

func hasThrottleObject(items []MetricItem, metricID, objectID string) bool {
	for _, item := range items {
		if item.MetricID == metricID && item.ObjectID == objectID {
			return true
		}
	}
	return false
}

// readLinuxSystemCounters reads host-wide cumulative counters and process
// state gauges from /proc/stat. procs_running includes tasks currently running
// or runnable; it is not the waiting-only system.cpu.runqueue metric.
func readLinuxSystemCounters(ctx context.Context, procStatPath string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		now := time.Now().UTC()
		items := make([]MetricItem, 0, 5)
		for _, metricID := range []string{MetricInterrupts, MetricContextSwitches, MetricForks, MetricProcessesRunning, MetricProcessesBlocked} {
			items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.proc.stat", now))
		}
		return items, nil
	}
	now := time.Now().UTC()
	file, err := os.Open(procStatPath)
	if err != nil {
		status, reason, code := classifySourceError(err)
		items := make([]MetricItem, 0, 5)
		for _, metricID := range []string{MetricInterrupts, MetricContextSwitches, MetricForks, MetricProcessesRunning, MetricProcessesBlocked} {
			items = append(items, statusItem(metricID, "", status, reason, code, "linux.host.proc.stat", now))
		}
		return items, nil
	}
	defer file.Close()

	type counterSpec struct {
		key string
		metricID string
	}
	specs := []counterSpec{
		{key: "intr", metricID: MetricInterrupts},
		{key: "ctxt", metricID: MetricContextSwitches},
		{key: "processes", metricID: MetricForks},
		{key: "procs_running", metricID: MetricProcessesRunning},
		{key: "procs_blocked", metricID: MetricProcessesBlocked},
	}
	values := make(map[string]uint64, 5)
	invalid := make(map[string]bool, 5)
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
		case "intr", "ctxt", "processes", "procs_running", "procs_blocked":
			value, parseErr := strconv.ParseUint(fields[1], 10, 64)
			if parseErr != nil {
				invalid[fields[0]] = true
				continue
			}
			values[fields[0]] = value
		}
		if len(values)+len(invalid) >= 5 {
			break
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		status, reason, code := classifySourceError(scanErr)
		items := make([]MetricItem, 0, len(specs))
		for _, spec := range specs {
			items = append(items, statusItem(spec.metricID, "", status, reason, code, "linux.host.proc.stat", now))
		}
		return items, nil
	}

	items := make([]MetricItem, 0, len(specs))
	for _, spec := range specs {
		if invalid[spec.key] {
			items = append(items, statusItem(spec.metricID, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.host.proc.stat", now))
			continue
		}
		value, ok := values[spec.key]
		if !ok {
			items = append(items, statusItem(spec.metricID, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "linux.host.proc.stat", now))
			continue
		}
		items = append(items, MetricItem{
			MetricID: spec.metricID, Value: value, Unit: "count",
			ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
			SourceID: "linux.host.proc.stat",
		})
	}
	return items, nil
}
