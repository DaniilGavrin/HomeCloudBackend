package cpu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"

	"HomeCloudBackend/internal/commands"

	gopsutilcpu "github.com/shirou/gopsutil/v4/cpu"
)

const (
	MetricFrequencyCurrent  = "system.cpu.frequency.current"
	MetricFrequencyMin      = "system.cpu.frequency.min"
	MetricFrequencyMax      = "system.cpu.frequency.max"
	MetricFrequencyBase     = "system.cpu.frequency.base"
	MetricFrequencyGovernor = "system.cpu.frequency.governor"
	MetricFrequencyTarget   = "system.cpu.frequency"
	MetricCoreFrequency     = "system.cpu.core.frequency"
	MetricTemperature       = "system.cpu.temperature"

	MetricTopologyCommand       = "system.cpu.topology"
	MetricTopologyLogicalCount  = "system.cpu.topology.logical_count"
	MetricTopologyOnlineCount   = "system.cpu.topology.online_count"
	MetricTopologyOfflineCount  = "system.cpu.topology.offline_count"
	MetricTopologyCoreCount     = "system.cpu.topology.physical_core_count"
	MetricTopologyPackageCount  = "system.cpu.topology.package_count"
	MetricTopologyDieCount      = "system.cpu.topology.die_count"
	MetricTopologyNUMACount     = "system.cpu.topology.numa_node_count"
	MetricTopologyArchitecture  = "system.cpu.topology.architecture"
	MetricTopologyVendor        = "system.cpu.topology.vendor"
	MetricTopologyModel         = "system.cpu.topology.model"
	MetricTopologyFamily        = "system.cpu.topology.family"
	MetricTopologyModelID       = "system.cpu.topology.model_id"
	MetricTopologyStepping      = "system.cpu.topology.stepping"
	MetricTopologyFeatures      = "system.cpu.topology.features"
	MetricTopologyCoreID        = "system.cpu.topology.core_id"
	MetricTopologyPackageID     = "system.cpu.topology.package_id"
	MetricTopologyDieID         = "system.cpu.topology.die_id"
	MetricTopologyNUMAID        = "system.cpu.topology.numa_node_id"
	MetricTopologyOnline        = "system.cpu.topology.online"
)

const defaultSysfsCPUPath = "/sys/devices/system/cpu"

const (
	MetricCStateTarget                  = "system.cpu.idle.cstate"
	MetricThrottlingTarget              = "system.cpu.throttling"
	MetricThrottleThermalCoreEvents     = "system.cpu.throttling.thermal.core.events"
	MetricThrottleThermalCoreDuration   = "system.cpu.throttling.thermal.core.duration"
	MetricThrottleThermalPackageEvents  = "system.cpu.throttling.thermal.package.events"
	MetricThrottleThermalPackageDuration = "system.cpu.throttling.thermal.package.duration"
	MetricThrottlePowerCoreEvents       = "system.cpu.throttling.power.core.events"
	MetricThrottlePowerPackageEvents    = "system.cpu.throttling.power.package.events"
	MetricThrottlePowerCoreDuration    = "system.cpu.throttling.power.core.duration"
	MetricThrottlePowerPackageDuration = "system.cpu.throttling.power.package.duration"
	MetricThrottleActive               = "system.cpu.throttling.active"
	MetricInterrupts                    = "system.cpu.interrupts"
	MetricContextSwitches                = "system.cpu.context_switches"
	MetricForks                         = "system.cpu.forks"
	MetricProcessesRunning              = "system.cpu.processes.running"
	MetricProcessesBlocked              = "system.cpu.processes.blocked"
	MetricProcessesTarget               = "system.cpu.processes"
)

var ErrMetricUnsupported = commands.NewClassifiedError(
	"METRIC_UNSUPPORTED", "source",
	"CPU metric is not supported by this platform or hardware", false, nil,
)

// readLinuxFrequency reads cpufreq policy values. Linux sysfs reports
// frequencies in kHz; Monitoring's canonical unit is Hz.
func readLinuxFrequency(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	now := time.Now().UTC()
	metricIDs := []string{MetricFrequencyTarget, MetricFrequencyCurrent, MetricFrequencyMin, MetricFrequencyMax, MetricFrequencyBase, MetricFrequencyGovernor, MetricCoreFrequency}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		items := make([]MetricItem, 0, len(metricIDs))
		for _, id := range metricIDs {
			items = append(items, statusItem(id, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.frequency", now))
		}
		return items, nil
	}
	policies, err := filepath.Glob(filepath.Join(cpuSysfsPath, "cpufreq", "policy[0-9]*"))
	if err != nil {
		status, reason, code := classifySourceError(err)
		items := make([]MetricItem, 0, len(metricIDs))
		for _, id := range metricIDs {
			items = append(items, statusItem(id, "", status, reason, code, "linux.sysfs.cpufreq", now))
		}
		return items, nil
	}
	if len(policies) == 0 {
		items := make([]MetricItem, 0, len(metricIDs))
		for _, id := range metricIDs {
			items = append(items, statusItem(id, "", StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.sysfs.cpufreq", now))
		}
		return items, nil
	}

	items := make([]MetricItem, 0, len(policies)*len(metricIDs))
	for _, policyPath := range policies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		objectID := filepath.Base(policyPath)
		appendFrequency := func(metricID, filename, unit string, fallback string) {
			path := filepath.Join(policyPath, filename)
			value, readErr := readSysfsValue(path)
			if readErr != nil && fallback != "" && errors.Is(readErr, os.ErrNotExist) {
				value, readErr = readSysfsValue(filepath.Join(policyPath, fallback))
			}
			if readErr != nil {
				status, reason, code := classifySourceError(readErr)
				if errors.Is(readErr, os.ErrNotExist) {
					status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
				}
				items = append(items, statusItem(metricID, objectID, status, reason, code, "linux.sysfs.cpufreq", now))
				return
			}
			var parsed any = value
			if unit == "Hz" {
				number, parseErr := strconv.ParseUint(value, 10, 64)
				if parseErr != nil || number == 0 || number > uint64(^uint64(0)>>1)/1000 {
					items = append(items, statusItem(metricID, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.sysfs.cpufreq", now))
					return
				}
				parsed = int64(number * 1000)
			}
			items = append(items, MetricItem{
				MetricID: metricID, ObjectID: objectID, Value: parsed, Unit: unit,
				ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
				SourceID: "linux.sysfs.cpufreq",
			})
		}
		appendFrequency(MetricFrequencyTarget, "scaling_cur_freq", "Hz", "cpuinfo_cur_freq")
		appendFrequency(MetricFrequencyCurrent, "scaling_cur_freq", "Hz", "cpuinfo_cur_freq")
		appendFrequency(MetricFrequencyMin, "cpuinfo_min_freq", "Hz", "scaling_min_freq")
		appendFrequency(MetricFrequencyMax, "cpuinfo_max_freq", "Hz", "scaling_max_freq")
		appendFrequency(MetricFrequencyBase, "base_frequency", "Hz", "cpuinfo_base_freq")
		appendFrequency(MetricFrequencyGovernor, "scaling_governor", "string", "")

		// cpufreq policies may cover one or several logical CPUs. Expose the
		// current policy value against each affected logical CPU when the
		// kernel provides an explicit mapping.
		cpuIDs, listErr := readPolicyCPUList(policyPath)
		if listErr != nil {
			status, reason, code := classifySourceError(listErr)
			if errors.Is(listErr, os.ErrNotExist) {
				status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
			}
			items = append(items, statusItem(MetricCoreFrequency, objectID, status, reason, code, "linux.sysfs.cpufreq", now))
		} else {
			currentText, currentErr := readSysfsValue(filepath.Join(policyPath, "scaling_cur_freq"))
			var currentHz uint64
			if currentErr == nil {
				var parsed uint64
				parsed, currentErr = strconv.ParseUint(currentText, 10, 64)
				if currentErr == nil && parsed > 0 && parsed <= uint64(^uint64(0)>>1)/1000 {
					currentHz = parsed * 1000
				} else if currentErr == nil {
					currentErr = errors.New("current CPU frequency is outside the supported range")
				}
			}
			for _, cpuID := range cpuIDs {
				coreObjectID := fmt.Sprintf("cpu%d", cpuID)
				if currentErr != nil {
					status, reason, code := classifySourceError(currentErr)
					if errors.Is(currentErr, os.ErrNotExist) {
						status, reason, code = StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED"
					}
					items = append(items, statusItem(MetricCoreFrequency, coreObjectID, status, reason, code, "linux.sysfs.cpufreq", now))
					continue
				}
				items = append(items, MetricItem{
					MetricID: MetricCoreFrequency, ObjectID: coreObjectID, Value: int64(currentHz), Unit: "Hz",
					ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpufreq",
				})
			}
		}
	}
	return items, nil
}

func findMetric(items []MetricItem, metricID, objectID string) (MetricItem, bool) {
	for _, item := range items {
		if item.MetricID == metricID && item.ObjectID == objectID {
			return item, true
		}
	}
	return MetricItem{}, false
}

func readLinuxTopology(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	now := time.Now().UTC()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return []MetricItem{statusItem(MetricTopologyCommand, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "platform.cpu.topology", now)}, nil
	}

	presentText, err := readSysfsValue(filepath.Join(cpuSysfsPath, "present"))
	if err != nil {
		status, reason, code := classifySourceError(err)
		return []MetricItem{statusItem(MetricTopologyCommand, "", status, reason, code, "linux.sysfs.cpu", now)}, nil
	}
	present, err := parseCPUList(presentText)
	if err != nil {
		return []MetricItem{statusItem(MetricTopologyCommand, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.sysfs.cpu", now)}, nil
	}

	onlineText, onlineErr := readSysfsValue(filepath.Join(cpuSysfsPath, "online"))
	var online []int
	onlineStatus, onlineReason, onlineCode := StatusValid, "", ""
	onlineAvailable := onlineErr == nil
	if onlineErr != nil {
		onlineStatus, onlineReason, onlineCode = classifySourceError(onlineErr)
	} else if parsed, parseErr := parseCPUList(onlineText); parseErr != nil {
		onlineAvailable = false
		onlineStatus, onlineReason, onlineCode = StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA"
	} else {
		online = parsed
	}

	onlineSet := make(map[int]bool, len(online))
	for _, id := range online {
		onlineSet[id] = true
	}
	presentSet := make(map[int]bool, len(present))
	for _, id := range present {
		presentSet[id] = true
	}
	if onlineAvailable {
		for _, id := range online {
			if !presentSet[id] {
				onlineAvailable = false
				onlineStatus, onlineReason, onlineCode = StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA"
				break
			}
		}
	}

	items := []MetricItem{
		{MetricID: MetricTopologyLogicalCount, Value: len(present), Unit: "count", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"},
		{MetricID: MetricTopologyArchitecture, Value: runtime.GOARCH, Unit: "string", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "runtime"},
	}
	if onlineAvailable {
		items = append(items,
			MetricItem{MetricID: MetricTopologyOnlineCount, Value: len(online), Unit: "count", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"},
			MetricItem{MetricID: MetricTopologyOfflineCount, Value: len(present)-len(online), Unit: "count", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"},
		)
	} else {
		items = append(items,
			statusItem(MetricTopologyOnlineCount, "", onlineStatus, onlineReason, onlineCode, "linux.sysfs.cpu", now),
			statusItem(MetricTopologyOfflineCount, "", onlineStatus, onlineReason, onlineCode, "linux.sysfs.cpu", now),
		)
	}

	type cpuLocation struct {
		cpuID int
		coreID string
		packageID string
		dieID string
		numaID string
		online bool
	}
	locations := make([]cpuLocation, 0, len(present))
	cores, packages, dies, numaNodes := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, id := range present {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		cpuName := fmt.Sprintf("cpu%d", id)
		topologyPath := filepath.Join(cpuSysfsPath, cpuName, "topology")
		coreID, _ := readSysfsValue(filepath.Join(topologyPath, "core_id"))
		packageID, _ := readSysfsValue(filepath.Join(topologyPath, "physical_package_id"))
		dieID, _ := readSysfsValue(filepath.Join(topologyPath, "die_id"))
		numaID := findNUMANode(filepath.Join(cpuSysfsPath, cpuName))
		location := cpuLocation{cpuID: id, coreID: coreID, packageID: packageID, dieID: dieID, numaID: numaID, online: onlineSet[id]}
		locations = append(locations, location)
		if coreID != "" && packageID != "" {
			cores[packageID+":"+coreID] = true
		}
		if packageID != "" {
			packages[packageID] = true
		}
		if dieID != "" && packageID != "" {
			dies[packageID+":"+dieID] = true
		}
		if numaID != "" {
			numaNodes[numaID] = true
		}
	}
	appendCount := func(metricID string, count int, available bool) {
		if available {
			items = append(items, MetricItem{MetricID: metricID, Value: count, Unit: "count", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"})
		} else {
			items = append(items, statusItem(metricID, "", StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.sysfs.cpu", now))
		}
	}
	appendCount(MetricTopologyCoreCount, len(cores), len(cores) > 0)
	appendCount(MetricTopologyPackageCount, len(packages), len(packages) > 0)
	appendCount(MetricTopologyDieCount, len(dies), len(dies) > 0)
	appendCount(MetricTopologyNUMACount, len(numaNodes), len(numaNodes) > 0)

	for _, location := range locations {
		objectID := fmt.Sprintf("cpu%d", location.cpuID)
		if onlineAvailable {
			items = append(items, MetricItem{MetricID: MetricTopologyOnline, ObjectID: objectID, Value: location.online, Unit: "boolean", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"})
		} else {
			items = append(items, statusItem(MetricTopologyOnline, objectID, onlineStatus, onlineReason, onlineCode, "linux.sysfs.cpu", now))
		}
		appendID := func(metricID, value string) {
			if value == "" {
				items = append(items, statusItem(metricID, objectID, StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.sysfs.cpu", now))
				return
			}
			id, parseErr := strconv.Atoi(value)
			if parseErr != nil || id < 0 {
				items = append(items, statusItem(metricID, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.sysfs.cpu", now))
				return
			}
			items = append(items, MetricItem{MetricID: metricID, ObjectID: objectID, Value: id, Unit: "id", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.cpu"})
		}
		appendID(MetricTopologyCoreID, location.coreID)
		appendID(MetricTopologyPackageID, location.packageID)
		appendID(MetricTopologyDieID, location.dieID)
		appendID(MetricTopologyNUMAID, location.numaID)
	}

	info, infoErr := gopsutilcpu.InfoWithContext(ctx)
	if infoErr != nil {
		status, reason, code := classifySourceError(infoErr)
		if len(locations) == 0 {
			for _, metricID := range []string{MetricTopologyVendor, MetricTopologyModel, MetricTopologyFamily, MetricTopologyModelID, MetricTopologyFeatures, MetricTopologyStepping} {
				items = append(items, statusItem(metricID, "", status, reason, code, "gopsutil.cpu.info", now))
			}
		} else {
			for _, location := range locations {
				objectID := fmt.Sprintf("cpu%d", location.cpuID)
				for _, metricID := range []string{MetricTopologyVendor, MetricTopologyModel, MetricTopologyFamily, MetricTopologyModelID, MetricTopologyFeatures, MetricTopologyStepping} {
					items = append(items, statusItem(metricID, objectID, status, reason, code, "gopsutil.cpu.info", now))
				}
			}
		}
	} else {
		seenInfo := make(map[string]bool, len(info))
		for _, cpuInfo := range info {
			objectID := fmt.Sprintf("cpu%d", cpuInfo.CPU)
			seenInfo[objectID] = true
			appendStringMetric := func(metricID, value string) {
				if strings.TrimSpace(value) == "" {
					items = append(items, statusItem(metricID, objectID, StatusUnsupported, "not_present", "METRIC_UNSUPPORTED", "gopsutil.cpu.info", now))
					return
				}
				items = append(items, MetricItem{MetricID: metricID, ObjectID: objectID, Value: value, Unit: "string", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "gopsutil.cpu.info"})
			}
			appendStringMetric(MetricTopologyVendor, cpuInfo.VendorID)
			appendStringMetric(MetricTopologyModel, cpuInfo.ModelName)
			appendStringMetric(MetricTopologyFamily, cpuInfo.Family)
			appendStringMetric(MetricTopologyModelID, cpuInfo.Model)
			if len(cpuInfo.Flags) > 0 {
				items = append(items, MetricItem{MetricID: MetricTopologyFeatures, ObjectID: objectID, Value: cpuInfo.Flags, Unit: "list", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "gopsutil.cpu.info"})
			} else {
				items = append(items, statusItem(MetricTopologyFeatures, objectID, StatusUnsupported, "not_present", "METRIC_UNSUPPORTED", "gopsutil.cpu.info", now))
			}
			// Stepping zero is a valid hardware value; a successful CPU-info
			// response is sufficient evidence that the field was observed.
			items = append(items, MetricItem{MetricID: MetricTopologyStepping, ObjectID: objectID, Value: cpuInfo.Stepping, Unit: "count", ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "gopsutil.cpu.info"})
		}
		for _, location := range locations {
			objectID := fmt.Sprintf("cpu%d", location.cpuID)
			if seenInfo[objectID] {
				continue
			}
			for _, metricID := range []string{MetricTopologyVendor, MetricTopologyModel, MetricTopologyFamily, MetricTopologyModelID, MetricTopologyFeatures, MetricTopologyStepping} {
				items = append(items, statusItem(metricID, objectID, StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.info", now))
			}
		}
	}
	return items, nil
}

func readSysfsValue(path string) (string, error) {
	value, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(value)), nil
}

func parseCPUList(value string) ([]int, error) {
	tokens := strings.Fields(value)
	if len(tokens) == 0 {
		return nil, errors.New("CPU list is empty")
	}
	result := make([]int, 0)
	seen := make(map[int]bool)
	for _, token := range tokens {
		for _, part := range strings.Split(token, ",") {
			if part == "" {
				return nil, fmt.Errorf("invalid CPU list segment %q", part)
			}
			bounds := strings.Split(part, "-")
			if len(bounds) > 2 {
				return nil, fmt.Errorf("invalid CPU list segment %q", part)
			}
			start, err := strconv.Atoi(bounds[0])
			if err != nil || start < 0 {
				return nil, fmt.Errorf("invalid CPU list segment %q", part)
			}
			end := start
			if len(bounds) == 2 {
				end, err = strconv.Atoi(bounds[1])
				if err != nil || end < start || end-start > 4096 {
					return nil, fmt.Errorf("invalid CPU list range %q", part)
				}
			}
			for id := start; id <= end; id++ {
				if seen[id] {
					return nil, fmt.Errorf("duplicate CPU id %d", id)
				}
				seen[id] = true
				result = append(result, id)
			}
		}
	}
	return result, nil
}

func findNUMANode(cpuPath string) string {
	matches, err := filepath.Glob(filepath.Join(cpuPath, "node[0-9]*"))
	if err != nil || len(matches) == 0 {
		return ""
	}
	return strings.TrimPrefix(filepath.Base(matches[0]), "node")
}

func readPolicyCPUList(policyPath string) ([]int, error) {
	var lastErr error
	for _, filename := range []string{"affected_cpus", "related_cpus"} {
		raw, err := readSysfsValue(filepath.Join(policyPath, filename))
		if err != nil {
			lastErr = err
			continue
		}
		if strings.TrimSpace(raw) == "" {
			lastErr = fmt.Errorf("%w: cpufreq policy CPU list is empty", os.ErrNotExist)
			continue
		}
		cpus, err := parseCPUList(raw)
		if err != nil || len(cpus) == 0 {
			if err == nil {
				err = fmt.Errorf("%w: cpufreq policy CPU list is empty", os.ErrNotExist)
			}
			lastErr = err
			continue
		}
		return cpus, nil
	}
	if lastErr == nil {
		lastErr = errors.New("cpufreq policy CPU list is unavailable")
	}
	return nil, lastErr
}

func frequencyPoliciesForCPU(cpuSysfsPath string, cpuID int64) []string {
	policies, err := filepath.Glob(filepath.Join(cpuSysfsPath, "cpufreq", "policy[0-9]*"))
	if err != nil {
		return nil
	}
	sort.Strings(policies)
	result := make([]string, 0, 1)
	for _, policyPath := range policies {
		cpuIDs, readErr := readPolicyCPUList(policyPath)
		if readErr != nil {
			continue
		}
		for _, id := range cpuIDs {
			if int64(id) == cpuID {
				result = append(result, filepath.Base(policyPath))
				break
			}
		}
	}
	return result
}

func isFrequencyDomainID(value string) bool {
	if !strings.HasPrefix(value, "policy") {
		return false
	}
	id, err := strconv.Atoi(strings.TrimPrefix(value, "policy"))
	return err == nil && id >= 0
}

func cpusForFrequencyPolicy(cpuSysfsPath, policyID string) []int {
	if !isFrequencyDomainID(policyID) {
		return nil
	}
	path := filepath.Join(cpuSysfsPath, "cpufreq", policyID)
	cpus, err := readPolicyCPUList(path)
	if err != nil {
		return nil
	}
	sort.Ints(cpus)
	return cpus
}
