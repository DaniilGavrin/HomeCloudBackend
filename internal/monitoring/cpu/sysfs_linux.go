package cpu

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	gopsutilcpu "github.com/shirou/gopsutil/v4/cpu"
)

const (
	MetricFrequencyCurrent  = "system.cpu.frequency.current"
	MetricFrequencyMin      = "system.cpu.frequency.min"
	MetricFrequencyMax      = "system.cpu.frequency.max"
	MetricFrequencyBase     = "system.cpu.frequency.base"
	MetricFrequencyGovernor = "system.cpu.frequency.governor"
	MetricFrequencyTarget   = "system.cpu.frequency"

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

type FrequencyStats struct {
	ObjectID string
	Items    []MetricItem
}

// readLinuxFrequency reads the cpufreq policy interface. scaling_cur_freq is
// intentionally reported as the driver's last/current scaling value; it is
// not presented as a hardware-instantaneous APERF/MPERF measurement.
func readLinuxFrequency(ctx context.Context, cpuSysfsPath string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return nil, nil
	}
	policies, err := filepath.Glob(filepath.Join(cpuSysfsPath, "cpufreq", "policy[0-9]*"))
	if err != nil {
		return nil, fmt.Errorf("list CPU frequency policies: %w", err)
	}
	items := make([]MetricItem, 0, len(policies)*5)
	for _, policyPath := range policies {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		objectID := filepath.Base(policyPath)
		appendFrequency := func(metricID, filename, unit string) {
			value, readErr := readSysfsValue(filepath.Join(policyPath, filename))
			if readErr != nil {
				return
			}
			var parsed any = value
			if unit == "kHz" {
				number, parseErr := strconv.ParseInt(value, 10, 64)
				if parseErr != nil || number <= 0 {
					return
				}
				parsed = number
			}
			items = append(items, MetricItem{
				MetricID: metricID, ObjectID: objectID, Value: parsed, Unit: unit,
			})
		}
		appendFrequency(MetricFrequencyCurrent, "scaling_cur_freq", "kHz")
		appendFrequency(MetricFrequencyMin, "cpuinfo_min_freq", "kHz")
		appendFrequency(MetricFrequencyMax, "cpuinfo_max_freq", "kHz")
		appendFrequency(MetricFrequencyBase, "base_frequency", "kHz")
		if _, exists := findMetric(items, MetricFrequencyBase, objectID); !exists {
			appendFrequency(MetricFrequencyBase, "cpuinfo_base_freq", "kHz")
		}
		appendFrequency(MetricFrequencyGovernor, "scaling_governor", "string")
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if runtime.GOOS != "linux" {
		return nil, errors.New("CPU topology sysfs provider is supported on Linux only")
	}

	presentText, err := readSysfsValue(filepath.Join(cpuSysfsPath, "present"))
	if err != nil {
		return nil, fmt.Errorf("read present CPU list: %w", err)
	}
	present, err := parseCPUList(presentText)
	if err != nil {
		return nil, fmt.Errorf("parse present CPU list: %w", err)
	}
	onlineText, err := readSysfsValue(filepath.Join(cpuSysfsPath, "online"))
	if err != nil {
		// Some systems expose only the present list. In that case online state
		// cannot safely be inferred, so return an explicit source error.
		return nil, fmt.Errorf("read online CPU list: %w", err)
	}
	online, err := parseCPUList(onlineText)
	if err != nil {
		return nil, fmt.Errorf("parse online CPU list: %w", err)
	}
	onlineSet := make(map[int]bool, len(online))
	for _, id := range online {
		onlineSet[id] = true
	}
	presentSet := make(map[int]bool, len(present))
	for _, id := range present {
		presentSet[id] = true
	}
	for _, id := range online {
		if !presentSet[id] {
			return nil, fmt.Errorf("online CPU %d is not present", id)
		}
	}

	items := []MetricItem{
		{MetricID: MetricTopologyLogicalCount, Value: len(present), Unit: "count"},
		{MetricID: MetricTopologyOnlineCount, Value: len(online), Unit: "count"},
		{MetricID: MetricTopologyOfflineCount, Value: len(present) - len(online), Unit: "count"},
		{MetricID: MetricTopologyArchitecture, Value: runtime.GOARCH, Unit: "string"},
	}

	type cpuLocation struct {
		cpuID    int
		coreID   string
		packageID string
		dieID    string
		numaID   string
		online   bool
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
		location := cpuLocation{
			cpuID: id, coreID: coreID, packageID: packageID, dieID: dieID,
			numaID: numaID, online: onlineSet[id],
		}
		locations = append(locations, location)
		if location.online {
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
	}
	if len(cores) > 0 {
		items = append(items, MetricItem{MetricID: MetricTopologyCoreCount, Value: len(cores), Unit: "count"})
	}
	if len(packages) > 0 {
		items = append(items, MetricItem{MetricID: MetricTopologyPackageCount, Value: len(packages), Unit: "count"})
	}
	if len(dies) > 0 {
		items = append(items, MetricItem{MetricID: MetricTopologyDieCount, Value: len(dies), Unit: "count"})
	}
	if len(numaNodes) > 0 {
		items = append(items, MetricItem{MetricID: MetricTopologyNUMACount, Value: len(numaNodes), Unit: "count"})
	}

	for _, location := range locations {
		objectID := fmt.Sprintf("cpu%d", location.cpuID)
		items = append(items, MetricItem{MetricID: MetricTopologyOnline, ObjectID: objectID, Value: location.online, Unit: "boolean"})
		appendID := func(metricID, value string) {
			if value != "" {
				items = append(items, MetricItem{MetricID: metricID, ObjectID: objectID, Value: value, Unit: "id"})
			}
		}
		appendID(MetricTopologyCoreID, location.coreID)
		appendID(MetricTopologyPackageID, location.packageID)
		appendID(MetricTopologyDieID, location.dieID)
		appendID(MetricTopologyNUMAID, location.numaID)
	}

	if info, infoErr := gopsutilcpu.InfoWithContext(ctx); infoErr == nil && len(info) > 0 {
		first := info[0]
		appendStringMetric := func(metricID, value string) {
			if strings.TrimSpace(value) != "" {
				items = append(items, MetricItem{MetricID: metricID, Value: value, Unit: "string"})
			}
		}
		appendStringMetric(MetricTopologyVendor, first.VendorID)
		appendStringMetric(MetricTopologyModel, first.ModelName)
		appendStringMetric(MetricTopologyFamily, first.Family)
		appendStringMetric(MetricTopologyModelID, first.Model)
		if len(first.Flags) > 0 {
			items = append(items, MetricItem{MetricID: MetricTopologyFeatures, Value: first.Flags, Unit: "list"})
		}
		if first.Stepping != 0 {
			items = append(items, MetricItem{MetricID: MetricTopologyStepping, Value: first.Stepping, Unit: "count"})
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
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("CPU list is empty")
	}
	seen := make(map[int]bool)
	var result []int
	for _, part := range strings.Split(value, ",") {
		bounds := strings.Split(strings.TrimSpace(part), "-")
		if len(bounds) < 1 || len(bounds) > 2 {
			return nil, fmt.Errorf("invalid CPU range %q", part)
		}
		start, err := strconv.Atoi(bounds[0])
		if err != nil || start < 0 {
			return nil, fmt.Errorf("invalid CPU index %q", bounds[0])
		}
		end := start
		if len(bounds) == 2 {
			end, err = strconv.Atoi(bounds[1])
			if err != nil || end < start {
				return nil, fmt.Errorf("invalid CPU range %q", part)
			}
		}
		if end-start > 4096 {
			return nil, fmt.Errorf("CPU range %q is too large", part)
		}
		for id := start; id <= end; id++ {
			if !seen[id] {
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
