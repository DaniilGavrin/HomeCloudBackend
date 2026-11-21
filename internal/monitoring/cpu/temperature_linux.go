//go:build linux

package cpu

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const defaultHwmonPath = "/sys/class/hwmon"

var (
	coreLabelPattern = regexp.MustCompile(`(?i)core\s*(?:id\s*)?(\d+)`)
	packageLabelPattern = regexp.MustCompile(`(?i)package\s*(?:id\s*)?(\d+)`)
)

func (c *SystemCollector) ReadTemperatureMetrics(ctx context.Context) ([]MetricItem, error) {
	return readLinuxCPUTemperatures(ctx, defaultHwmonPath)
}

func readLinuxCPUTemperatures(ctx context.Context, hwmonPath string) ([]MetricItem, error) {
	now := time.Now().UTC()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	directories, err := filepath.Glob(filepath.Join(hwmonPath, "hwmon[0-9]*"))
	if err != nil {
		status, reason, code := classifySourceError(err)
		return []MetricItem{statusItem(MetricTemperature, "", status, reason, code, "linux.sysfs.hwmon", now)}, nil
	}
	sort.Strings(directories)
	items := make([]MetricItem, 0)
	for _, directory := range directories {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		driverName, nameErr := readSysfsValue(filepath.Join(directory, "name"))
		if nameErr != nil {
			continue
		}
		inputs, globErr := filepath.Glob(filepath.Join(directory, "temp[0-9]*_input"))
		if globErr != nil {
			continue
		}
		sort.Strings(inputs)
		for _, inputPath := range inputs {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			inputName := filepath.Base(inputPath)
			index := strings.TrimSuffix(strings.TrimPrefix(inputName, "temp"), "_input")
			label, labelErr := readSysfsValue(filepath.Join(directory, "temp"+index+"_label"))
			if labelErr != nil {
				label = ""
			}
			if !isCPUTemperatureSensor(driverName, label) {
				continue
			}
			objectID := cpuTemperatureObjectID(driverName, label, index)
			raw, readErr := readSysfsValue(inputPath)
			if readErr != nil {
				status, reason, code := classifySourceError(readErr)
				if errors.Is(readErr, os.ErrNotExist) {
					status, reason, code = StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE"
				}
				items = append(items, statusItem(MetricTemperature, objectID, status, reason, code, "linux.sysfs.hwmon", now))
				continue
			}
			milliCelsius, parseErr := strconv.ParseInt(raw, 10, 64)
			if parseErr != nil {
				items = append(items, statusItem(MetricTemperature, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.sysfs.hwmon", now))
				continue
			}
			celsius := float64(milliCelsius) / 1000
			if math.IsNaN(celsius) || math.IsInf(celsius, 0) || celsius < -40 || celsius > 150 {
				items = append(items, statusItem(MetricTemperature, objectID, StatusInvalid, "out_of_range", "SOURCE_INVALID_DATA", "linux.sysfs.hwmon", now))
				continue
			}
			items = append(items, MetricItem{
				MetricID: MetricTemperature, ObjectID: objectID, Value: celsius, Unit: "°C",
				ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid, SourceID: "linux.sysfs.hwmon",
			})
		}
	}
	if len(items) == 0 {
		return []MetricItem{statusItem(MetricTemperature, "", StatusUnsupported, "unsupported_device", "METRIC_UNSUPPORTED", "linux.sysfs.hwmon", now)}, nil
	}
	return items, nil
}

func isCPUTemperatureSensor(driverName, label string) bool {
	driver := strings.ToLower(strings.TrimSpace(driverName))
	lowerLabel := strings.ToLower(strings.TrimSpace(label))
	switch driver {
	case "coretemp", "k10temp", "zenpower", "x86_pkg_temp", "cpu_thermal", "soc_thermal":
		return true
	}
	return strings.Contains(lowerLabel, "cpu") ||
		strings.Contains(lowerLabel, "core") ||
		strings.Contains(lowerLabel, "package")
}

func cpuTemperatureObjectID(driverName, label, index string) string {
	if match := coreLabelPattern.FindStringSubmatch(label); len(match) == 2 {
		return "cpu" + match[1]
	}
	if match := packageLabelPattern.FindStringSubmatch(label); len(match) == 2 {
		return "package" + match[1]
	}
	lower := strings.ToLower(strings.TrimSpace(label))
	if strings.Contains(lower, "cpu") || strings.Contains(lower, "tctl") || strings.Contains(lower, "tdie") {
		return "package0"
	}
	if strings.TrimSpace(label) == "" {
		return fmt.Sprintf("%s/temp%s", metricSlug(driverName), index)
	}
	return fmt.Sprintf("%s/%s", metricSlug(driverName), metricSlug(label))
}
