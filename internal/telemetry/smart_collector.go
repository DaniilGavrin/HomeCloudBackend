package telemetry

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// SMARTCollector uses smartmontools when installed. Devices are discovered from
// sysfs; commands are executed without a shell and have a strict per-device timeout.
type SMARTCollector struct{ interval time.Duration }

func NewSMARTCollector(interval time.Duration) *SMARTCollector {
	if interval < time.Minute { interval = time.Minute }
	return &SMARTCollector{interval: interval}
}
func (c *SMARTCollector) Name() string { return "smart" }
func (c *SMARTCollector) Interval() time.Duration { return c.interval }

func (c *SMARTCollector) Collect(ctx context.Context) ([]Sample, error) {
	if _, err := exec.LookPath("smartctl"); err != nil { return nil, nil }
	paths, err := filepath.Glob("/sys/block/*")
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(paths)*4)
	for _, path := range paths {
		if err := ctx.Err(); err != nil { return out, err }
		if _, err := os.Stat(filepath.Join(path, "device")); err != nil { continue }
		device := filepath.Base(path)
		devicePath := filepath.Join("/dev", device)
		cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		cmd := exec.CommandContext(cmdCtx, "smartctl", "-j", "-a", devicePath)
		raw, runErr := cmd.Output()
		cancel()
		if runErr != nil && len(raw) == 0 { continue }
		var payload map[string]any
		if json.Unmarshal(raw, &payload) != nil { continue }
		labels := map[string]string{"device": device}
		appendMetric := func(name string, value float64, unit string) {
			out = append(out, Sample{Name:name, Labels:labels, Value:value, Unit:unit, Timestamp:now})
		}
		if smart, ok := payload["smart_status"].(map[string]any); ok {
			if passed, ok := smart["passed"].(bool); ok {
				value := 0.0; if passed { value = 1 }
				appendMetric("disk.smart.health_passed", value, "boolean")
			}
		}
		if temp := nestedNumber(payload, "temperature", "current"); temp > 0 {
			appendMetric("disk.smart.temperature_celsius", temp, "celsius")
		} else if temp := nestedNumber(payload, "nvme_smart_health_information_log", "temperature"); temp > 0 {
			appendMetric("disk.smart.temperature_celsius", temp, "celsius")
		}
		if used := nestedNumber(payload, "nvme_smart_health_information_log", "percentage_used"); used >= 0 {
			appendMetric("disk.smart.nvme_percentage_used", used, "percent")
		}
		// Export all standard NVMe health-log counters when smartctl exposes them.
		if nvme, ok := payload["nvme_smart_health_information_log"].(map[string]any); ok {
			fields := map[string]string{
				"critical_warning":"disk.smart.nvme_critical_warning",
				"available_spare":"disk.smart.nvme_available_spare_percent",
				"available_spare_threshold":"disk.smart.nvme_available_spare_threshold_percent",
				"data_units_read":"disk.smart.nvme_data_units_read",
				"data_units_written":"disk.smart.nvme_data_units_written",
				"host_reads":"disk.smart.nvme_host_reads",
				"host_writes":"disk.smart.nvme_host_writes",
				"controller_busy_time":"disk.smart.nvme_controller_busy_minutes",
				"power_cycles":"disk.smart.nvme_power_cycles",
				"unsafe_shutdowns":"disk.smart.nvme_unsafe_shutdowns",
				"media_errors":"disk.smart.nvme_media_errors",
				"num_err_log_entries":"disk.smart.nvme_error_log_entries",
				"warning_temp_time":"disk.smart.nvme_warning_temperature_minutes",
				"critical_comp_time":"disk.smart.nvme_critical_temperature_minutes",
			}
			for field, metric := range fields {
				if value, ok := nvme[field].(float64); ok {
					unit := "count"
					if strings.Contains(metric, "percent") { unit = "percent" }
					if strings.Contains(metric, "minutes") { unit = "minutes" }
					appendMetric(metric, value, unit)
				}
			}
			for i := 1; i <= 8; i++ {
				field := "temperature_sensor_" + strconv.Itoa(i)
				if value, ok := nvme[field].(float64); ok && value > 0 {
					out = append(out, Sample{Name:"disk.smart.nvme_temperature_sensor_celsius", Labels:map[string]string{"device":device,"sensor":strconv.Itoa(i)}, Value:value, Unit:"celsius", Timestamp:now})
				}
			}
			if sensors, ok := nvme["temperature_sensors"].([]any); ok {
				for i, raw := range sensors {
					if value, ok := raw.(float64); ok && value > 0 {
						out = append(out, Sample{Name:"disk.smart.nvme_temperature_sensor_celsius", Labels:map[string]string{"device":device,"sensor":strconv.Itoa(i+1)}, Value:value, Unit:"celsius", Timestamp:now})
					}
				}
			}
		}
		if hours := nestedNumber(payload, "power_on_time", "hours"); hours >= 0 {
			appendMetric("disk.smart.power_on_hours", hours, "hours")
		}
		if table, ok := payload["ata_smart_attributes"].(map[string]any); ok {
			if attrs, ok := table["table"].([]any); ok {
				for _, item := range attrs {
					attr, ok := item.(map[string]any); if !ok { continue }
					name, _ := attr["name"].(string)
					rawValue, ok := attr["raw"].(map[string]any); if !ok { continue }
					value, ok := rawValue["value"].(float64); if !ok { continue }
					attributeLabels := map[string]string{"device":device,"attribute":strings.ToLower(name)}
					out = append(out, Sample{Name:"disk.smart.ata_attribute_raw_value", Labels:attributeLabels, Value:value, Unit:"count", Timestamp:now})
					if normalized, ok := attr["value"].(float64); ok {
						out = append(out, Sample{Name:"disk.smart.ata_attribute_normalized_value", Labels:attributeLabels, Value:normalized, Unit:"count", Timestamp:now})
					}
					if worst, ok := attr["worst"].(float64); ok {
						out = append(out, Sample{Name:"disk.smart.ata_attribute_worst_value", Labels:attributeLabels, Value:worst, Unit:"count", Timestamp:now})
					}
					switch strings.ToLower(name) {
					case "reallocated_sector_ct": appendMetric("disk.smart.reallocated_sectors", value, "sectors")
					case "current_pending_sector": appendMetric("disk.smart.pending_sectors", value, "sectors")
					case "offline_uncorrectable": appendMetric("disk.smart.uncorrectable_sectors", value, "sectors")
					case "temperature_celsius", "airflow_temperature_cel": appendMetric("disk.smart.temperature_celsius", value, "celsius")
					}
				}
			}
		}
	}
	return out, nil
}

func nestedNumber(root map[string]any, first, second string) float64 {
	obj, ok := root[first].(map[string]any); if !ok { return -1 }
	value, ok := obj[second]
	if !ok { return -1 }
	switch n := value.(type) {
	case float64: return n
	case json.Number: f, _ := strconv.ParseFloat(string(n), 64); return f
	default: return -1
	}
}
