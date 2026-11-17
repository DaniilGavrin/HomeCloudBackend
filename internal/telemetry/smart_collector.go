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
		cmdCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
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
					switch strings.ToLower(name) {
					case "reallocated_sector_ct": appendMetric("disk.smart.reallocated_sectors", value, "sectors")
					case "current_pending_sector": appendMetric("disk.smart.pending_sectors", value, "sectors")
					case "offline_uncorrectable": appendMetric("disk.smart.uncorrectable_sectors", value, "sectors")
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
