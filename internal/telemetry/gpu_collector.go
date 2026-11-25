package telemetry

import (
	"context"
	"encoding/csv"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type GPUCollector struct{ interval time.Duration }

func NewGPUCollector(interval time.Duration) *GPUCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &GPUCollector{interval:interval}
}
func (c *GPUCollector) Name() string { return "gpu" }
func (c *GPUCollector) Interval() time.Duration { return c.interval }

func (c *GPUCollector) Collect(ctx context.Context) ([]Sample, error) {
	if _, err := exec.LookPath("nvidia-smi"); err == nil {
		return c.collectNVIDIA(ctx)
	}
	return c.collectDRM(ctx)
}

func (c *GPUCollector) collectNVIDIA(ctx context.Context) ([]Sample, error) {
	cmdCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
	defer cancel()
	cmd := exec.CommandContext(cmdCtx, "nvidia-smi",
		"--query-gpu=index,utilization.gpu,memory.used,memory.total,temperature.gpu,power.draw,clocks.gr,clocks.mem",
		"--format=csv,noheader,nounits")
	raw, err := cmd.Output()
	if err != nil { return nil, err }
	rows, err := csv.NewReader(strings.NewReader(string(raw))).ReadAll()
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(rows)*7)
	for _, row := range rows {
		if len(row) != 8 { continue }
		labels := map[string]string{"gpu":strings.TrimSpace(row[0]), "vendor":"nvidia"}
		fields := []struct{ index int; name, unit string }{
			{1,"hardware.gpu.utilization_percent","percent"},
			{2,"hardware.gpu.memory_used_bytes","bytes"},
			{3,"hardware.gpu.memory_total_bytes","bytes"},
			{4,"hardware.gpu.temperature_celsius","celsius"},
			{5,"hardware.gpu.power_watts","watts"},
			{6,"hardware.gpu.clock_graphics_mhz","megahertz"},
			{7,"hardware.gpu.clock_memory_mhz","megahertz"},
		}
		for _, field := range fields {
			value, parseErr := strconv.ParseFloat(strings.TrimSpace(row[field.index]), 64)
			if parseErr != nil { continue }
			if field.index == 2 || field.index == 3 { value *= 1024 * 1024 }
			out = append(out, Sample{Name:field.name, Labels:labels, Value:value, Unit:field.unit, Timestamp:now})
		}
	}
	return out, nil
}

func (c *GPUCollector) collectDRM(ctx context.Context) ([]Sample, error) {
	paths, err := filepath.Glob("/sys/class/drm/card[0-9]*/device/gpu_busy_percent")
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(paths)*3)
	for _, path := range paths {
		if err := ctx.Err(); err != nil { return out, err }
		raw, err := os.ReadFile(path)
		if err != nil { continue }
		utilization, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
		if err != nil || utilization < 0 || utilization > 100 { continue }
		card := strings.Split(filepath.Base(filepath.Dir(filepath.Dir(path))), "-")[0]
		labels := map[string]string{"gpu":card, "vendor":"sysfs"}
		out = append(out, Sample{Name:"hardware.gpu.utilization_percent", Labels:labels, Value:utilization, Unit:"percent", Timestamp:now})
		deviceDir := filepath.Dir(path)
		for _, item := range []struct{ file, metric string }{
			{"mem_info_vram_used","hardware.gpu.memory_used_bytes"},
			{"mem_info_vram_total","hardware.gpu.memory_total_bytes"},
		} {
			if data, readErr := os.ReadFile(filepath.Join(deviceDir,item.file)); readErr == nil {
				if value, parseErr := strconv.ParseFloat(strings.TrimSpace(string(data)),64); parseErr == nil && value >= 0 {
					out = append(out, Sample{Name:item.metric, Labels:labels, Value:value, Unit:"bytes", Timestamp:now})
				}
			}
		}
	}
	return out, nil
}
