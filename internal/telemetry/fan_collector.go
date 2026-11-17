package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type FanCollector struct{ interval time.Duration }

func NewFanCollector(interval time.Duration) *FanCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &FanCollector{interval:interval}
}
func (c *FanCollector) Name() string { return "fans" }
func (c *FanCollector) Interval() time.Duration { return c.interval }
func (c *FanCollector) Collect(ctx context.Context) ([]Sample, error) {
	if runtime.GOOS != "linux" { return nil, nil }
	paths, err := filepath.Glob("/sys/class/hwmon/hwmon*/fan*_input")
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(paths))
	for _, path := range paths {
		if err := ctx.Err(); err != nil { return out, err }
		raw, err := os.ReadFile(path)
		if err != nil { continue }
		rpm, err := strconv.ParseFloat(strings.TrimSpace(string(raw)), 64)
		if err != nil || rpm < 0 { continue }
		dir := filepath.Dir(path)
		device := "hwmon"
		if name, readErr := os.ReadFile(filepath.Join(dir, "name")); readErr == nil {
			if value := strings.TrimSpace(string(name)); value != "" { device = value }
		}
		fan := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(path), "fan"), "_input")
		if label, readErr := os.ReadFile(filepath.Join(dir, "fan"+fan+"_label")); readErr == nil {
			if value := strings.TrimSpace(string(label)); value != "" { fan = value }
		}
		out = append(out, Sample{
			Name:"hardware.fan.rpm",
			Labels:map[string]string{"device":device, "fan":fan},
			Value:rpm, Unit:"rpm", Timestamp:now,
		})
	}
	return out, nil
}
