package telemetry

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// DiskInventoryCollector discovers physical block devices through Linux sysfs.
// It deliberately omits serial numbers and other persistent hardware identifiers.
type DiskInventoryCollector struct{ interval time.Duration }

func NewDiskInventoryCollector(interval time.Duration) *DiskInventoryCollector {
	if interval < 5*time.Minute { interval = 5*time.Minute }
	return &DiskInventoryCollector{interval:interval}
}
func (c *DiskInventoryCollector) Name() string { return "disk_inventory" }
func (c *DiskInventoryCollector) Interval() time.Duration { return c.interval }
func (c *DiskInventoryCollector) Collect(ctx context.Context) ([]Sample, error) {
	paths, err := filepath.Glob("/sys/block/*")
	if err != nil { return nil, err }
	now := time.Now().UTC()
	out := make([]Sample, 0, len(paths)*2)
	for _, path := range paths {
		if err := ctx.Err(); err != nil { return out, err }
		if _, err := os.Stat(filepath.Join(path, "device")); err != nil { continue }
		device := filepath.Base(path)
		labels := map[string]string{"device":device}
		if raw, e := os.ReadFile(filepath.Join(path, "device", "model")); e == nil {
			model := strings.TrimSpace(string(raw)); if model != "" { labels["model"] = model }
		}
		if raw, e := os.ReadFile(filepath.Join(path, "device", "vendor")); e == nil {
			vendor := strings.TrimSpace(string(raw)); if vendor != "" { labels["vendor"] = vendor }
		}
		if raw, e := os.ReadFile(filepath.Join(path, "size")); e == nil {
			if sectors, e := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64); e == nil {
				out = append(out, Sample{Name:"disk.device.size_bytes", Labels:labels, Value:float64(sectors)*512, Unit:"bytes", Timestamp:now})
			}
		}
		if raw, e := os.ReadFile(filepath.Join(path, "queue", "rotational")); e == nil {
			if rotational, e := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 8); e == nil && rotational <= 1 {
				out = append(out, Sample{Name:"disk.device.rotational", Labels:labels, Value:float64(rotational), Unit:"boolean", Timestamp:now})
			}
		}
	}
	return out, nil
}
