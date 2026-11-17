package telemetry

import (
	"bufio"
	"context"
	"os"
	"strconv"
	"strings"
	"time"
)

type PressureCollector struct{ interval time.Duration }

func NewPressureCollector(interval time.Duration) *PressureCollector {
	if interval < 5*time.Second { interval = 5*time.Second }
	return &PressureCollector{interval:interval}
}
func (c *PressureCollector) Name() string { return "pressure" }
func (c *PressureCollector) Interval() time.Duration { return c.interval }

// Collect reads Linux PSI (Pressure Stall Information) without invoking subprocesses.
func (c *PressureCollector) Collect(ctx context.Context) ([]Sample, error) {
	now := time.Now().UTC()
	out := make([]Sample, 0, 24)
	for _, resource := range []string{"cpu","memory","io"} {
		if err := ctx.Err(); err != nil { return out, err }
		file, err := os.Open("/proc/pressure/"+resource)
		if err != nil { continue }
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			fields := strings.Fields(scanner.Text())
			if len(fields) == 0 { continue }
			stall := fields[0]
			for _, field := range fields[1:] {
				key, raw, ok := strings.Cut(field, "=")
				if !ok { continue }
				value, parseErr := strconv.ParseFloat(raw,64)
				if parseErr != nil { continue }
				metric := ""
				unit := "percent"
				switch key {
				case "avg10": metric = "avg10"
				case "avg60": metric = "avg60"
				case "avg300": metric = "avg300"
				case "total": metric = "total_microseconds"; unit = "microseconds"
				default: continue
				}
				out = append(out, Sample{
					Name:"system.pressure."+metric,
					Labels:map[string]string{"resource":resource,"stall":stall},
					Value:value, Unit:unit, Timestamp:now,
				})
			}
		}
		scanErr := scanner.Err()
		_ = file.Close()
		if scanErr != nil { return out, scanErr }
	}
	return out, nil
}
