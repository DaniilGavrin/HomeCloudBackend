package cpu

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultCPUPressurePath = "/proc/pressure/cpu"

func (c *SystemCollector) ReadPressureMetrics(ctx context.Context) ([]MetricItem, error) {
	return readLinuxCPUPressure(ctx, defaultCPUPressurePath)
}

// readLinuxCPUPressure exports PSI avg10 as the canonical CPU pressure gauge.
// The observation window is explicit; PSI's cumulative total is intentionally
// not substituted for the percentage metric.
func readLinuxCPUPressure(ctx context.Context, path string) ([]MetricItem, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	data, err := os.Open(path)
	if err != nil {
		status, reason, code := classifySourceError(err)
		if errors.Is(err, os.ErrNotExist) {
			status, reason, code = StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED"
		}
		return []MetricItem{
			statusItem(MetricPressureSome, "", status, reason, code, "linux.proc.pressure.cpu", now),
			statusItem(MetricPressureFull, "", status, reason, code, "linux.proc.pressure.cpu", now),
		}, nil
	}
	defer data.Close()

	values := make(map[string]float64, 2)
	seen := make(map[string]bool, 2)
	scanner := bufio.NewScanner(data)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		name := fields[0]
		if name != "some" && name != "full" {
			continue
		}
		seen[name] = true
		for _, field := range fields[1:] {
			key, raw, ok := strings.Cut(field, "=")
			if !ok || key != "avg10" {
				continue
			}
			value, parseErr := strconv.ParseFloat(raw, 64)
			if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
				continue
			}
			values[name] = value
			break
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		status, reason, code := classifySourceError(scanErr)
		return []MetricItem{
			statusItem(MetricPressureSome, "", status, reason, code, "linux.proc.pressure.cpu", now),
			statusItem(MetricPressureFull, "", status, reason, code, "linux.proc.pressure.cpu", now),
		}, nil
	}

	items := make([]MetricItem, 0, 2)
	for _, metric := range []struct {
		name string
		id string
	}{
		{name: "some", id: MetricPressureSome},
		{name: "full", id: MetricPressureFull},
	} {
		value, ok := values[metric.name]
		if !seen[metric.name] {
			items = append(items, statusItem(metric.id, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "linux.proc.pressure.cpu", now))
			continue
		}
		if !ok {
			items = append(items, statusItem(metric.id, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "linux.proc.pressure.cpu", now))
			continue
		}
		items = append(items, MetricItem{
			MetricID: metric.id, Value: value, Unit: "percent",
			ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
			SourceID: "linux.proc.pressure.cpu", WindowMS: 10_000,
		})
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("CPU pressure source returned no metrics")
	}
	return items, nil
}
