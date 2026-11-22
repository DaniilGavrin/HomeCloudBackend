package cpu

import (
	"bufio"
	"context"
	"errors"
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
		return pressureStatusItems(status, reason, code, now), nil
	}
	defer data.Close()

	type pressureValues struct {
		seen bool
		values map[string]float64
		total uint64
		totalSeen bool
		invalid map[string]bool
	}
	parsed := map[string]*pressureValues{
		"some": {values: make(map[string]float64), invalid: make(map[string]bool)},
		"full": {values: make(map[string]float64), invalid: make(map[string]bool)},
	}
	scanner := bufio.NewScanner(data)
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fields := strings.Fields(scanner.Text())
		if len(fields) == 0 {
			continue
		}
		values, ok := parsed[fields[0]]
		if !ok {
			continue
		}
		values.seen = true
		for _, field := range fields[1:] {
			key, raw, ok := strings.Cut(field, "=")
			if !ok {
				continue
			}
			switch key {
			case "avg10", "avg60", "avg300":
				value, parseErr := strconv.ParseFloat(raw, 64)
				if parseErr != nil || math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 100 {
					values.invalid[key] = true
					continue
				}
				values.values[key] = value
			case "total":
				value, parseErr := strconv.ParseUint(raw, 10, 64)
				if parseErr != nil || value > math.MaxInt64/1000 {
					values.invalid[key] = true
					continue
				}
				values.total = value
				values.totalSeen = true
			}
		}
	}
	if scanErr := scanner.Err(); scanErr != nil {
		status, reason, code := classifySourceError(scanErr)
		return pressureStatusItems(status, reason, code, now), nil
	}

	items := make([]MetricItem, 0, 8)
	for _, metric := range []struct {
		name string
		id string
	}{
		{name: "some", id: MetricPressureSome},
		{name: "full", id: MetricPressureFull},
	} {
		values := parsed[metric.name]
		if !values.seen {
			for _, window := range []int64{10_000, 60_000, 300_000} {
				items = append(items, statusItem(metric.id, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "linux.proc.pressure.cpu", now))
				items[len(items)-1].WindowMS = window
			}
			items = append(items, statusItem(metric.id, "", StatusUnsupported, "unsupported_platform", "METRIC_UNSUPPORTED", "linux.proc.pressure.cpu", now))
			continue
		}
		for _, window := range []struct {
			key string
			ms int64
		}{
			{key: "avg10", ms: 10_000},
			{key: "avg60", ms: 60_000},
			{key: "avg300", ms: 300_000},
		} {
			value, ok := values.values[window.key]
			if !ok {
				status, reason, code := StatusUnsupported, "field_not_provided", "METRIC_UNSUPPORTED"
				if values.invalid[window.key] {
					status, reason, code = StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA"
				}
				item := statusItem(metric.id, "", status, reason, code, "linux.proc.pressure.cpu", now)
				item.WindowMS = window.ms
				items = append(items, item)
				continue
			}
			items = append(items, MetricItem{
				MetricID: metric.id, Value: value, Unit: "percent",
				ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
				SourceID: "linux.proc.pressure.cpu", WindowMS: window.ms,
			})
		}
		if !values.totalSeen {
			status, reason, code := StatusUnsupported, "field_not_provided", "METRIC_UNSUPPORTED"
			if values.invalid["total"] {
				status, reason, code = StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA"
			}
			items = append(items, statusItem(metric.id, "", status, reason, code, "linux.proc.pressure.cpu", now))
			continue
		}
		items = append(items, MetricItem{
			MetricID: metric.id, Value: int64(values.total * 1000), Unit: "ns",
			ObservedAt: now.Format(time.RFC3339Nano), Status: StatusValid,
			SourceID: "linux.proc.pressure.cpu",
		})
	}
	return items, nil
}

func pressureStatusItems(status, reason, code string, observedAt time.Time) []MetricItem {
	items := make([]MetricItem, 0, 8)
	for _, metricID := range []string{MetricPressureSome, MetricPressureFull} {
		for _, window := range []int64{10_000, 60_000, 300_000} {
			item := statusItem(metricID, "", status, reason, code, "linux.proc.pressure.cpu", observedAt)
			item.WindowMS = window
			items = append(items, item)
		}
		items = append(items, statusItem(metricID, "", status, reason, code, "linux.proc.pressure.cpu", observedAt))
	}
	return items
}
