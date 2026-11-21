package cpu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"HomeCloudBackend/internal/commands"
	"github.com/shirou/gopsutil/v4/cpu"
)

const (
	MetricTotalUsage = "system.cpu.totalusage"
	MetricUser = "system.cpu.user"
	MetricSystem = "system.cpu.system"
	Target = "system.cpu"
	DefaultWindow = time.Second
	minWindow = 100 * time.Millisecond
	maxWindow = 10 * time.Second
)

type Counters struct {
	TotalSeconds float64
	IdleSeconds float64
	UserSeconds float64
	SystemSeconds float64
}

type Collector interface {
	Read(context.Context) (Counters, error)
	TotalUsage(context.Context, time.Duration) (float64, error)
}

type SystemCollector struct{}

func NewSystemCollector() *SystemCollector { return &SystemCollector{} }

func (c *SystemCollector) Read(ctx context.Context) (Counters, error) {
	stats, err := cpu.TimesWithContext(ctx, false)
	if err != nil {
		return Counters{}, fmt.Errorf("read CPU counters: %w", err)
	}
	if len(stats) == 0 {
		return Counters{}, errors.New("CPU counters are unavailable")
	}
	var result Counters
	for _, stat := range stats {
		result.UserSeconds += stat.User
		result.SystemSeconds += stat.System
		result.IdleSeconds += stat.Idle
		result.TotalSeconds += stat.User + stat.System + stat.Nice + stat.Idle +
			stat.Iowait + stat.Irq + stat.Softirq + stat.Steal + stat.Guest + stat.GuestNice
	}
	if math.IsNaN(result.TotalSeconds) || math.IsInf(result.TotalSeconds, 0) ||
		math.IsNaN(result.UserSeconds) || math.IsInf(result.UserSeconds, 0) ||
		math.IsNaN(result.SystemSeconds) || math.IsInf(result.SystemSeconds, 0) {
		return Counters{}, errors.New("CPU counters contain non-finite values")
	}
	return result, nil
}

func (c *SystemCollector) TotalUsage(ctx context.Context, window time.Duration) (float64, error) {
	if window < minWindow || window > maxWindow {
		return 0, fmt.Errorf("window_ms must be between %d and %d", minWindow.Milliseconds(), maxWindow.Milliseconds())
	}
	before, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-timer.C:
	}
	after, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	value, ok := CalculateTotalUsage(before, after)
	if !ok {
		return 0, errors.New("CPU counters did not advance during the measurement window")
	}
	return value, nil
}

// CalculateTotalUsage excludes idle time. I/O wait is counted as non-idle.
// Invalid or reset counter intervals are unavailable, never synthetic zeroes.
func CalculateTotalUsage(previous, current Counters) (float64, bool) {
	totalDelta := current.TotalSeconds - previous.TotalSeconds
	idleDelta := current.IdleSeconds - previous.IdleSeconds
	if totalDelta <= 0 || idleDelta < 0 || idleDelta > totalDelta {
		return 0, false
	}
	value := (totalDelta - idleDelta) / totalDelta * 100
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return 0, false
	}
	return math.Max(0, math.Min(100, value)), true
}

func counterNanoseconds(seconds float64) int64 {
	if seconds <= 0 {
		return 0
	}
	return int64(math.Round(seconds * float64(time.Second)))
}

type metricResult struct {
	Value any `json:"value"`
	Unit string `json:"unit"`
	ObservedAt string `json:"observed_at"`
}

func RegisterCommands(registry *commands.Registry, collector Collector) {
	registry.MustRegister(MetricTotalUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		window := DefaultWindow
		if len(msg.Data) > 0 {
			var params struct {
				WindowMS int64 `json:"window_ms"`
			}
			if err := json.Unmarshal(msg.Data, &params); err != nil {
				return nil, fmt.Errorf("invalid command params: %w", err)
			}
			if params.WindowMS != 0 {
				window = time.Duration(params.WindowMS) * time.Millisecond
			}
		}
		value, err := collector.TotalUsage(ctx, window)
		if err != nil {
			return nil, err
		}
		return metricResult{Value: value, Unit: "percent", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
	})

	registerCounter := func(commandID, field string) {
		registry.MustRegister(commandID, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			counters, err := collector.Read(ctx)
			if err != nil {
				return nil, err
			}
			value := counters.SystemSeconds
			if field == "user" {
				value = counters.UserSeconds
			}
			return metricResult{Value: counterNanoseconds(value), Unit: "ns", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
		})
	}
	registerCounter(MetricUser, "user")
	registerCounter(MetricSystem, "system")
}
