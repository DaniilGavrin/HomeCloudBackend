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
	MetricUser       = "system.cpu.user"
	MetricSystem     = "system.cpu.system"
	MetricNice       = "system.cpu.nice"
	MetricIdle       = "system.cpu.idle"
	MetricIOWait     = "system.cpu.iowait"
	MetricIRQ        = "system.cpu.irq"
	MetricSoftIRQ    = "system.cpu.softirq"
	MetricSteal      = "system.cpu.steal"
	MetricGuest      = "system.cpu.guest"
	MetricGuestNice  = "system.cpu.guest_nice"
	MetricCoreTotalUsage = "system.cpu.core.totalusage"
	MetricCoreUser       = "system.cpu.core.user"
	Target           = "system.cpu"
	DefaultWindow    = time.Second
	minWindow        = 100 * time.Millisecond
	maxWindow        = 10 * time.Second
)

type Counters struct {
	TotalSeconds     float64
	IdleSeconds      float64
	UserSeconds      float64
	SystemSeconds    float64
	NiceSeconds      float64
	IOWaitSeconds    float64
	IRQSeconds       float64
	SoftIRQSeconds   float64
	StealSeconds     float64
	GuestSeconds     float64
	GuestNiceSeconds float64
}

type CoreCounters struct {
	ObjectID string
	Counters Counters
}

type Snapshot struct {
	Counters  Counters
	Cores     []CoreCounters
	Frequency []MetricItem
}

type Collector interface {
	Read(context.Context) (Snapshot, error)
	TotalUsage(context.Context, time.Duration) (float64, error)
}

type SystemCollector struct{}

func NewSystemCollector() *SystemCollector { return &SystemCollector{} }

// Read samples per-logical-CPU counters once and derives the aggregate from the
// same sample, keeping aggregate and per-core values temporally consistent.
func (c *SystemCollector) Read(ctx context.Context) (Snapshot, error) {
	aggregateStats, err := cpu.TimesWithContext(ctx, false)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read aggregate CPU counters: %w", err)
	}
	if len(aggregateStats) == 0 {
		return Snapshot{}, errors.New("aggregate CPU counters are unavailable")
	}
	perCPUStats, err := cpu.TimesWithContext(ctx, true)
	if err != nil {
		return Snapshot{}, fmt.Errorf("read per-CPU counters: %w", err)
	}
	if len(perCPUStats) == 0 {
		return Snapshot{}, errors.New("per-CPU counters are unavailable")
	}

	aggregate := countersFromTimes(aggregateStats[0])
	if err := validateCounters(aggregate); err != nil {
		return Snapshot{}, fmt.Errorf("invalid aggregate CPU counters: %w", err)
	}
	result := Snapshot{
		Counters: aggregate,
		Cores: make([]CoreCounters, 0, len(perCPUStats)),
	}
	seen := make(map[string]struct{}, len(perCPUStats))
	for _, stat := range perCPUStats {
		if stat.CPU == "" || stat.CPU == "cpu-total" {
			continue
		}
		if _, exists := seen[stat.CPU]; exists {
			return Snapshot{}, fmt.Errorf("duplicate logical CPU identifier %q", stat.CPU)
		}
		seen[stat.CPU] = struct{}{}
		counters := countersFromTimes(stat)
		if err := validateCounters(counters); err != nil {
			return Snapshot{}, fmt.Errorf("invalid counters for logical CPU %q: %w", stat.CPU, err)
		}
		result.Cores = append(result.Cores, CoreCounters{ObjectID: stat.CPU, Counters: counters})
	}
	if len(result.Cores) == 0 {
		return Snapshot{}, errors.New("per-CPU counters are unavailable")
	}
	// Frequency is an optional Linux provider. Missing cpufreq support does not
	// make CPU time counters unavailable.
	result.Frequency, _ = readLinuxFrequency(ctx, defaultSysfsCPUPath)
	return result, nil
}

func countersFromTimes(stat cpu.TimesStat) Counters {
	// On Linux, guest is already included in user and guest_nice in nice.
	// Do not add those buckets again or total CPU time will be double-counted.
	return Counters{
		TotalSeconds: stat.User + stat.System + stat.Nice + stat.Idle +
			stat.Iowait + stat.Irq + stat.Softirq + stat.Steal,
		IdleSeconds:      stat.Idle,
		UserSeconds:      stat.User,
		SystemSeconds:    stat.System,
		NiceSeconds:      stat.Nice,
		IOWaitSeconds:    stat.Iowait,
		IRQSeconds:       stat.Irq,
		SoftIRQSeconds:   stat.Softirq,
		StealSeconds:     stat.Steal,
		GuestSeconds:     stat.Guest,
		GuestNiceSeconds: stat.GuestNice,
	}
}

func validateCounters(c Counters) error {
	values := []float64{
		c.TotalSeconds, c.IdleSeconds, c.UserSeconds, c.SystemSeconds,
		c.NiceSeconds, c.IOWaitSeconds, c.IRQSeconds, c.SoftIRQSeconds,
		c.StealSeconds, c.GuestSeconds, c.GuestNiceSeconds,
	}
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 {
			return errors.New("CPU counters contain invalid values")
		}
	}
	return nil
}

func (c *SystemCollector) TotalUsage(ctx context.Context, window time.Duration) (float64, error) {
	if err := validateWindow(window); err != nil {
		return 0, err
	}
	before, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	if err := waitWindow(ctx, window); err != nil {
		return 0, err
	}
	after, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	value, ok := CalculateTotalUsage(before.Counters, after.Counters)
	if !ok {
		return 0, errors.New("CPU counters did not advance during the measurement window")
	}
	return value, nil
}

func validateWindow(window time.Duration) error {
	if window < minWindow || window > maxWindow {
		return fmt.Errorf("window_ms must be between %d and %d", minWindow.Milliseconds(), maxWindow.Milliseconds())
	}
	return nil
}

func waitWindow(ctx context.Context, window time.Duration) error {
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// CalculateTotalUsage excludes idle time. I/O wait is counted as non-idle.
// Guest time is not added separately because Linux already includes it in user/nice.
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
	Value      any    `json:"value"`
	Unit       string `json:"unit"`
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

	registerCounter := func(commandID string, read func(Counters) float64) {
		registry.MustRegister(commandID, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			snapshot, err := collector.Read(ctx)
			if err != nil {
				return nil, err
			}
			return metricResult{Value: counterNanoseconds(read(snapshot.Counters)), Unit: "ns", ObservedAt: time.Now().UTC().Format(time.RFC3339Nano)}, nil
		})
	}
	registerCounter(MetricUser, func(c Counters) float64 { return c.UserSeconds })
	registerCounter(MetricSystem, func(c Counters) float64 { return c.SystemSeconds })
	registerCounter(MetricNice, func(c Counters) float64 { return c.NiceSeconds })
	registerCounter(MetricIdle, func(c Counters) float64 { return c.IdleSeconds })
	registerCounter(MetricIOWait, func(c Counters) float64 { return c.IOWaitSeconds })
	registerCounter(MetricIRQ, func(c Counters) float64 { return c.IRQSeconds })
	registerCounter(MetricSoftIRQ, func(c Counters) float64 { return c.SoftIRQSeconds })
	registerCounter(MetricSteal, func(c Counters) float64 { return c.StealSeconds })
	registerCounter(MetricGuest, func(c Counters) float64 { return c.GuestSeconds })
	registerCounter(MetricGuestNice, func(c Counters) float64 { return c.GuestNiceSeconds })

	registry.MustRegister(MetricCoreUser, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		snapshot, err := collector.Read(ctx)
		if err != nil {
			return nil, err
		}
		return coreUserItems(snapshot.Cores, time.Now().UTC()), nil
	})
	registry.MustRegister(MetricTopologyCommand, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		items, err := readLinuxTopology(ctx, defaultSysfsCPUPath)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for i := range items {
			items[i].ObservedAt = now
		}
		return items, nil
	})

	registerFrequencyCommand := func(metricID string) {
		registry.MustRegister(metricID, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			items, err := readLinuxFrequency(ctx, defaultSysfsCPUPath)
			if err != nil {
				return nil, err
			}
			now := time.Now().UTC().Format(time.RFC3339Nano)
			filtered := make([]MetricItem, 0)
			for _, item := range items {
				if item.MetricID == metricID {
					item.ObservedAt = now
					filtered = append(filtered, item)
				}
			}
			return filtered, nil
		})
	}
	registerFrequencyCommand(MetricFrequencyCurrent)
	registerFrequencyCommand(MetricFrequencyMin)
	registerFrequencyCommand(MetricFrequencyMax)
	registerFrequencyCommand(MetricFrequencyBase)
	registerFrequencyCommand(MetricFrequencyGovernor)

	registry.MustRegister(MetricCoreTotalUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
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
		if err := validateWindow(window); err != nil {
			return nil, err
		}
		before, err := collector.Read(ctx)
		if err != nil {
			return nil, err
		}
		if err := waitWindow(ctx, window); err != nil {
			return nil, err
		}
		after, err := collector.Read(ctx)
		if err != nil {
			return nil, err
		}
		return coreTotalUsageItems(before.Cores, after.Cores, time.Now().UTC()), nil
	})
}

func coreUserItems(cores []CoreCounters, observedAt time.Time) []MetricItem {
	timestamp := observedAt.Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, len(cores))
	for _, core := range cores {
		items = append(items, MetricItem{
			MetricID: MetricCoreUser, ObjectID: core.ObjectID,
			Value: counterNanoseconds(core.Counters.UserSeconds), Unit: "ns", ObservedAt: timestamp,
		})
	}
	return items
}

func coreTotalUsageItems(previous, current []CoreCounters, observedAt time.Time) []MetricItem {
	previousByID := make(map[string]Counters, len(previous))
	for _, core := range previous {
		previousByID[core.ObjectID] = core.Counters
	}
	timestamp := observedAt.Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, len(current))
	for _, core := range current {
		before, exists := previousByID[core.ObjectID]
		if !exists {
			continue
		}
		value, ok := CalculateTotalUsage(before, core.Counters)
		if !ok {
			continue
		}
		items = append(items, MetricItem{
			MetricID: MetricCoreTotalUsage, ObjectID: core.ObjectID,
			Value: value, Unit: "percent", ObservedAt: timestamp,
		})
	}
	return items
}
