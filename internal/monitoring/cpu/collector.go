package cpu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
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
	Counters        Counters
	Cores           []CoreCounters
	CollectedAt     time.Time
	AggregateStatus string
	Issues          []MetricItem
}

type Collector interface {
	Read(context.Context) (Snapshot, error)
	TotalUsage(context.Context, time.Duration) (float64, error)
}

type SystemCollector struct {
	perfMu sync.Mutex
	perf   *perfReader
}

func NewSystemCollector() *SystemCollector { return &SystemCollector{} }

func (c *SystemCollector) ReadFrequency(ctx context.Context) ([]MetricItem, error) {
	return readLinuxFrequency(ctx, defaultSysfsCPUPath)
}

// Read samples aggregate and per-logical-CPU counters independently. A failure
// in one source is represented as a metric-level quality item and must not
// discard valid data from the other source.
func (c *SystemCollector) Read(ctx context.Context) (Snapshot, error) {
	aggregateStats, aggregateErr := cpu.TimesWithContext(ctx, false)
	if ctx.Err() != nil {
		return Snapshot{}, ctx.Err()
	}
	perCPUStats, perCPUErr := cpu.TimesWithContext(ctx, true)
	if ctx.Err() != nil {
		return Snapshot{}, ctx.Err()
	}
	return snapshotFromStats(aggregateStats, aggregateErr, perCPUStats, perCPUErr, time.Now().UTC()), nil
}

func snapshotFromStats(aggregateStats []cpu.TimesStat, aggregateErr error, perCPUStats []cpu.TimesStat, perCPUErr error, now time.Time) Snapshot {
	result := Snapshot{
		CollectedAt: now.UTC(),
		AggregateStatus: StatusUnknown,
		Cores: make([]CoreCounters, 0, len(perCPUStats)),
	}
	if aggregateErr == nil && len(aggregateStats) > 0 {
		aggregate := countersFromTimes(aggregateStats[0])
		if validationErr := validateCounters(aggregate); validationErr != nil {
			result.AggregateStatus = StatusInvalid
			for _, metricID := range aggregateMetricIDs() {
				result.Issues = append(result.Issues, statusItem(metricID, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now))
			}
		} else {
			result.Counters = aggregate
			result.AggregateStatus = StatusValid
		}
	} else {
		if aggregateErr == nil {
			aggregateErr = errors.New("aggregate CPU counters are unavailable")
		}
		status, reason, code := classifySourceError(aggregateErr)
		result.AggregateStatus = status
		for _, metricID := range aggregateMetricIDs() {
			result.Issues = append(result.Issues, statusItem(metricID, "", status, reason, code, "gopsutil.cpu.times", now))
		}
	}

	if perCPUErr != nil || len(perCPUStats) == 0 {
		if perCPUErr == nil {
			perCPUErr = errors.New("per-CPU counters are unavailable")
		}
		status, reason, code := classifySourceError(perCPUErr)
		result.Issues = append(result.Issues,
			statusItem(MetricCoreUser, "", status, reason, code, "gopsutil.cpu.times", now),
			statusItem(MetricCoreTotalUsage, "", status, reason, code, "gopsutil.cpu.times", now),
		)
		return result
	}

	seen := make(map[string]struct{}, len(perCPUStats))
	for _, stat := range perCPUStats {
		if stat.CPU == "" || stat.CPU == "cpu-total" {
			continue
		}
		objectID := stat.CPU
		if _, exists := seen[objectID]; exists {
			result.Issues = append(result.Issues,
				statusItem(MetricCoreUser, objectID, StatusInvalid, "duplicate_object_id", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
				statusItem(MetricCoreTotalUsage, objectID, StatusInvalid, "duplicate_object_id", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
			)
			continue
		}
		seen[objectID] = struct{}{}
		counters := countersFromTimes(stat)
		if validationErr := validateCounters(counters); validationErr != nil {
			result.Issues = append(result.Issues,
				statusItem(MetricCoreUser, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
				statusItem(MetricCoreTotalUsage, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
			)
			continue
		}
		result.Cores = append(result.Cores, CoreCounters{ObjectID: objectID, Counters: counters})
	}
	if len(result.Cores) == 0 && len(result.Issues) == 0 {
		result.Issues = append(result.Issues,
			statusItem(MetricCoreUser, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
			statusItem(MetricCoreTotalUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
		)
	}
	return result
}

func aggregateMetricIDs() []string {
	return []string{
		MetricTotalUsage, MetricUser, MetricSystem, MetricNice, MetricIdle,
		MetricIOWait, MetricIRQ, MetricSoftIRQ, MetricSteal, MetricGuest, MetricGuestNice,
	}
}

func aggregateAvailable(snapshot Snapshot) bool {
	return snapshot.AggregateStatus == "" || snapshot.AggregateStatus == StatusValid
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
	if !aggregateAvailable(before) {
		return 0, errors.New("aggregate CPU counters are unavailable")
	}
	if err := waitWindow(ctx, window); err != nil {
		return 0, err
	}
	after, err := c.Read(ctx)
	if err != nil {
		return 0, err
	}
	if !aggregateAvailable(after) {
		return 0, errors.New("aggregate CPU counters are unavailable")
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
	if value < 0 || value > 100 {
		return 0, false
	}
	return value, true
}

func counterNanoseconds(seconds float64) int64 {
	if seconds <= 0 {
		return 0
	}
	return int64(math.Round(seconds * float64(time.Second)))
}

type metricResult struct {
	Value       any    `json:"value"`
	Unit        string `json:"unit"`
	ObservedAt  string `json:"observed_at"`
	Status      string `json:"status,omitempty"`
	Reason      string `json:"reason,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	SourceID    string `json:"source_id,omitempty"`
	WindowMS    int64  `json:"window_ms,omitempty"`
	WindowStart string `json:"window_start,omitempty"`
	WindowEnd   string `json:"window_end,omitempty"`
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
		windowStart := time.Now().UTC()
		value, err := collector.TotalUsage(ctx, window)
		if err != nil {
			return nil, err
		}
		windowEnd := time.Now().UTC()
		return metricResult{
			Value: value, Unit: "percent", ObservedAt: windowEnd.Format(time.RFC3339Nano),
			Status: StatusValid, SourceID: "gopsutil.cpu.times", WindowMS: window.Milliseconds(),
			WindowStart: windowStart.Format(time.RFC3339Nano), WindowEnd: windowEnd.Format(time.RFC3339Nano),
		}, nil
	})

	registerCounter := func(commandID string, read func(Counters) float64) {
		registry.MustRegister(commandID, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			snapshot, err := collector.Read(ctx)
			if err != nil {
				return nil, err
			}
			if !aggregateAvailable(snapshot) {
				return nil, fmt.Errorf("aggregate CPU metric unavailable: %s", snapshot.AggregateStatus)
			}
			return metricResult{
				Value: counterNanoseconds(read(snapshot.Counters)), Unit: "ns",
				ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
				Status: StatusValid, SourceID: "gopsutil.cpu.times",
			}, nil
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
	readTopology := func(ctx context.Context) ([]MetricItem, error) {
		items, err := readLinuxTopology(ctx, defaultSysfsCPUPath)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for i := range items {
			items[i].ObservedAt = now
			if items[i].Status == "" {
				items[i].Status = StatusValid
			}
			if items[i].SourceID == "" {
				items[i].SourceID = "linux.sysfs.cpu"
			}
		}
		return items, nil
	}
	registry.MustRegister(MetricTopologyCommand, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readTopology(ctx)
	})
	// D.6 retains the public command alias "system.cpu.info"; topology.* remains
	// the canonical metric namespace used by section 17 and the Metric Registry.
	registry.MustRegister("system.cpu.info", func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readTopology(ctx)
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
	registry.MustRegister(MetricFrequencyTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		items, err := readLinuxFrequency(ctx, defaultSysfsCPUPath)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC().Format(time.RFC3339Nano)
		for i := range items {
			items[i].ObservedAt = now
			if items[i].Status == "" {
				items[i].Status = StatusValid
			}
			if items[i].SourceID == "" {
				items[i].SourceID = "linux.sysfs.cpufreq"
			}
		}
		return items, nil
	})
	registerFrequencyCommand(MetricFrequencyCurrent)
	registerFrequencyCommand(MetricFrequencyMin)
	registerFrequencyCommand(MetricFrequencyMax)
	registerFrequencyCommand(MetricFrequencyBase)
	registerFrequencyCommand(MetricFrequencyGovernor)
	registerFrequencyCommand(MetricCoreFrequency)

	readCoreUsage := func(ctx context.Context, msg commands.Inbound) ([]MetricItem, error) {
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
		items := coreTotalUsageItems(before.Cores, after.Cores, time.Now().UTC())
		if len(items) == 0 {
			return []MetricItem{statusItem(MetricCoreTotalUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", time.Now().UTC())}, nil
		}
		return items, nil
	}
	registry.MustRegister(MetricCoreTotalUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		return readCoreUsage(ctx, msg)
	})
	// Backward-compatible command alias from the D.6 command registry.
	registry.MustRegister("system.cpu.core.usage", func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		return readCoreUsage(ctx, msg)
	})
	registerExtendedCommands(registry, collector)
}

func metricResultFromItem(item MetricItem) metricResult {
	return metricResult{
		Value: item.Value, Unit: item.Unit, ObservedAt: item.ObservedAt,
		Status: item.Status, Reason: item.Reason, ErrorCode: item.ErrorCode,
		SourceID: item.SourceID, WindowMS: item.WindowMS,
	}
}

func registerExtendedCommands(registry *commands.Registry, collector Collector) {
	readCStates := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadCStates(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadCStates(ctx)
	}
	readThrottling := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadThrottling(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadThrottling(ctx)
	}
	readSystemCounters := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadSystemCounters(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadSystemCounters(ctx)
	}
	readPressure := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadPressureMetrics(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadPressureMetrics(ctx)
	}
	readPerf := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadPerfMetrics(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadPerfMetrics(ctx)
	}

	registry.MustRegister(MetricCStateTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readCStates(ctx)
	})
	registry.MustRegister(MetricProcessesTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		items, err := readSystemCounters(ctx)
		if err != nil {
			return nil, err
		}
		filtered := make([]MetricItem, 0, 3)
		for _, item := range items {
			if item.MetricID == MetricForks || item.MetricID == MetricProcessesRunning || item.MetricID == MetricProcessesBlocked {
				filtered = append(filtered, item)
			}
		}
		if len(filtered) == 0 {
			return nil, ErrMetricUnsupported
		}
		return filtered, nil
	})
	registry.MustRegister(MetricThrottlingTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readThrottling(ctx)
	})
	registry.MustRegister(MetricPressureTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readPressure(ctx)
	})
	registry.MustRegister(MetricPerfTarget, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
		return readPerf(ctx)
	})

	for _, metricID := range []string{
		MetricThrottleThermalCoreEvents,
		MetricThrottleThermalCoreDuration,
		MetricThrottleThermalPackageEvents,
		MetricThrottleThermalPackageDuration,
		MetricThrottlePowerCoreEvents,
		MetricThrottlePowerPackageEvents,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			items, err := readThrottling(ctx)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				if item.MetricID == id {
					return metricResult{Value: item.Value, Unit: item.Unit, ObservedAt: item.ObservedAt}, nil
				}
			}
			return nil, ErrMetricUnsupported
		})
	}

	for _, metricID := range []string{
		MetricInterrupts, MetricContextSwitches, MetricForks,
		MetricProcessesRunning, MetricProcessesBlocked, MetricRunQueue,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			items, err := readSystemCounters(ctx)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				if item.MetricID == id {
					return metricResult{Value: item.Value, Unit: item.Unit, ObservedAt: item.ObservedAt}, nil
				}
			}
			return nil, ErrMetricUnsupported
		})
	}
	for _, metricID := range []string{MetricPressureSome, MetricPressureFull} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			items, err := readPressure(ctx)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				if item.MetricID == id {
					return metricResultFromItem(item), nil
				}
			}
			return nil, ErrMetricUnsupported
		})
	}
	for _, metricID := range []string{
		MetricPerfCycles, MetricPerfRefCycles, MetricPerfInstructions, MetricPerfIPC,
		MetricPerfBranches, MetricPerfBranchMisses, MetricPerfCacheReferences,
		MetricPerfCacheMisses, MetricPerfBusCycles, MetricPerfStalledFrontend,
		MetricPerfStalledBackend, MetricPerfBranchMissRate, MetricPerfCacheMissRate,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			items, err := readPerf(ctx)
			if err != nil {
				return nil, err
			}
			for _, item := range items {
				if item.MetricID == id {
					return metricResultFromItem(item), nil
				}
			}
			return nil, ErrMetricUnsupported
		})
	}
}

func coreUserItems(cores []CoreCounters, observedAt time.Time) []MetricItem {
	timestamp := observedAt.UTC().Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, len(cores))
	for _, core := range cores {
		items = append(items, MetricItem{
			MetricID: MetricCoreUser, ObjectID: core.ObjectID,
			Value: counterNanoseconds(core.Counters.UserSeconds), Unit: "ns", ObservedAt: timestamp,
			Status: StatusValid, SourceID: "gopsutil.cpu.times",
		})
	}
	return items
}

func coreTotalUsageItems(previous, current []CoreCounters, observedAt time.Time) []MetricItem {
	previousByID := make(map[string]Counters, len(previous))
	for _, core := range previous {
		previousByID[core.ObjectID] = core.Counters
	}
	timestamp := observedAt.UTC().Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, len(current))
	for _, core := range current {
		before, exists := previousByID[core.ObjectID]
		if !exists {
			items = append(items, statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt))
			continue
		}
		value, ok := CalculateTotalUsage(before, core.Counters)
		if !ok {
			items = append(items, statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt))
			continue
		}
		items = append(items, MetricItem{
			MetricID: MetricCoreTotalUsage, ObjectID: core.ObjectID,
			Value: value, Unit: "percent", ObservedAt: timestamp,
			Status: StatusValid, SourceID: "gopsutil.cpu.times", WindowMS: EffectiveInterval.Milliseconds(),
		})
	}
	return items
}
