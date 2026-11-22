package cpu

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strconv"
	"strings"
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
	if len(aggregateStats) > 0 {
		aggregate := countersFromTimes(aggregateStats[0])
		if validationErr := validateCounters(aggregate); validationErr != nil {
			result.AggregateStatus = StatusInvalid
			for _, metricID := range aggregateMetricIDs() {
				result.Issues = append(result.Issues, statusItem(metricID, "", StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now))
			}
		} else {
			result.Counters = aggregate
			if aggregateErr != nil {
				// Keep a valid aggregate row even when the provider also reports
				// an error; consumers receive the value with partial quality.
				result.AggregateStatus = StatusPartial
			} else {
				result.AggregateStatus = StatusValid
			}
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

	if len(perCPUStats) == 0 {
		if perCPUErr == nil {
			perCPUErr = errors.New("per-CPU counters are unavailable")
		}
		status, reason, code := classifySourceError(perCPUErr)
		result.Issues = append(result.Issues,
			statusItem(MetricCoreUser, "", status, reason, code, "gopsutil.cpu.times", now),
			statusItem(MetricCoreTotalUsage, "", status, reason, code, "gopsutil.cpu.times", now),
			statusItem(MetricCoreUsage, "", status, reason, code, "gopsutil.cpu.times", now),
		)
		return result
	}
	if perCPUErr != nil {
		// gopsutil may return usable CPU rows together with a source error.
		// Preserve those rows and mark the per-CPU collection as partial.
		result.Issues = append(result.Issues,
			statusItem(MetricCoreUser, "", StatusPartial, "partial_source", "PARTIAL_RESULT", "gopsutil.cpu.times", now),
			statusItem(MetricCoreTotalUsage, "", StatusPartial, "partial_source", "PARTIAL_RESULT", "gopsutil.cpu.times", now),
			statusItem(MetricCoreUsage, "", StatusPartial, "partial_source", "PARTIAL_RESULT", "gopsutil.cpu.times", now),
		)
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
				statusItem(MetricCoreUsage, objectID, StatusInvalid, "duplicate_object_id", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
			)
			continue
		}
		seen[objectID] = struct{}{}
		counters := countersFromTimes(stat)
		if validationErr := validateCounters(counters); validationErr != nil {
			result.Issues = append(result.Issues,
				statusItem(MetricCoreUser, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
				statusItem(MetricCoreTotalUsage, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
				statusItem(MetricCoreUsage, objectID, StatusInvalid, "invalid_value", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", now),
			)
			continue
		}
		result.Cores = append(result.Cores, CoreCounters{ObjectID: objectID, Counters: counters})
	}
	if len(result.Cores) == 0 && len(result.Issues) == 0 {
		result.Issues = append(result.Issues,
			statusItem(MetricCoreUser, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
			statusItem(MetricCoreTotalUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
			statusItem(MetricCoreUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
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
	return snapshot.AggregateStatus == "" || snapshot.AggregateStatus == StatusValid || snapshot.AggregateStatus == StatusPartial
}

func countersFromTimes(stat cpu.TimesStat) Counters {
	// gopsutil's Total includes guest buckets on every platform. Linux already
	// accounts guest inside user/nice, so subtract those buckets there only.
	total := stat.Total()
	if runtime.GOOS == "linux" {
		total -= stat.Guest
		total -= stat.GuestNice
	}
	return Counters{
		TotalSeconds: total,
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
	maxSeconds := float64(^uint64(0)>>1) / float64(time.Second)
	for _, value := range values {
		if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > maxSeconds {
			return errors.New("CPU counters contain invalid values")
		}
	}
	if c.IdleSeconds+c.IOWaitSeconds > c.TotalSeconds {
		return errors.New("CPU idle time exceeds total time")
	}
	return nil
}

type UsageSample struct {
	Value       float64
	WindowStart time.Time
	WindowEnd   time.Time
	WindowMS    int64
	Status      string
	Reason      string
	ErrorCode   string
}

func (c *SystemCollector) TotalUsage(ctx context.Context, window time.Duration) (float64, error) {
	sample, err := c.TotalUsageSample(ctx, window)
	if err != nil {
		return 0, err
	}
	return sample.Value, nil
}

// TotalUsageSample returns the actual interval represented by the two counter
// snapshots. The requested wait duration is not used as a substitute for the
// observed sampling window.
func (c *SystemCollector) TotalUsageSample(ctx context.Context, window time.Duration) (UsageSample, error) {
	if err := validateWindow(window); err != nil {
		return UsageSample{}, err
	}
	before, err := c.Read(ctx)
	if err != nil {
		return UsageSample{}, err
	}
	if !aggregateAvailable(before) {
		return UsageSample{}, commands.NewClassifiedError("SOURCE_UNAVAILABLE", "source", "aggregate CPU counters are unavailable", true, nil)
	}
	if err := waitWindow(ctx, window); err != nil {
		return UsageSample{}, err
	}
	after, err := c.Read(ctx)
	if err != nil {
		return UsageSample{}, err
	}
	return usageSampleFromSnapshots(before, after)
}

func usageSampleFromSnapshots(before, after Snapshot) (UsageSample, error) {
	if !aggregateAvailable(before) || !aggregateAvailable(after) {
		return UsageSample{}, commands.NewClassifiedError("SOURCE_UNAVAILABLE", "source", "aggregate CPU counters are unavailable", true, nil)
	}
	value, ok := CalculateTotalUsage(before.Counters, after.Counters)
	if !ok {
		return UsageSample{}, commands.NewClassifiedError("SOURCE_INVALID_DATA", "source", "CPU counters did not advance during the measurement window", false, nil)
	}
	windowMS := after.CollectedAt.Sub(before.CollectedAt).Milliseconds()
	if windowMS <= 0 {
		return UsageSample{}, commands.NewClassifiedError("SOURCE_INVALID_DATA", "source", "CPU sample timestamps do not form a positive interval", false, nil)
	}
	sample := UsageSample{
		Value: value, WindowStart: before.CollectedAt,
		WindowEnd: after.CollectedAt, WindowMS: windowMS, Status: StatusValid,
	}
	if before.AggregateStatus == StatusPartial || after.AggregateStatus == StatusPartial {
		sample.Status = StatusPartial
		sample.Reason = "partial_source"
		sample.ErrorCode = "PARTIAL_RESULT"
	}
	return sample, nil
}

func validateWindow(window time.Duration) error {
	if window < minWindow || window > maxWindow {
		return commands.NewClassifiedError(
			"VALIDATION_OUT_OF_RANGE", "validation",
			fmt.Sprintf("window_ms must be between %d and %d", minWindow.Milliseconds(), maxWindow.Milliseconds()),
			false, nil,
		)
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
	// Linux I/O wait is preserved as its own counter but excluded from busy
	// CPU time: the CPU is not executing work while waiting for I/O.
	previousIdle := previous.IdleSeconds + previous.IOWaitSeconds
	currentIdle := current.IdleSeconds + current.IOWaitSeconds
	idleDelta := currentIdle - previousIdle
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
	Value       any    `json:"value,omitempty"`
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

type intervalCommandParams struct {
	SampleInterval json.RawMessage `json:"sample_interval"`
	WindowMS       json.RawMessage `json:"window_ms"`
}

type coreUsageCommandParams struct {
	SampleInterval json.RawMessage `json:"sample_interval"`
	WindowMS       json.RawMessage `json:"window_ms"`
	CPUId          *int64          `json:"cpu_id"`
	CoreID         *int64          `json:"core_id"`
}

func decodeStrictParams(data json.RawMessage, destination any, allowedFields ...string) error {
	if len(data) == 0 {
		return nil
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil || fields == nil {
		return commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "command parameters must be a JSON object", false, err)
	}
	allowed := make(map[string]struct{}, len(allowedFields))
	for _, field := range allowedFields {
		allowed[field] = struct{}{}
	}
	for field := range fields {
		if _, ok := allowed[field]; !ok {
			return commands.NewClassifiedError("VALIDATION_UNKNOWN_PARAMETER", "validation", "command contains an unknown parameter", false, nil)
		}
	}
	if destination != nil {
		if err := json.Unmarshal(data, destination); err != nil {
			return commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "command parameter has an invalid type or format", false, err)
		}
	}
	return nil
}

func rejectUnexpectedParams(data json.RawMessage) error {
	return decodeStrictParams(data, nil)
}

func parseIntervalFields(sampleInterval, windowMS json.RawMessage) (time.Duration, error) {
	if len(sampleInterval) > 0 && len(windowMS) > 0 {
		return 0, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "sample_interval and window_ms cannot be used together", false, nil)
	}
	if len(sampleInterval) == 0 && len(windowMS) == 0 {
		return DefaultWindow, nil
	}
	var duration time.Duration
	if len(sampleInterval) > 0 {
		var textValue string
		if json.Unmarshal(sampleInterval, &textValue) == nil {
			parsed, err := time.ParseDuration(textValue)
			if err != nil {
				return 0, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "sample_interval must be a duration string or milliseconds", false, err)
			}
			duration = parsed
		} else {
			var milliseconds int64
			if err := json.Unmarshal(sampleInterval, &milliseconds); err != nil {
				return 0, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "sample_interval must be a duration string or milliseconds", false, err)
			}
			if milliseconds <= 0 || milliseconds > maxWindow.Milliseconds() {
				return 0, commands.NewClassifiedError("VALIDATION_OUT_OF_RANGE", "validation", "sample_interval is outside the supported range", false, nil)
			}
			duration = time.Duration(milliseconds) * time.Millisecond
		}
	} else {
		var milliseconds int64
		if err := json.Unmarshal(windowMS, &milliseconds); err != nil {
			return 0, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "window_ms must be an integer", false, err)
		}
		if milliseconds <= 0 || milliseconds > maxWindow.Milliseconds() {
			return 0, commands.NewClassifiedError("VALIDATION_OUT_OF_RANGE", "validation", "window_ms is outside the supported range", false, nil)
		}
		duration = time.Duration(milliseconds) * time.Millisecond
	}
	if err := validateWindow(duration); err != nil {
		return 0, err
	}
	return duration, nil
}

func filterCoreUsageItems(ctx context.Context, items []MetricItem, cpuID, coreID *int64) ([]MetricItem, error) {
	if cpuID == nil && coreID == nil {
		return items, nil
	}
	selected := make(map[string]bool)
	requestedObject := ""
	if cpuID != nil {
		requestedObject = fmt.Sprintf("cpu%d", *cpuID)
		selected[requestedObject] = true
	}
	if coreID != nil {
		requestedObject = fmt.Sprintf("core%d", *coreID)
		topology, err := readLinuxTopology(ctx, defaultSysfsCPUPath)
		if err != nil {
			return nil, err
		}
		for _, item := range topology {
			if item.MetricID != MetricTopologyCoreID || item.Status != StatusValid {
				continue
			}
			value, ok := item.Value.(int)
			if !ok || int64(value) != *coreID {
				continue
			}
			selected[item.ObjectID] = true
		}
	}
	filtered := make([]MetricItem, 0)
	for _, item := range items {
		if selected[item.ObjectID] {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) > 0 {
		return filtered, nil
	}
	now := time.Now().UTC()
	return []MetricItem{
		statusItem(MetricCoreTotalUsage, requestedObject, StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
		statusItem(MetricCoreUsage, requestedObject, StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", now),
	}, nil
}

type frequencyCommandParams struct {
	CPUId           *int64 `json:"cpu_id"`
	CoreID          *int64 `json:"core_id"`
	FrequencyDomain string `json:"frequency_domain"`
}

func parseFrequencyCommandParams(data json.RawMessage) (frequencyCommandParams, error) {
	var params frequencyCommandParams
	if err := decodeStrictParams(data, &params, "cpu_id", "core_id", "frequency_domain"); err != nil {
		return params, err
	}
	if params.CPUId != nil && *params.CPUId < 0 || params.CoreID != nil && *params.CoreID < 0 {
		return params, commands.NewClassifiedError("VALIDATION_OUT_OF_RANGE", "validation", "cpu_id and core_id must be non-negative", false, nil)
	}
	if params.CPUId != nil && params.CoreID != nil {
		return params, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "cpu_id and core_id cannot be used together", false, nil)
	}
	if params.FrequencyDomain != "" {
		if !isFrequencyDomainID(params.FrequencyDomain) {
			return params, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "frequency_domain must identify a cpufreq policy", false, nil)
		}
		if params.CPUId != nil || params.CoreID != nil {
			return params, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "frequency_domain cannot be combined with cpu_id or core_id", false, nil)
		}
	}
	return params, nil
}

func filterFrequencyItems(ctx context.Context, items []MetricItem, params frequencyCommandParams) ([]MetricItem, error) {
	if params.CPUId == nil && params.CoreID == nil && params.FrequencyDomain == "" {
		return items, nil
	}
	selectedCPUs := make(map[string]bool)
	selectedPolicies := make(map[string]bool)
	requestedObject := ""
	if params.FrequencyDomain != "" {
		requestedObject = params.FrequencyDomain
		selectedPolicies[params.FrequencyDomain] = true
		for _, cpuID := range cpusForFrequencyPolicy(defaultSysfsCPUPath, params.FrequencyDomain) {
			selectedCPUs[fmt.Sprintf("cpu%d", cpuID)] = true
		}
	} else {
		cpuIDs := make([]int64, 0, 1)
		if params.CPUId != nil {
			cpuIDs = append(cpuIDs, *params.CPUId)
		}
		if params.CoreID != nil {
			requestedObject = fmt.Sprintf("core%d", *params.CoreID)
			topology, err := readLinuxTopology(ctx, defaultSysfsCPUPath)
			if err != nil {
				return nil, err
			}
			for _, item := range topology {
				if item.MetricID != MetricTopologyCoreID || item.Status != StatusValid {
					continue
				}
				coreValue, ok := item.Value.(int)
				if !ok || int64(coreValue) != *params.CoreID {
					continue
				}
				cpuText := strings.TrimPrefix(item.ObjectID, "cpu")
				cpuID, parseErr := strconv.ParseInt(cpuText, 10, 64)
				if parseErr == nil && cpuID >= 0 {
					cpuIDs = append(cpuIDs, cpuID)
				}
			}
		} else {
			requestedObject = fmt.Sprintf("cpu%d", *params.CPUId)
		}
		for _, cpuID := range cpuIDs {
			cpuObjectID := fmt.Sprintf("cpu%d", cpuID)
			selectedCPUs[cpuObjectID] = true
			for _, policyID := range frequencyPoliciesForCPU(defaultSysfsCPUPath, cpuID) {
				selectedPolicies[policyID] = true
			}
		}
	}

	filtered := make([]MetricItem, 0)
	for _, item := range items {
		if selectedPolicies[item.ObjectID] || (item.MetricID == MetricCoreFrequency && selectedCPUs[item.ObjectID]) {
			filtered = append(filtered, item)
		}
	}
	if len(filtered) > 0 {
		return filtered, nil
	}
	now := time.Now().UTC()
	metricIDs := make([]string, 0, len(items))
	seenMetricIDs := make(map[string]bool, len(items))
	for _, item := range items {
		if item.MetricID == "" || seenMetricIDs[item.MetricID] {
			continue
		}
		seenMetricIDs[item.MetricID] = true
		metricIDs = append(metricIDs, item.MetricID)
	}
	if len(metricIDs) == 0 {
		metricIDs = []string{
			MetricFrequencyTarget, MetricFrequencyCurrent, MetricFrequencyMin,
			MetricFrequencyMax, MetricFrequencyBase, MetricFrequencyGovernor,
			MetricCoreFrequency,
		}
	}
	statusItems := make([]MetricItem, 0, len(metricIDs))
	for _, metricID := range metricIDs {
		status, reason, code := StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE"
		foundSourceStatus := false
		hasValidSource := false
		for _, item := range items {
			if item.MetricID != metricID {
				continue
			}
			if item.Status == "" || item.Status == StatusValid {
				hasValidSource = true
				continue
			}
			if !foundSourceStatus {
				status, reason, code = item.Status, item.Reason, item.ErrorCode
				foundSourceStatus = true
			} else if status != item.Status || reason != item.Reason || code != item.ErrorCode {
				status, reason, code = StatusPartial, "partial_source", "PARTIAL_RESULT"
			}
		}
		if hasValidSource {
			status, reason, code = StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE"
		}
		statusItems = append(statusItems, statusItem(metricID, requestedObject, status, reason, code, "linux.host.sysfs.cpufreq", now))
	}
	return statusItems, nil
}

func RegisterCommands(registry *commands.Registry, collector Collector) {
	registry.MustRegister(MetricTotalUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		var params intervalCommandParams
		if err := decodeStrictParams(msg.Data, &params, "sample_interval", "window_ms"); err != nil {
			return nil, err
		}
		window, err := parseIntervalFields(params.SampleInterval, params.WindowMS)
		if err != nil {
			return nil, err
		}
		if sampler, ok := collector.(interface {
			TotalUsageSample(context.Context, time.Duration) (UsageSample, error)
		}); ok {
			sample, err := sampler.TotalUsageSample(ctx, window)
			if err != nil {
				return nil, err
			}
			return metricResult{
				Value: sample.Value, Unit: "percent", ObservedAt: sample.WindowEnd.Format(time.RFC3339Nano),
				Status: sample.Status, Reason: sample.Reason, ErrorCode: sample.ErrorCode,
				SourceID: "gopsutil.cpu.times", WindowMS: sample.WindowMS,
				WindowStart: sample.WindowStart.Format(time.RFC3339Nano),
				WindowEnd: sample.WindowEnd.Format(time.RFC3339Nano),
			}, nil
		}
		// Keep compatibility with third-party Collector implementations. The
		// built-in SystemCollector always provides the measured snapshot window.
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
		registry.MustRegister(commandID, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			if err := rejectUnexpectedParams(msg.Data); err != nil {
				return nil, err
			}
			snapshot, err := collector.Read(ctx)
			if err != nil {
				return nil, err
			}
			if !aggregateAvailable(snapshot) {
				return nil, commands.NewClassifiedError("SOURCE_UNAVAILABLE", "source", "aggregate CPU metric is unavailable", true, nil)
			}
			return metricResult{
				Value: counterNanoseconds(read(snapshot.Counters)), Unit: "ns",
				ObservedAt: time.Now().UTC().Format(time.RFC3339Nano),
				Status: snapshot.AggregateStatus, SourceID: "gopsutil.cpu.times",
				Reason: func() string { if snapshot.AggregateStatus == StatusPartial { return "partial_source" }; return "" }(),
				ErrorCode: func() string { if snapshot.AggregateStatus == StatusPartial { return "PARTIAL_RESULT" }; return "" }(),
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

	registry.MustRegister(MetricCoreUser, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		snapshot, err := collector.Read(ctx)
		if err != nil {
			return nil, err
		}
		items := coreUserItems(snapshot.Cores, time.Now().UTC())
		for _, issue := range snapshot.Issues {
			if issue.MetricID == MetricCoreUser {
				items = append(items, issue)
			}
		}
		return items, nil
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
	registry.MustRegister(MetricTopologyCommand, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readTopology(ctx)
	})
	// D.6 retains the public command alias "system.cpu.info"; topology.* remains
	// the canonical metric namespace used by section 17 and the Metric Registry.
	registry.MustRegister("system.cpu.info", func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readTopology(ctx)
	})

	registerFrequencyCommand := func(metricID string) {
		registry.MustRegister(metricID, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			params, err := parseFrequencyCommandParams(msg.Data)
			if err != nil {
				return nil, err
			}
			items, err := readLinuxFrequency(ctx, defaultSysfsCPUPath)
			if err != nil {
				return nil, err
			}
			items, err = filterFrequencyItems(ctx, items, params)
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
	registry.MustRegister(MetricFrequencyTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		params, err := parseFrequencyCommandParams(msg.Data)
		if err != nil {
			return nil, err
		}
		items, err := readLinuxFrequency(ctx, defaultSysfsCPUPath)
		if err != nil {
			return nil, err
		}
		items, err = filterFrequencyItems(ctx, items, params)
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
				items[i].SourceID = "linux.host.sysfs.cpufreq"
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
		var params coreUsageCommandParams
		if err := decodeStrictParams(msg.Data, &params, "sample_interval", "window_ms", "cpu_id", "core_id"); err != nil {
			return nil, err
		}
		if params.CPUId != nil && *params.CPUId < 0 || params.CoreID != nil && *params.CoreID < 0 {
			return nil, commands.NewClassifiedError("VALIDATION_OUT_OF_RANGE", "validation", "cpu_id and core_id must be non-negative", false, nil)
		}
		if params.CPUId != nil && params.CoreID != nil {
			return nil, commands.NewClassifiedError("VALIDATION_INVALID_FIELD", "validation", "cpu_id and core_id cannot be used together", false, nil)
		}
		window, err := parseIntervalFields(params.SampleInterval, params.WindowMS)
		if err != nil {
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
		observedAt := time.Now().UTC()
		items := coreTotalUsageItems(before.Cores, after.Cores, observedAt)
		windowMS := sampleWindowMilliseconds(before, after)
		for i := range items {
			if items[i].Status == StatusValid {
				if windowMS <= 0 {
					items[i] = statusItem(items[i].MetricID, items[i].ObjectID, StatusInvalid, "invalid_window", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt)
				} else {
					items[i].WindowMS = windowMS
				}
			}
		}
		for _, issue := range after.Issues {
			if issue.MetricID == MetricCoreTotalUsage || issue.MetricID == MetricCoreUsage {
				items = append(items, issue)
			}
		}
		if len(items) == 0 {
			items = []MetricItem{
				statusItem(MetricCoreTotalUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", time.Now().UTC()),
				statusItem(MetricCoreUsage, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", time.Now().UTC()),
			}
		}
		return filterCoreUsageItems(ctx, items, params.CPUId, params.CoreID)
	}
	registry.MustRegister(MetricCoreTotalUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		return readCoreUsage(ctx, msg)
	})
	// Backward-compatible command alias from the D.6 command registry.
	registry.MustRegister(MetricCoreUsage, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
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
	readTemperature := func(ctx context.Context) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadTemperatureMetrics(context.Context) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadTemperatureMetrics(ctx)
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
	readPerf := func(ctx context.Context, target string) ([]MetricItem, error) {
		reader, ok := collector.(interface {
			ReadPerfMetrics(context.Context, ...string) ([]MetricItem, error)
		})
		if !ok {
			return nil, ErrMetricUnsupported
		}
		return reader.ReadPerfMetrics(ctx, target)
	}

	registry.MustRegister(MetricCStateTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readCStates(ctx)
	})
	registry.MustRegister(MetricProcessesTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
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
	registry.MustRegister(MetricThrottlingTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readThrottling(ctx)
	})
	registry.MustRegister(MetricPressureTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readPressure(ctx)
	})
	registry.MustRegister(MetricTemperature, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readTemperature(ctx)
	})
	registry.MustRegister(MetricPerfTarget, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
		if err := rejectUnexpectedParams(msg.Data); err != nil {
			return nil, err
		}
		return readPerf(ctx, MetricPerfTarget)
	})

	for _, metricID := range []string{
		MetricThrottleThermalCoreEvents,
		MetricThrottleThermalCoreDuration,
		MetricThrottleThermalPackageEvents,
		MetricThrottleThermalPackageDuration,
		MetricThrottlePowerCoreEvents,
		MetricThrottlePowerPackageEvents,
		MetricThrottlePowerCoreDuration,
		MetricThrottlePowerPackageDuration,
		MetricThrottleActive,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			if err := rejectUnexpectedParams(msg.Data); err != nil {
				return nil, err
			}
			items, err := readThrottling(ctx)
			if err != nil {
				return nil, err
			}
			filtered := make([]MetricItem, 0)
			for _, item := range items {
				if item.MetricID == id {
					filtered = append(filtered, item)
				}
			}
			if len(filtered) == 0 {
				return nil, ErrMetricUnsupported
			}
			return filtered, nil
		})
	}

	for _, metricID := range []string{
		MetricInterrupts, MetricContextSwitches, MetricForks,
		MetricProcessesRunning, MetricProcessesBlocked, MetricRunQueue,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			if err := rejectUnexpectedParams(msg.Data); err != nil {
				return nil, err
			}
			items, err := readSystemCounters(ctx)
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
		MetricPressureSome, MetricPressureFull,
		MetricPressureSomeTotal, MetricPressureFullTotal,
	} {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			if err := rejectUnexpectedParams(msg.Data); err != nil {
				return nil, err
			}
			items, err := readPressure(ctx)
			if err != nil {
				return nil, err
			}
			filtered := make([]MetricItem, 0, 4)
			for _, item := range items {
				if item.MetricID == id {
					filtered = append(filtered, item)
				}
			}
			if len(filtered) == 0 {
				return nil, ErrMetricUnsupported
			}
			return filtered, nil
		})
	}
	for _, metricID := range allPerfMetricIDs() {
		id := metricID
		registry.MustRegister(id, func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			if err := rejectUnexpectedParams(msg.Data); err != nil {
				return nil, err
			}
			items, err := readPerf(ctx, id)
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
	currentIDs := make(map[string]bool, len(current))
	for _, core := range current {
		currentIDs[core.ObjectID] = true
		before, exists := previousByID[core.ObjectID]
		if !exists {
			items = append(items,
				statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt),
				statusItem(MetricCoreUsage, core.ObjectID, StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt),
			)
			continue
		}
		value, ok := CalculateTotalUsage(before, core.Counters)
		if !ok {
			items = append(items,
				statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt),
				statusItem(MetricCoreUsage, core.ObjectID, StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt),
			)
			continue
		}
		items = append(items,
			MetricItem{
				MetricID: MetricCoreTotalUsage, ObjectID: core.ObjectID,
				Value: value, Unit: "percent", ObservedAt: timestamp,
				Status: StatusValid, SourceID: "gopsutil.cpu.times", WindowMS: EffectiveInterval.Milliseconds(),
			},
			MetricItem{
				MetricID: MetricCoreUsage, ObjectID: core.ObjectID,
				Value: value, Unit: "percent", ObservedAt: timestamp,
				Status: StatusValid, SourceID: "gopsutil.cpu.times", WindowMS: EffectiveInterval.Milliseconds(),
			},
		)
	}
	for _, core := range previous {
		if currentIDs[core.ObjectID] {
			continue
		}
		items = append(items,
			statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", observedAt),
			statusItem(MetricCoreUsage, core.ObjectID, StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", observedAt),
		)
	}
	return items
}
