package cpu

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sort"
	"sync"
	"time"

	"HomeCloudBackend/internal/commands"
)

const (
	EffectiveInterval = time.Second
	maxSubscriptionsPerSession = 32
	maxSubscriptionsTotal = 256

	MetricRunQueue = "system.cpu.runqueue"
	MetricCoreUsage = "system.cpu.core.usage"
	MetricPressureTarget = "system.cpu.pressure"
	MetricPressureSome = "system.cpu.pressure.some"
	MetricPressureFull = "system.cpu.pressure.full"
	MetricPressureSomeTotal = "system.cpu.pressure.some.total"
	MetricPressureFullTotal = "system.cpu.pressure.full.total"
	MetricPerfTarget = "system.cpu.perf"
	MetricPerfCycles = "system.cpu.perf.cycles"
	MetricPerfRefCycles = "system.cpu.perf.ref_cycles"
	MetricPerfInstructions = "system.cpu.perf.instructions"
	MetricPerfIPC = "system.cpu.perf.ipc"
	MetricPerfBranches = "system.cpu.perf.branch_instructions"
	MetricPerfBranchMisses = "system.cpu.perf.branch_misses"
	MetricPerfCacheReferences = "system.cpu.perf.cache_references"
	MetricPerfCacheMisses = "system.cpu.perf.cache_misses"
	MetricPerfBusCycles = "system.cpu.perf.bus_cycles"
	MetricPerfStalledFrontend = "system.cpu.perf.stalled_cycles_frontend"
	MetricPerfStalledBackend = "system.cpu.perf.stalled_cycles_backend"
	MetricPerfBranchMissRate = "system.cpu.perf.branch_miss_rate"
	MetricPerfCacheMissRate = "system.cpu.perf.cache_miss_rate"
	MetricPerfL1DReadAccess = "system.cpu.perf.cache.l1d.read_access"
	MetricPerfL1DReadMiss = "system.cpu.perf.cache.l1d.read_miss"
	MetricPerfL1IReadAccess = "system.cpu.perf.cache.l1i.read_access"
	MetricPerfL1IReadMiss = "system.cpu.perf.cache.l1i.read_miss"
	MetricPerfL2ReadAccess = "system.cpu.perf.cache.l2.read_access"
	MetricPerfL2ReadMiss = "system.cpu.perf.cache.l2.read_miss"
	MetricPerfLLCReadAccess = "system.cpu.perf.cache.llc.read_access"
	MetricPerfLLCReadMiss = "system.cpu.perf.cache.llc.read_miss"
	MetricPerfDTLBReadAccess = "system.cpu.perf.tlb.dtlb.read_access"
	MetricPerfDTLBReadMiss = "system.cpu.perf.tlb.dtlb.read_miss"
	MetricPerfITLBReadAccess = "system.cpu.perf.tlb.itlb.read_access"
	MetricPerfITLBReadMiss = "system.cpu.perf.tlb.itlb.read_miss"
	MetricPerfL1DReadMissRate = "system.cpu.perf.cache.l1d.read_miss_rate"
	MetricPerfL1IReadMissRate = "system.cpu.perf.cache.l1i.read_miss_rate"
	MetricPerfL2ReadMissRate = "system.cpu.perf.cache.l2.read_miss_rate"
	MetricPerfLLCReadMissRate = "system.cpu.perf.cache.llc.read_miss_rate"
	MetricPerfDTLBReadMissRate = "system.cpu.perf.tlb.dtlb.read_miss_rate"
	MetricPerfITLBReadMissRate = "system.cpu.perf.tlb.itlb.read_miss_rate"
)

var (
	ErrInvalidTarget = errors.New("unsupported CPU subscription target")
	ErrInvalidInterval = errors.New("CPU subscription interval must be 1000 ms")
	ErrSubscriptionLimit = errors.New("CPU subscription limit reached")
	ErrManagerClosed = errors.New("CPU subscription manager is closed")
)

type Delivery func(context.Context, commands.Outbound) error

type subscription struct {
	id string
	sessionID int64
	target string
	deliver Delivery
}

type Manager struct {
	mu sync.Mutex
	collector Collector
	nodeID func() string
	subs map[string]subscription
	bySession map[int64]map[string]string
	wake chan struct{}
	cancel context.CancelFunc
	wg sync.WaitGroup
	closed bool
	frequencyFailed bool
}

func NewManager(collector Collector, nodeID func() string) *Manager {
	return &Manager{
		collector: collector,
		nodeID: nodeID,
		subs: make(map[string]subscription),
		bySession: make(map[int64]map[string]string),
		wake: make(chan struct{}, 1),
	}
}

func validTarget(target string) bool {
	switch target {
	case Target, MetricTotalUsage, MetricUser, MetricSystem, MetricNice, MetricIdle,
		MetricIOWait, MetricIRQ, MetricSoftIRQ, MetricSteal, MetricGuest, MetricGuestNice,
		MetricCoreTotalUsage, MetricCoreUser, MetricCoreUsage, MetricFrequencyTarget, MetricCoreFrequency, MetricTemperature,
		MetricFrequencyCurrent, MetricFrequencyMin, MetricFrequencyMax,
		MetricFrequencyBase, MetricFrequencyGovernor,
		MetricCStateTarget, MetricThrottlingTarget, MetricInterrupts, MetricContextSwitches,
		MetricThrottleThermalCoreEvents, MetricThrottleThermalCoreDuration,
		MetricThrottleThermalPackageEvents, MetricThrottleThermalPackageDuration,
		MetricThrottlePowerCoreEvents, MetricThrottlePowerPackageEvents,
		MetricThrottlePowerCoreDuration, MetricThrottlePowerPackageDuration, MetricThrottleActive,
		MetricProcessesTarget, MetricForks, MetricProcessesRunning, MetricProcessesBlocked,
		MetricRunQueue, MetricPressureTarget, MetricPressureSome, MetricPressureFull,
		MetricPressureSomeTotal, MetricPressureFullTotal,
		MetricPerfTarget, MetricPerfCycles, MetricPerfRefCycles, MetricPerfInstructions,
		MetricPerfIPC, MetricPerfBranches, MetricPerfBranchMisses, MetricPerfCacheReferences,
		MetricPerfCacheMisses, MetricPerfBusCycles, MetricPerfStalledFrontend, MetricPerfStalledBackend,
		MetricPerfBranchMissRate, MetricPerfCacheMissRate,
		MetricPerfL1DReadAccess, MetricPerfL1DReadMiss, MetricPerfL1IReadAccess, MetricPerfL1IReadMiss,
		MetricPerfL2ReadAccess, MetricPerfL2ReadMiss, MetricPerfLLCReadAccess, MetricPerfLLCReadMiss,
		MetricPerfDTLBReadAccess, MetricPerfDTLBReadMiss, MetricPerfITLBReadAccess, MetricPerfITLBReadMiss,
		MetricPerfL1DReadMissRate, MetricPerfL1IReadMissRate, MetricPerfL2ReadMissRate,
		MetricPerfLLCReadMissRate, MetricPerfDTLBReadMissRate, MetricPerfITLBReadMissRate:
		return true
	default:
		return strings.HasPrefix(target, MetricCStateTarget+".")
	}
}

// Subscribe deduplicates the same target within one WebSocket session.
func (m *Manager) Subscribe(sessionID int64, target string, interval time.Duration, deliver Delivery) (string, error) {
	if sessionID <= 0 || deliver == nil {
		return "", errors.New("invalid CPU subscription session")
	}
	if !validTarget(target) {
		return "", ErrInvalidTarget
	}
	if interval != EffectiveInterval {
		return "", ErrInvalidInterval
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return "", ErrManagerClosed
	}
	owned := m.bySession[sessionID]
	if existingID, ok := owned[target]; ok {
		return existingID, nil
	}
	if len(owned) >= maxSubscriptionsPerSession || len(m.subs) >= maxSubscriptionsTotal {
		return "", ErrSubscriptionLimit
	}
	id, err := newSubscriptionID()
	if err != nil {
		return "", fmt.Errorf("create subscription ID: %w", err)
	}
	sub := subscription{id: id, sessionID: sessionID, target: target, deliver: deliver}
	m.subs[id] = sub
	if owned == nil {
		owned = make(map[string]string)
		m.bySession[sessionID] = owned
	}
	owned[target] = id
	if m.cancel == nil {
		ctx, cancel := context.WithCancel(context.Background())
		m.cancel = cancel
		m.wg.Add(1)
		go m.run(ctx)
	}
	return id, nil
}

func newSubscriptionID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return "subscription-" + hex.EncodeToString(value[:]), nil
}

// Wake is called after subscription.created has been queued, preserving wire order.
func (m *Manager) Wake() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) Cancel(sessionID int64, subscriptionID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return false
	}
	sub, exists := m.subs[subscriptionID]
	if !exists || sub.sessionID != sessionID {
		return false
	}
	m.removeLocked(sub)
	return true
}

func (m *Manager) DetachSession(sessionID int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range m.bySession[sessionID] {
		if sub, ok := m.subs[id]; ok {
			m.removeLocked(sub)
		}
	}
}

func (m *Manager) removeLocked(sub subscription) {
	delete(m.subs, sub.id)
	owned := m.bySession[sub.sessionID]
	delete(owned, sub.target)
	if len(owned) == 0 {
		delete(m.bySession, sub.sessionID)
	}
}

func (m *Manager) Close() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		m.wg.Wait()
		m.stopPerfReader()
		return
	}
	m.closed = true
	if m.cancel != nil {
		m.cancel()
		m.cancel = nil
	}
	clear(m.subs)
	clear(m.bySession)
	m.mu.Unlock()
	m.wg.Wait()
	m.stopPerfReader()
}

func (m *Manager) hasSubscribers() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.subs) > 0
}

func isFrequencyTarget(target string) bool {
	return target == MetricFrequencyTarget || strings.HasPrefix(target, MetricFrequencyTarget+".")
}

func isCStateTarget(target string) bool {
	return target == MetricCStateTarget || strings.HasPrefix(target, MetricCStateTarget+".")
}

func isThrottlingTarget(target string) bool {
	return target == MetricThrottlingTarget || strings.HasPrefix(target, MetricThrottlingTarget+".")
}

func isPressureTarget(target string) bool {
	return target == MetricPressureTarget || strings.HasPrefix(target, MetricPressureTarget+".")
}

func isPerfTarget(target string) bool {
	return target == MetricPerfTarget || strings.HasPrefix(target, MetricPerfTarget+".")
}

func isTemperatureTarget(target string) bool {
	return target == MetricTemperature || strings.HasPrefix(target, MetricTemperature+".")
}

func isSystemCounterTarget(target string) bool {
	switch target {
	case MetricRunQueue, MetricInterrupts, MetricContextSwitches, MetricForks,
		MetricProcessesRunning, MetricProcessesBlocked, MetricProcessesTarget:
		return true
	default:
		return false
	}
}

func (m *Manager) hasMatchingTarget(match func(string) bool) bool {
	for _, sub := range m.snapshot() {
		if match(sub.target) {
			return true
		}
	}
	return false
}

func (m *Manager) hasCPUSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool {
		return target == Target || (!isFrequencyTarget(target) &&
			!isCStateTarget(target) && !isThrottlingTarget(target) &&
			!isSystemCounterTarget(target) && !isPressureTarget(target) &&
			!isPerfTarget(target) && !isTemperatureTarget(target))
	})
}

func (m *Manager) hasFrequencySubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isFrequencyTarget(target) })
}

func (m *Manager) hasCStateSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isCStateTarget(target) })
}

func (m *Manager) hasThrottlingSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isThrottlingTarget(target) })
}

func (m *Manager) hasSystemCounterSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isSystemCounterTarget(target) })
}

func (m *Manager) hasPressureSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isPressureTarget(target) })
}

func (m *Manager) hasTemperatureSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return target == Target || isTemperatureTarget(target) })
}

func (m *Manager) hasPerfSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool { return isPerfTarget(target) })
}

func (m *Manager) publishFrequency(ctx context.Context) {
	if !m.hasFrequencySubscribers() {
		m.frequencyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadFrequency(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricFrequencyTarget, ErrMetricUnsupported, &m.frequencyFailed)
		return
	}
	items, err := reader.ReadFrequency(ctx)
	if err != nil {
		m.publishExtendedError(ctx, MetricFrequencyTarget, err, &m.frequencyFailed)
		return
	}
	m.frequencyFailed = false
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range items {
		items[i].ObservedAt = now
	}
	m.publish(ctx, items)
}

func (m *Manager) snapshot() []subscription {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]subscription, 0, len(m.subs))
	for _, sub := range m.subs {
		result = append(result, sub)
	}
	return result
}

func (m *Manager) run(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(EffectiveInterval)
	defer ticker.Stop()

	var previous *Snapshot
	lastCollectionFailed := false
	lastCStateFailed := false
	lastThrottlingFailed := false
	lastSystemCountersFailed := false
	lastPressureFailed := false
	lastTemperatureFailed := false
	lastPerfFailed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			// A newly created subscription must receive its own capability error,
			// even when another subscriber already encountered the same failure.
			m.frequencyFailed = false
			lastCStateFailed = false
			lastThrottlingFailed = false
			lastSystemCountersFailed = false
			lastPressureFailed = false
			lastTemperatureFailed = false
			lastPerfFailed = false
			if !m.hasSubscribers() {
				previous = nil
				lastCollectionFailed = false
				m.frequencyFailed = false
				lastCStateFailed = false
				lastThrottlingFailed = false
				lastSystemCountersFailed = false
				lastPressureFailed = false
				lastTemperatureFailed = false
				lastPerfFailed = false
				m.stopPerfReader()
				continue
			}
			if m.hasCPUSubscribers() {
				current, err := m.collector.Read(ctx)
				if err != nil {
					previous = nil
					m.publishCollectionError(ctx, err, &lastCollectionFailed)
				} else {
					lastCollectionFailed = false
					previous = &current
					m.publish(ctx, itemsFor(current, nil, time.Now().UTC()))
				}
			} else {
				previous = nil
				lastCollectionFailed = false
			}
			m.publishFrequency(ctx)
			m.publishCStates(ctx, &lastCStateFailed)
			m.publishThrottling(ctx, &lastThrottlingFailed)
			m.publishSystemCounters(ctx, &lastSystemCountersFailed)
			m.publishPressure(ctx, &lastPressureFailed)
			m.publishTemperature(ctx, &lastTemperatureFailed)
			m.publishPerf(ctx, &lastPerfFailed)
		case <-ticker.C:
			if !m.hasSubscribers() {
				previous = nil
				lastCollectionFailed = false
				m.frequencyFailed = false
				lastCStateFailed = false
				lastThrottlingFailed = false
				lastSystemCountersFailed = false
				lastPressureFailed = false
				lastTemperatureFailed = false
				lastPerfFailed = false
				m.stopPerfReader()
				continue
			}
			if m.hasCPUSubscribers() {
				current, err := m.collector.Read(ctx)
				if err != nil {
					previous = nil
					m.publishCollectionError(ctx, err, &lastCollectionFailed)
				} else {
					lastCollectionFailed = false
					now := time.Now().UTC()
					items := itemsFor(current, previous, now)
					previous = &current
					m.publish(ctx, items)
				}
			} else {
				previous = nil
				lastCollectionFailed = false
			}
			m.publishFrequency(ctx)
			m.publishCStates(ctx, &lastCStateFailed)
			m.publishThrottling(ctx, &lastThrottlingFailed)
			m.publishSystemCounters(ctx, &lastSystemCountersFailed)
			m.publishPressure(ctx, &lastPressureFailed)
			m.publishTemperature(ctx, &lastTemperatureFailed)
			m.publishPerf(ctx, &lastPerfFailed)
		}
	}
}

type MetricItem struct {
	MetricID string `json:"metric_id"`
	ObjectID string `json:"object_id,omitempty"`
	Value any `json:"value,omitempty"`
	Unit string `json:"unit,omitempty"`
	ObservedAt string `json:"observed_at,omitempty"`
	Status string `json:"status,omitempty"`
	Reason string `json:"reason,omitempty"`
	ErrorCode string `json:"error_code,omitempty"`
	SourceID string `json:"source_id,omitempty"`
	WindowMS int64 `json:"window_ms,omitempty"`
}

func itemsFor(current Snapshot, previous *Snapshot, observedAt time.Time) []MetricItem {
	timestamp := observedAt.UTC().Format(time.RFC3339Nano)
	items := make([]MetricItem, 0, 16+len(current.Cores)*3+len(current.Issues))
	if aggregateAvailable(current) {
		c := current.Counters
		qualityStatus := current.AggregateStatus
		if qualityStatus == "" {
			qualityStatus = StatusValid
		}
		qualityReason, qualityCode := "", ""
		if qualityStatus == StatusPartial {
			qualityReason, qualityCode = "partial_source", "PARTIAL_RESULT"
		}
		items = append(items,
			MetricItem{MetricID: MetricUser, Value: counterNanoseconds(c.UserSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricSystem, Value: counterNanoseconds(c.SystemSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricNice, Value: counterNanoseconds(c.NiceSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricIdle, Value: counterNanoseconds(c.IdleSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricIOWait, Value: counterNanoseconds(c.IOWaitSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricIRQ, Value: counterNanoseconds(c.IRQSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricSoftIRQ, Value: counterNanoseconds(c.SoftIRQSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricSteal, Value: counterNanoseconds(c.StealSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricGuest, Value: counterNanoseconds(c.GuestSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
			MetricItem{MetricID: MetricGuestNice, Value: counterNanoseconds(c.GuestNiceSeconds), Unit: "ns", ObservedAt: timestamp, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode},
		)
		if previous != nil && aggregateAvailable(*previous) {
			if value, ok := CalculateTotalUsage(previous.Counters, c); ok {
				windowMS := sampleWindowMilliseconds(*previous, current)
				if windowMS <= 0 {
					items = append(items, statusItem(MetricTotalUsage, "", StatusInvalid, "invalid_window", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt))
				} else {
					qualityStatus := StatusValid
				qualityReason, qualityCode := "", ""
				if current.AggregateStatus == StatusPartial || previous.AggregateStatus == StatusPartial {
					qualityStatus = StatusPartial
					qualityReason, qualityCode = "partial_source", "PARTIAL_RESULT"
				}
				items = append(items, MetricItem{MetricID: MetricTotalUsage, Value: value, Unit: "percent", ObservedAt: timestamp, WindowMS: windowMS, SourceID: "gopsutil.cpu.times", Status: qualityStatus, Reason: qualityReason, ErrorCode: qualityCode})
				}
			} else {
				items = append(items, statusItem(MetricTotalUsage, "", StatusUnavailable, "counter_reset", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt))
			}
		} else {
			items = append(items, statusItem(MetricTotalUsage, "", StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt))
		}
	} else {
		status := current.AggregateStatus
		if status == "" {
			status = StatusUnknown
		}
		if !hasMetricStatus(current.Issues, MetricTotalUsage, "") {
			items = append(items, statusItem(MetricTotalUsage, "", status, "aggregate_source_unavailable", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", observedAt))
		}
	}
	items = append(items, coreUserItems(current.Cores, observedAt)...)
	if previous != nil {
		currentCoreIDs := make(map[string]bool, len(current.Cores))
		for _, core := range current.Cores {
			currentCoreIDs[core.ObjectID] = true
		}
		for _, core := range previous.Cores {
			if !currentCoreIDs[core.ObjectID] && !hasMetricStatus(current.Issues, MetricCoreUser, core.ObjectID) {
				items = append(items, statusItem(MetricCoreUser, core.ObjectID, StatusUnavailable, "source_failure", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times", observedAt))
			}
		}
		coreItems := coreTotalUsageItems(previous.Cores, current.Cores, observedAt)
		windowMS := sampleWindowMilliseconds(*previous, current)
		for i := range coreItems {
			if coreItems[i].Status == StatusValid {
				if windowMS <= 0 {
					coreItems[i] = statusItem(coreItems[i].MetricID, coreItems[i].ObjectID, StatusInvalid, "invalid_window", "SOURCE_INVALID_DATA", "gopsutil.cpu.times", observedAt)
				} else {
					coreItems[i].WindowMS = windowMS
				}
			}
		}
		items = append(items, coreItems...)
	} else {
		for _, core := range current.Cores {
			items = append(items,
				statusItem(MetricCoreTotalUsage, core.ObjectID, StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt),
				statusItem(MetricCoreUsage, core.ObjectID, StatusUnknown, "no_baseline", "", "gopsutil.cpu.times", observedAt),
			)
		}
	}
	for _, issue := range current.Issues {
		if isPerCoreCPUMetric(issue.MetricID) {
			items = upsertCoreStatus(items, issue, true)
		} else {
			items = append(items, issue)
		}
	}
	return applyCoreAvailability(items, current, previous)
}

func applyCoreAvailability(items []MetricItem, current Snapshot, previous *Snapshot) []MetricItem {
	if !current.CoreAvailabilityKnown {
		return items
	}
	currentCores := make(map[string]bool, len(current.Cores))
	for _, core := range current.Cores {
		currentCores[core.ObjectID] = true
		state, exists := current.CoreAvailability[core.ObjectID]
		if !exists || !state.Present || !state.Online {
			status, reason, code, sourceID := coreUnavailableStatus(state, exists)
			for i := range items {
				if isPerCoreCPUMetric(items[i].MetricID) && items[i].ObjectID == core.ObjectID {
					items[i] = statusItem(items[i].MetricID, core.ObjectID, status, reason, code, sourceID, current.CollectedAt)
				}
			}
		}
	}
	// Emit explicit states for present CPUs that are offline or unexpectedly
	// absent from gopsutil's online per-CPU sample, including on first sample.
	availabilityIDs := make([]string, 0, len(current.CoreAvailability))
	for objectID := range current.CoreAvailability {
		availabilityIDs = append(availabilityIDs, objectID)
	}
	sort.Strings(availabilityIDs)
	for _, objectID := range availabilityIDs {
		if currentCores[objectID] {
			continue
		}
		state := current.CoreAvailability[objectID]
		status, reason, code, sourceID := coreUnavailableStatus(state, true)
		for _, metricID := range []string{MetricCoreUser, MetricCoreTotalUsage, MetricCoreUsage} {
			override := reason == "cpu_offline" || reason == "not_present"
			if reason == "source_failure" {
				for _, existing := range items {
					if existing.MetricID == metricID && existing.ObjectID == objectID && existing.Reason == "cpu_offline" {
						override = true
						break
					}
				}
			}
			items = upsertCoreStatus(items, statusItem(metricID, objectID, status, reason, code, sourceID, current.CollectedAt), override)
		}
	}
	if previous == nil {
		return items
	}
	for _, core := range previous.Cores {
		if currentCores[core.ObjectID] {
			continue
		}
		if _, presentInSysfs := current.CoreAvailability[core.ObjectID]; presentInSysfs {
			continue
		}
		status, reason, code, sourceID := coreUnavailableStatus(CoreAvailability{}, false)
		for _, metricID := range []string{MetricCoreUser, MetricCoreTotalUsage, MetricCoreUsage} {
			items = upsertCoreStatus(items, statusItem(metricID, core.ObjectID, status, reason, code, sourceID, current.CollectedAt), true)
		}
	}
	return items
}

func upsertCoreStatus(items []MetricItem, replacement MetricItem, override bool) []MetricItem {
	for i := range items {
		if items[i].MetricID != replacement.MetricID || items[i].ObjectID != replacement.ObjectID {
			continue
		}
		if override {
			items[i] = replacement
		}
		return items
	}
	return append(items, replacement)
}

func isPerCoreCPUMetric(metricID string) bool {
	return metricID == MetricCoreUser || metricID == MetricCoreTotalUsage || metricID == MetricCoreUsage
}

func coreUnavailableStatus(state CoreAvailability, exists bool) (status, reason, code, sourceID string) {
	switch {
	case !exists || !state.Present:
		return StatusUnavailable, "not_present", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu"
	case !state.Online:
		return StatusUnavailable, "cpu_offline", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpu"
	default:
		return StatusUnavailable, "source_failure", "SOURCE_UNAVAILABLE", "gopsutil.cpu.times"
	}
}

func sampleWindowMilliseconds(previous, current Snapshot) int64 {
	if previous.CollectedAt.IsZero() || current.CollectedAt.IsZero() {
		return 0
	}
	return current.CollectedAt.Sub(previous.CollectedAt).Milliseconds()
}

func (m *Manager) publishCStates(ctx context.Context, alreadyFailed *bool) {
	if !m.hasCStateSubscribers() {
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadCStates(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricCStateTarget, ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadCStates(ctx)
	if err != nil {
		m.publishExtendedError(ctx, MetricCStateTarget, err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) publishThrottling(ctx context.Context, alreadyFailed *bool) {
	if !m.hasThrottlingSubscribers() {
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadThrottling(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricThrottlingTarget, ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadThrottling(ctx)
	if err != nil {
		m.publishExtendedError(ctx, MetricThrottlingTarget, err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) publishSystemCounters(ctx context.Context, alreadyFailed *bool) {
	if !m.hasSystemCounterSubscribers() {
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadSystemCounters(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, "system.cpu", ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadSystemCounters(ctx)
	if err != nil {
		m.publishExtendedError(ctx, "system.cpu", err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) publishPressure(ctx context.Context, alreadyFailed *bool) {
	if !m.hasPressureSubscribers() {
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadPressureMetrics(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricPressureTarget, ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadPressureMetrics(ctx)
	if err != nil {
		m.publishExtendedError(ctx, MetricPressureTarget, err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) publishTemperature(ctx context.Context, alreadyFailed *bool) {
	if !m.hasTemperatureSubscribers() {
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadTemperatureMetrics(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricTemperature, ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadTemperatureMetrics(ctx)
	if err != nil {
		m.publishExtendedError(ctx, MetricTemperature, err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) publishPerf(ctx context.Context, alreadyFailed *bool) {
	if !m.hasPerfSubscribers() {
		m.stopPerfReader()
		*alreadyFailed = false
		return
	}
	reader, ok := m.collector.(interface {
		ReadPerfMetrics(context.Context, ...string) ([]MetricItem, error)
	})
	if !ok {
		m.publishExtendedError(ctx, MetricPerfTarget, ErrMetricUnsupported, alreadyFailed)
		return
	}
	items, err := reader.ReadPerfMetrics(ctx, m.perfTargets()...)
	if err != nil {
		m.publishExtendedError(ctx, MetricPerfTarget, err, alreadyFailed)
		return
	}
	*alreadyFailed = false
	m.publish(ctx, items)
}

func (m *Manager) perfTargets() []string {
	targets := make([]string, 0)
	for _, sub := range m.snapshot() {
		if isPerfTarget(sub.target) {
			targets = append(targets, sub.target)
		}
	}
	sort.Strings(targets)
	return targets
}

func (m *Manager) stopPerfReader() {
	if closer, ok := m.collector.(interface{ StopPerfMetrics() }); ok {
		closer.StopPerfMetrics()
	}
}

func (m *Manager) publishExtendedError(ctx context.Context, target string, cause error, alreadyFailed *bool) {
	if *alreadyFailed {
		return
	}
	*alreadyFailed = true
	code, category, retryable := "SOURCE_UNAVAILABLE", "source", true
	if errors.Is(cause, ErrMetricUnsupported) {
		code, category, retryable = "METRIC_UNSUPPORTED", "source", false
	}
	for _, sub := range m.snapshot() {
		matches := false
		switch target {
		case MetricFrequencyTarget, MetricCStateTarget, MetricThrottlingTarget, MetricPressureTarget, MetricTemperature:
			matches = sub.target == Target || sub.target == target || strings.HasPrefix(sub.target, target+".")
		case MetricPerfTarget:
			matches = sub.target == target || strings.HasPrefix(sub.target, target+".")
		case "system.cpu":
			matches = sub.target == Target || isSystemCounterTarget(sub.target)
		}
		if !matches {
			continue
		}
		nodeID := ""
		if m.nodeID != nil {
			nodeID = m.nodeID()
		}
		deliveryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = sub.deliver(deliveryCtx, commands.Outbound{
			V: 1, Type: "subscription.error", NodeID: nodeID,
			Payload: map[string]any{
				"subscription_id": sub.id,
				"target": sub.target,
				"error": map[string]any{
					"code": code, "category": category,
					"retryable": retryable, "message": "CPU metric source is unavailable",
				},
			},
		})
		cancel()
	}
}

func (m *Manager) publish(ctx context.Context, items []MetricItem) {
	if len(items) == 0 {
		return
	}
	for i := range items {
		if items[i].Status == "" {
			items[i].Status = StatusValid
		}
	}
	for _, sub := range m.snapshot() {
		filtered := items
		if sub.target == Target {
			// PMU is an explicit opt-in profile and must never leak into the
			// base CPU group merely because another subscriber enabled it.
			filtered = make([]MetricItem, 0, len(items))
			for _, item := range items {
				if !isPerfTarget(item.MetricID) {
					filtered = append(filtered, item)
				}
			}
		} else {
			filtered = make([]MetricItem, 0, len(items))
			for _, item := range items {
				switch {
				case sub.target == MetricFrequencyTarget:
					if item.MetricID == MetricFrequencyTarget || item.MetricID == MetricCoreFrequency || strings.HasPrefix(item.MetricID, MetricFrequencyTarget+".") {
						filtered = append(filtered, item)
					}
				case sub.target == MetricCStateTarget, sub.target == MetricThrottlingTarget,
					sub.target == MetricProcessesTarget, sub.target == MetricPressureTarget,
					sub.target == MetricPerfTarget:
					if item.MetricID == sub.target || strings.HasPrefix(item.MetricID, sub.target+".") ||
						(sub.target == MetricProcessesTarget && item.MetricID == MetricForks) {
						filtered = append(filtered, item)
					}
				default:
					if item.MetricID == sub.target {
						filtered = append(filtered, item)
					} else if item.Status != "" && item.Value == nil &&
						((item.MetricID == MetricCStateTarget && isCStateTarget(sub.target)) ||
							(item.MetricID == MetricThrottlingTarget && isThrottlingTarget(sub.target))) {
						// A provider-level failure has no discovered child IDs.
						// Fan its explicit status out to the requested child target
						// instead of silently leaving that subscription without data.
						item.MetricID = sub.target
						filtered = append(filtered, item)
					}
				}
			}
		}
		if len(filtered) == 0 {
			if strings.HasPrefix(sub.target, MetricCStateTarget+".") {
				filtered = []MetricItem{
					statusItem(sub.target, "", StatusUnavailable, "not_discovered", "SOURCE_UNAVAILABLE", "linux.host.sysfs.cpuidle", time.Now().UTC()),
				}
			} else {
				continue
			}
		}
		nodeID := ""
		if m.nodeID != nil {
			nodeID = m.nodeID()
		}
		deliveryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = sub.deliver(deliveryCtx, commands.Outbound{
			V: 1, Type: "subscription.update", NodeID: nodeID,
			Payload: map[string]any{"subscription_id": sub.id, "items": filtered},
		})
		cancel()
	}
}

func (m *Manager) publishCollectionError(ctx context.Context, cause error, alreadyFailed *bool) {
	if *alreadyFailed {
		return
	}
	*alreadyFailed = true
	for _, sub := range m.snapshot() {
		if isFrequencyTarget(sub.target) || isCStateTarget(sub.target) ||
			isThrottlingTarget(sub.target) || isSystemCounterTarget(sub.target) ||
			isPressureTarget(sub.target) || isPerfTarget(sub.target) || isTemperatureTarget(sub.target) {
			continue
		}
		nodeID := ""
		if m.nodeID != nil {
			nodeID = m.nodeID()
		}
		deliveryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		_ = sub.deliver(deliveryCtx, commands.Outbound{
			V: 1, Type: "subscription.error", NodeID: nodeID,
			Payload: map[string]any{
				"subscription_id": sub.id,
				"target": sub.target,
				"error": map[string]any{
					"code": "SOURCE_UNAVAILABLE", "category": "source",
					"retryable": true, "message": "CPU metric source is unavailable",
				},
			},
		})
		cancel()
	}
}
