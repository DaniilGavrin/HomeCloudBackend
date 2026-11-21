package cpu

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"HomeCloudBackend/internal/commands"
)

const (
	EffectiveInterval = time.Second
	maxSubscriptionsPerSession = 32
	maxSubscriptionsTotal = 256
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
		MetricCoreTotalUsage, MetricCoreUser, MetricFrequencyTarget,
		MetricFrequencyCurrent, MetricFrequencyMin, MetricFrequencyMax,
		MetricFrequencyBase, MetricFrequencyGovernor,
		MetricCStateTarget, MetricThrottlingTarget, MetricInterrupts, MetricContextSwitches,
		MetricThrottleThermalCoreEvents, MetricThrottleThermalCoreDuration,
		MetricThrottleThermalPackageEvents, MetricThrottleThermalPackageDuration:
		return true
	default:
		return strings.HasPrefix(target, MetricCStateTarget+".") ||
			strings.HasPrefix(target, MetricThrottlingTarget+".")
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

func (m *Manager) Cancel(sessionID int64, id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	sub, ok := m.subs[id]
	if !ok || sub.sessionID != sessionID {
		return
	}
	m.removeLocked(sub)
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

func isSystemCounterTarget(target string) bool {
	return target == MetricInterrupts || target == MetricContextSwitches
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
			!isCStateTarget(target) && !isThrottlingTarget(target) && !isSystemCounterTarget(target))
	})
}

func (m *Manager) hasFrequencySubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool {
		return target == Target || isFrequencyTarget(target)
	})
}

func (m *Manager) hasCStateSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool {
		return target == Target || isCStateTarget(target)
	})
}

func (m *Manager) hasThrottlingSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool {
		return target == Target || isThrottlingTarget(target)
	})
}

func (m *Manager) hasSystemCounterSubscribers() bool {
	return m.hasMatchingTarget(func(target string) bool {
		return target == Target || isSystemCounterTarget(target)
	})
}

func (m *Manager) publishFrequency(ctx context.Context) {
	if !m.hasFrequencySubscribers() {
		return
	}
	reader, ok := m.collector.(interface {
		ReadFrequency(context.Context) ([]MetricItem, error)
	})
	if !ok {
		m.publishFrequencyError(ctx, errors.New("CPU frequency provider is unavailable"))
		return
	}
	items, err := reader.ReadFrequency(ctx)
	if err != nil {
		m.publishFrequencyError(ctx, err)
		return
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	for i := range items {
		items[i].ObservedAt = now
	}
	m.publish(ctx, items)
}

func (m *Manager) publishFrequencyError(ctx context.Context, cause error) {
	for _, sub := range m.snapshot() {
		if sub.target != Target && !isFrequencyTarget(sub.target) {
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
					"code": "METRIC_SOURCE_UNAVAILABLE", "category": "source",
					"retryable": true, "message": cause.Error(),
				},
			},
		})
		cancel()
	}
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
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			if !m.hasSubscribers() {
				previous = nil
				lastCollectionFailed = false
				lastCStateFailed = false
				lastThrottlingFailed = false
				lastSystemCountersFailed = false
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
		case <-ticker.C:
			if !m.hasSubscribers() {
				previous = nil
				lastCollectionFailed = false
				lastCStateFailed = false
				lastThrottlingFailed = false
				lastSystemCountersFailed = false
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
		}
	}
}

type MetricItem struct {
	MetricID string `json:"metric_id"`
	ObjectID string `json:"object_id,omitempty"`
	Value any `json:"value"`
	Unit string `json:"unit"`
	ObservedAt string `json:"observed_at"`
}

func itemsFor(current Snapshot, previous *Snapshot, observedAt time.Time) []MetricItem {
	timestamp := observedAt.Format(time.RFC3339Nano)
	c := current.Counters
	items := []MetricItem{
		{MetricID: MetricUser, Value: counterNanoseconds(c.UserSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricSystem, Value: counterNanoseconds(c.SystemSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricNice, Value: counterNanoseconds(c.NiceSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIdle, Value: counterNanoseconds(c.IdleSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIOWait, Value: counterNanoseconds(c.IOWaitSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIRQ, Value: counterNanoseconds(c.IRQSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricSoftIRQ, Value: counterNanoseconds(c.SoftIRQSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricSteal, Value: counterNanoseconds(c.StealSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricGuest, Value: counterNanoseconds(c.GuestSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricGuestNice, Value: counterNanoseconds(c.GuestNiceSeconds), Unit: "ns", ObservedAt: timestamp},
	}
	if previous != nil {
		if value, ok := CalculateTotalUsage(previous.Counters, c); ok {
			items = append([]MetricItem{{MetricID: MetricTotalUsage, Value: value, Unit: "percent", ObservedAt: timestamp}}, items...)
		}
	}
	items = append(items, coreUserItems(current.Cores, observedAt)...)
	if previous != nil {
		items = append(items, coreTotalUsageItems(previous.Cores, current.Cores, observedAt)...)
	}
	return items
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

func (m *Manager) publishExtendedError(ctx context.Context, target string, cause error, alreadyFailed *bool) {
	if *alreadyFailed {
		return
	}
	*alreadyFailed = true
	code, category, retryable := "METRIC_SOURCE_UNAVAILABLE", "source", true
	if errors.Is(cause, ErrMetricUnsupported) {
		code, category, retryable = "METRIC_UNSUPPORTED", "capability", false
	}
	for _, sub := range m.snapshot() {
		matches := false
		switch target {
		case MetricCStateTarget, MetricThrottlingTarget:
			matches = sub.target == target || strings.HasPrefix(sub.target, target+".")
		case "system.cpu":
			matches = isSystemCounterTarget(sub.target)
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
					"retryable": retryable, "message": cause.Error(),
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
	for _, sub := range m.snapshot() {
		filtered := items
		if sub.target != Target {
			filtered = make([]MetricItem, 0, len(items))
			for _, item := range items {
				switch {
				case sub.target == MetricFrequencyTarget:
					if strings.HasPrefix(item.MetricID, MetricFrequencyTarget+".") {
						filtered = append(filtered, item)
					}
				case sub.target == MetricCStateTarget, sub.target == MetricThrottlingTarget:
					if strings.HasPrefix(item.MetricID, sub.target+".") {
						filtered = append(filtered, item)
					}
				default:
					if item.MetricID == sub.target {
						filtered = append(filtered, item)
					}
				}
			}
		}
		if len(filtered) == 0 {
			continue
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
		if isFrequencyTarget(sub.target) {
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
					"code": "METRIC_SOURCE_UNAVAILABLE", "category": "source",
					"retryable": true, "message": cause.Error(),
				},
			},
		})
		cancel()
	}
}
