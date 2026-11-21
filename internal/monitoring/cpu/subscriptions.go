package cpu

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
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
	case Target, MetricTotalUsage, MetricUser, MetricSystem, MetricNice, MetricIdle, MetricIOWait, MetricIRQ:
		return true
	default:
		return false
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

	var previous *Counters
	lastCollectionFailed := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
			if !m.hasSubscribers() {
				previous = nil
				continue
			}
			current, err := m.collector.Read(ctx)
			if err != nil {
				m.publishCollectionError(ctx, err, &lastCollectionFailed)
				continue
			}
			lastCollectionFailed = false
			previous = &current
			m.publish(ctx, itemsFor(current, nil, time.Now().UTC()))
		case <-ticker.C:
			if !m.hasSubscribers() {
				previous = nil
				lastCollectionFailed = false
				continue
			}
			current, err := m.collector.Read(ctx)
			if err != nil {
				previous = nil
				m.publishCollectionError(ctx, err, &lastCollectionFailed)
				continue
			}
			lastCollectionFailed = false
			now := time.Now().UTC()
			items := itemsFor(current, previous, now)
			previous = &current
			m.publish(ctx, items)
		}
	}
}

type MetricItem struct {
	MetricID string `json:"metric_id"`
	Value any `json:"value"`
	Unit string `json:"unit"`
	ObservedAt string `json:"observed_at"`
}

func itemsFor(current Counters, previous *Counters, observedAt time.Time) []MetricItem {
	timestamp := observedAt.Format(time.RFC3339Nano)
	items := []MetricItem{
		{MetricID: MetricUser, Value: counterNanoseconds(current.UserSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricSystem, Value: counterNanoseconds(current.SystemSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricNice, Value: counterNanoseconds(current.NiceSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIdle, Value: counterNanoseconds(current.IdleSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIOWait, Value: counterNanoseconds(current.IOWaitSeconds), Unit: "ns", ObservedAt: timestamp},
		{MetricID: MetricIRQ, Value: counterNanoseconds(current.IRQSeconds), Unit: "ns", ObservedAt: timestamp},
	}
	if previous != nil {
		if value, ok := CalculateTotalUsage(*previous, current); ok {
			items = append([]MetricItem{{MetricID: MetricTotalUsage, Value: value, Unit: "percent", ObservedAt: timestamp}}, items...)
		}
	}
	return items
}

func (m *Manager) publish(ctx context.Context, items []MetricItem) {
	if len(items) == 0 {
		return
	}
	for _, sub := range m.snapshot() {
		filtered := items
		if sub.target != Target {
			filtered = make([]MetricItem, 0, 1)
			for _, item := range items {
				if item.MetricID == sub.target {
					filtered = append(filtered, item)
					break
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
