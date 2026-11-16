// Package events implements an in-memory, session-scoped pub/sub broker.
// It knows nothing about WebSocket or transports: delivery is injected by the host.
package events

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"sync"

	"HomeCloudBackend/internal/commands"
)

var topicPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]*(\.[a-z][a-z0-9_-]*)+$`)

const maxTopicsPerSubscriber = 128

var (
	ErrInvalidTopic = errors.New("invalid event topic")
	ErrDetached     = errors.New("subscriber is detached")
	ErrTopicLimit   = errors.New("too many subscriptions")
)

// Delivery must enqueue without waiting for the remote device to process the event.
// A failed enqueue is counted as dropped; one slow client must not block others.
type Delivery func(context.Context, commands.Outbound) error

type Subscriber struct {
	id       uint64
	DeviceID string
	deliver  Delivery
}

type Bus struct {
	mu     sync.RWMutex
	next   uint64
	active map[uint64]*Subscriber
	topics map[string]map[uint64]*Subscriber
	byPeer map[uint64]map[string]struct{}
}

type PublishResult struct {
	Topic       string `json:"topic"`
	Subscribers int    `json:"subscribers"`
	Queued      int    `json:"queued"`
	Dropped     int    `json:"dropped"`
}

func New() *Bus {
	return &Bus{
		active: make(map[uint64]*Subscriber),
		topics: make(map[string]map[uint64]*Subscriber),
		byPeer: make(map[uint64]map[string]struct{}),
	}
}

func ValidTopic(topic string) bool {
	return len(topic) <= 128 && topicPattern.MatchString(topic)
}

// Attach identifies a particular SESSION, not merely a device_id. A previous
// session's cleanup cannot remove subscriptions belonging to a reconnect.
func (b *Bus) Attach(deviceID string, deliver Delivery) *Subscriber {
	if deliver == nil {
		panic("events.Attach: nil delivery")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.next++
	s := &Subscriber{id: b.next, DeviceID: deviceID, deliver: deliver}
	b.active[s.id] = s
	b.byPeer[s.id] = make(map[string]struct{})
	return s
}

func (b *Bus) attached(s *Subscriber) bool {
	return s != nil && b.active[s.id] == s
}

// Subscribe registers exactly one topic, e.g. device.state.changed.
// Wildcard subscriptions are intentionally NOT supported in v1.
// The bool indicates whether a new subscription was created.
func (b *Bus) Subscribe(s *Subscriber, topic string) (bool, error) {
	if !ValidTopic(topic) {
		return false, ErrInvalidTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.attached(s) {
		return false, ErrDetached
	}
	owned := b.byPeer[s.id]
	if _, ok := owned[topic]; ok {
		return false, nil
	}
	if len(owned) >= maxTopicsPerSubscriber {
		return false, ErrTopicLimit
	}
	if b.topics[topic] == nil {
		b.topics[topic] = make(map[uint64]*Subscriber)
	}
	b.topics[topic][s.id] = s
	owned[topic] = struct{}{}
	return true, nil
}

// Unsubscribe is idempotent. The bool indicates whether a subscription existed.
func (b *Bus) Unsubscribe(s *Subscriber, topic string) (bool, error) {
	if !ValidTopic(topic) {
		return false, ErrInvalidTopic
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.attached(s) {
		return false, ErrDetached
	}
	if _, ok := b.byPeer[s.id][topic]; !ok {
		return false, nil
	}
	delete(b.byPeer[s.id], topic)
	delete(b.topics[topic], s.id)
	if len(b.topics[topic]) == 0 {
		delete(b.topics, topic)
	}
	return true, nil
}

func (b *Bus) Detach(s *Subscriber) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.attached(s) {
		return
	}
	for topic := range b.byPeer[s.id] {
		delete(b.topics[topic], s.id)
		if len(b.topics[topic]) == 0 {
			delete(b.topics, topic)
		}
	}
	delete(b.byPeer, s.id)
	delete(b.active, s.id)
}

// Topics returns a sorted copy of subscriptions for the specified session.
func (b *Bus) Topics(s *Subscriber) []string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.attached(s) {
		return nil
	}
	names := make([]string, 0, len(b.byPeer[s.id]))
	for topic := range b.byPeer[s.id] {
		names = append(names, topic)
	}
	sort.Strings(names)
	return names
}

// Has tests a specific session subscription without exposing mutable internal state.
func (b *Bus) Has(s *Subscriber, topic string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if !b.attached(s) {
		return false
	}
	_, exists := b.byPeer[s.id][topic]
	return exists
}

// Publish performs a best-effort fanout to each subscriber's BOUNDED outgoing
// queue. "queued" does not mean written to the socket or processed by a device.
// A delivery failure (including a full queue) is reported as dropped.
func (b *Bus) Publish(ctx context.Context, topic string, from string, payload any) (PublishResult, error) {
	result := PublishResult{Topic: topic}
	if !ValidTopic(topic) {
		return result, ErrInvalidTopic
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	// Validate the payload once rather than repeatedly failing at downstream JSON encoders.
	event := commands.Outbound{
		V: 1, Type: "event", Topic: topic,
		Data: map[string]any{"from": from, "payload": payload},
	}
	b.mu.RLock()
	listeners := make([]*Subscriber, 0, len(b.topics[topic]))
	for _, listener := range b.topics[topic] {
		listeners = append(listeners, listener)
	}
	b.mu.RUnlock()
	result.Subscribers = len(listeners)
	for _, listener := range listeners {
		if err := ctx.Err(); err != nil {
			return result, fmt.Errorf("publish interrupted: %w", err)
		}
		if err := listener.deliver(ctx, event); err != nil {
			result.Dropped++
		} else {
			result.Queued++
		}
	}
	return result, nil
}
