package cpu

import (
	"context"
	"errors"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
)

type testCollector struct{}

func (testCollector) Read(context.Context) (Snapshot, error) {
	return Snapshot{
		Counters: Counters{TotalSeconds: 10, IdleSeconds: 5, UserSeconds: 3, SystemSeconds: 2},
		Cores: []CoreCounters{
			{ObjectID: "cpu0", Counters: Counters{TotalSeconds: 5, IdleSeconds: 2, UserSeconds: 2}},
			{ObjectID: "cpu1", Counters: Counters{TotalSeconds: 5, IdleSeconds: 3, UserSeconds: 1}},
		},
	}, nil
}
func (testCollector) TotalUsage(context.Context, time.Duration) (float64, error) {
	return 50, nil
}

func TestSubscriptionLifecycleAndDeduplication(t *testing.T) {
	manager := NewManager(testCollector{}, func() string { return "node-test" })
	defer manager.Close()

	deliver := func(context.Context, commands.Outbound) error { return nil }
	first, err := manager.Subscribe(101, Target, EffectiveInterval, deliver)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	duplicate, err := manager.Subscribe(101, Target, EffectiveInterval, deliver)
	if err != nil {
		t.Fatalf("duplicate subscribe: %v", err)
	}
	if first != duplicate {
		t.Fatalf("duplicate subscription ID = %q, want %q", duplicate, first)
	}
	if len(manager.subs) != 1 {
		t.Fatalf("subscriptions = %d, want 1", len(manager.subs))
	}

	manager.Cancel(202, first)
	if len(manager.subs) != 1 {
		t.Fatal("another session cancelled the subscription")
	}
	manager.DetachSession(101)
	if len(manager.subs) != 0 {
		t.Fatalf("subscriptions after detach = %d, want 0", len(manager.subs))
	}
}

func TestSubscriptionValidation(t *testing.T) {
	manager := NewManager(testCollector{}, nil)
	defer manager.Close()
	deliver := func(context.Context, commands.Outbound) error { return nil }

	if _, err := manager.Subscribe(1, "system.cpu.unknown", EffectiveInterval, deliver); !errors.Is(err, ErrInvalidTarget) {
		t.Fatalf("invalid target error = %v", err)
	}
	if _, err := manager.Subscribe(1, Target, 500*time.Millisecond, deliver); !errors.Is(err, ErrInvalidInterval) {
		t.Fatalf("invalid interval error = %v", err)
	}
}

func TestNewCPUTargetsAreAccepted(t *testing.T) {
	manager := NewManager(testCollector{}, nil)
	defer manager.Close()
	deliver := func(context.Context, commands.Outbound) error { return nil }

	targets := []string{
		MetricSoftIRQ, MetricSteal, MetricGuest, MetricGuestNice,
		MetricCoreTotalUsage, MetricCoreUser,
	}
	for _, target := range targets {
		if _, err := manager.Subscribe(1, target, EffectiveInterval, deliver); err != nil {
			t.Errorf("Subscribe(%q): %v", target, err)
		}
	}
}

func TestPerCoreSubscriptionDeliversAllMatchingObjects(t *testing.T) {
	manager := NewManager(testCollector{}, func() string { return "node-test" })
	defer manager.Close()

	var delivered commands.Outbound
	deliver := func(_ context.Context, message commands.Outbound) error {
		delivered = message
		return nil
	}
	if _, err := manager.Subscribe(1, MetricCoreUser, EffectiveInterval, deliver); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	manager.publish(context.Background(), itemsFor(mustRead(t, testCollector{}), nil, time.Now().UTC()))

	payload, ok := delivered.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map", delivered.Payload)
	}
	items, ok := payload["items"].([]MetricItem)
	if !ok {
		t.Fatalf("items type = %T, want []MetricItem", payload["items"])
	}
	if len(items) != 2 {
		t.Fatalf("delivered core items = %d, want 2", len(items))
	}
	if items[0].ObjectID != "cpu0" || items[1].ObjectID != "cpu1" {
		t.Fatalf("unexpected per-core object IDs: %#v", items)
	}
}

func mustRead(t *testing.T, collector Collector) Snapshot {
	t.Helper()
	snapshot, err := collector.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
