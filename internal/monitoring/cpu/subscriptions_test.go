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

func (testCollector) ReadFrequency(context.Context) ([]MetricItem, error) {
	return []MetricItem{
		{MetricID: MetricFrequencyCurrent, ObjectID: "policy0", Value: int64(1800000), Unit: "kHz"},
		{MetricID: MetricFrequencyGovernor, ObjectID: "policy0", Value: "performance", Unit: "string"},
	}, nil
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
		MetricCoreTotalUsage, MetricCoreUser, MetricFrequencyTarget, MetricCoreFrequency,
		MetricFrequencyCurrent, MetricFrequencyMin, MetricFrequencyMax,
		MetricFrequencyBase, MetricFrequencyGovernor,
		MetricCStateTarget, "system.cpu.idle.cstate.c1", MetricThrottlingTarget,
		MetricThrottleThermalCoreEvents, MetricThrottleThermalCoreDuration,
		MetricThrottleThermalPackageEvents, MetricThrottleThermalPackageDuration,
		MetricThrottlePowerCoreEvents, MetricThrottlePowerPackageEvents,
		MetricInterrupts, MetricContextSwitches,
		MetricForks, MetricProcessesRunning, MetricProcessesBlocked, MetricProcessesTarget,
		MetricRunQueue, MetricPressureTarget, MetricPressureSome, MetricPressureFull,
		MetricPerfTarget, MetricPerfCycles, MetricPerfInstructions, MetricPerfIPC,
		MetricPerfBranchMissRate, MetricPerfCacheMissRate,
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

func TestFrequencyGroupSubscriptionPublishesAllFrequencyMetrics(t *testing.T) {
	manager := NewManager(testCollector{}, func() string { return "node-test" })
	defer manager.Close()

	var delivered commands.Outbound
	deliver := func(_ context.Context, message commands.Outbound) error {
		delivered = message
		return nil
	}
	if _, err := manager.Subscribe(1, MetricFrequencyTarget, EffectiveInterval, deliver); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	manager.publishFrequency(context.Background())

	payload, ok := delivered.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map", delivered.Payload)
	}
	items, ok := payload["items"].([]MetricItem)
	if !ok || len(items) != 2 {
		t.Fatalf("frequency items = %#v, want 2 metrics", payload["items"])
	}
	if items[0].ObjectID != "policy0" || items[0].ObservedAt == "" {
		t.Fatalf("frequency item missing policy object or timestamp: %#v", items[0])
	}
}

type extendedTestCollector struct{ testCollector }

func (extendedTestCollector) ReadCStates(context.Context) ([]MetricItem, error) {
	return []MetricItem{
		{MetricID: "system.cpu.idle.cstate.c1", ObjectID: "cpu0/state1", Value: int64(123000), Unit: "ns"},
		{MetricID: "system.cpu.idle.cstate.c2", ObjectID: "cpu0/state2", Value: int64(456000), Unit: "ns"},
	}, nil
}

func (extendedTestCollector) ReadThrottling(context.Context) ([]MetricItem, error) {
	return []MetricItem{
		{MetricID: MetricThrottleThermalCoreEvents, ObjectID: "package0/core0", Value: uint64(2), Unit: "count"},
		{MetricID: MetricThrottleThermalCoreDuration, ObjectID: "cpu0", Value: int64(3000000), Unit: "ns"},
		{MetricID: MetricThrottlePowerCoreEvents, ObjectID: "cpu0", Value: uint64(1), Unit: "count"},
	}, nil
}

func (extendedTestCollector) ReadSystemCounters(context.Context) ([]MetricItem, error) {
	return []MetricItem{
		{MetricID: MetricInterrupts, Value: uint64(100), Unit: "count"},
		{MetricID: MetricContextSwitches, Value: uint64(200), Unit: "count"},
		{MetricID: MetricForks, Value: uint64(300), Unit: "count"},
		{MetricID: MetricProcessesRunning, Value: uint64(4), Unit: "count"},
		{MetricID: MetricProcessesBlocked, Value: uint64(2), Unit: "count"},
	}, nil
}

func TestExtendedCPUSubscriptionsPublishAndFilterMetrics(t *testing.T) {
	tests := []struct {
		name string
		target string
		publish func(*Manager, *bool)
		wantMetric string
	}{
		{
			name: "C-state group", target: MetricCStateTarget,
			publish: func(m *Manager, failed *bool) { m.publishCStates(context.Background(), failed) },
			wantMetric: "system.cpu.idle.cstate.c1",
		},
		{
			name: "throttling group", target: MetricThrottlingTarget,
			publish: func(m *Manager, failed *bool) { m.publishThrottling(context.Background(), failed) },
			wantMetric: MetricThrottleThermalCoreEvents,
		},
		{
			name: "interrupts", target: MetricInterrupts,
			publish: func(m *Manager, failed *bool) { m.publishSystemCounters(context.Background(), failed) },
			wantMetric: MetricInterrupts,
		},
		{
			name: "context switches", target: MetricContextSwitches,
			publish: func(m *Manager, failed *bool) { m.publishSystemCounters(context.Background(), failed) },
			wantMetric: MetricContextSwitches,
		},
		{
			name: "forks", target: MetricForks,
			publish: func(m *Manager, failed *bool) { m.publishSystemCounters(context.Background(), failed) },
			wantMetric: MetricForks,
		},
		{
			name: "running processes", target: MetricProcessesRunning,
			publish: func(m *Manager, failed *bool) { m.publishSystemCounters(context.Background(), failed) },
			wantMetric: MetricProcessesRunning,
		},
		{
			name: "blocked processes", target: MetricProcessesBlocked,
			publish: func(m *Manager, failed *bool) { m.publishSystemCounters(context.Background(), failed) },
			wantMetric: MetricProcessesBlocked,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manager := NewManager(extendedTestCollector{}, func() string { return "node-test" })
			defer manager.Close()
			var delivered commands.Outbound
			deliver := func(_ context.Context, message commands.Outbound) error {
				delivered = message
				return nil
			}
			if _, err := manager.Subscribe(1, test.target, EffectiveInterval, deliver); err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			failed := false
			test.publish(manager, &failed)
			if failed {
				t.Fatal("provider unexpectedly marked as failed")
			}
			payload, ok := delivered.Payload.(map[string]any)
			if !ok {
				t.Fatalf("payload type = %T, want map", delivered.Payload)
			}
			items, ok := payload["items"].([]MetricItem)
			if !ok || len(items) == 0 {
				t.Fatalf("published items = %#v", payload["items"])
			}
			foundExpected := false
			for _, item := range items {
				if item.MetricID == test.wantMetric {
					foundExpected = true
				}
				if test.target != MetricCStateTarget && test.target != MetricThrottlingTarget &&
					item.MetricID != test.wantMetric {
					t.Errorf("unexpected metric %q for target %q", item.MetricID, test.target)
				}
			}
			if !foundExpected {
				t.Errorf("expected metric %q was not published", test.wantMetric)
			}
		})
	}
}

func TestProcessGroupSubscriptionPublishesOnlyProcessMetrics(t *testing.T) {
	manager := NewManager(extendedTestCollector{}, func() string { return "node-test" })
	defer manager.Close()

	var delivered commands.Outbound
	deliver := func(_ context.Context, message commands.Outbound) error {
		delivered = message
		return nil
	}
	if _, err := manager.Subscribe(1, MetricProcessesTarget, EffectiveInterval, deliver); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	failed := false
	manager.publishSystemCounters(context.Background(), &failed)
	if failed {
		t.Fatal("process counter provider unexpectedly failed")
	}

	payload, ok := delivered.Payload.(map[string]any)
	if !ok {
		t.Fatalf("payload type = %T, want map", delivered.Payload)
	}
	items, ok := payload["items"].([]MetricItem)
	if !ok || len(items) != 2 {
		t.Fatalf("process group items = %#v, want running and blocked", payload["items"])
	}
	for _, item := range items {
		if item.MetricID != MetricProcessesRunning && item.MetricID != MetricProcessesBlocked {
			t.Errorf("unexpected metric %q in process group", item.MetricID)
		}
	}
}

func TestUnsupportedExtendedProviderReportsCapabilityError(t *testing.T) {
	manager := NewManager(testCollector{}, func() string { return "node-test" })
	defer manager.Close()
	var delivered commands.Outbound
	deliver := func(_ context.Context, message commands.Outbound) error {
		delivered = message
		return nil
	}
	if _, err := manager.Subscribe(1, MetricCStateTarget, EffectiveInterval, deliver); err != nil {
		t.Fatal(err)
	}
	failed := false
	manager.publishCStates(context.Background(), &failed)
	if !failed {
		t.Fatal("expected provider failure state")
	}
	if delivered.Type != "subscription.error" {
		t.Fatalf("message type = %q, want subscription.error", delivered.Type)
	}
	payload := delivered.Payload.(map[string]any)
	errData := payload["error"].(map[string]any)
	if errData["code"] != "METRIC_UNSUPPORTED" || errData["retryable"] != false {
		t.Fatalf("unexpected unsupported error: %#v", errData)
	}
}

func TestIndividualFrequencySubscriptionFiltersMetrics(t *testing.T) {
	manager := NewManager(testCollector{}, func() string { return "node-test" })
	defer manager.Close()

	var delivered commands.Outbound
	deliver := func(_ context.Context, message commands.Outbound) error {
		delivered = message
		return nil
	}
	if _, err := manager.Subscribe(1, MetricFrequencyGovernor, EffectiveInterval, deliver); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	manager.publishFrequency(context.Background())

	payload := delivered.Payload.(map[string]any)
	items := payload["items"].([]MetricItem)
	if len(items) != 1 || items[0].MetricID != MetricFrequencyGovernor {
		t.Fatalf("individual frequency items = %#v, want governor only", items)
	}
}
