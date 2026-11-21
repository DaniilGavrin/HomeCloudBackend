package cpu

import (
	"context"
	"errors"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
)

type testCollector struct{}

func (testCollector) Read(context.Context) (Counters, error) {
	return Counters{TotalSeconds: 10, IdleSeconds: 5, UserSeconds: 3, SystemSeconds: 2}, nil
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

func TestNewCPUCounterTargetsAreAccepted(t *testing.T) {
	manager := NewManager(testCollector{}, nil)
	defer manager.Close()
	deliver := func(context.Context, commands.Outbound) error { return nil }

	for _, target := range []string{MetricNice, MetricIdle, MetricIOWait, MetricIRQ} {
		if _, err := manager.Subscribe(1, target, EffectiveInterval, deliver); err != nil {
			t.Errorf("Subscribe(%q): %v", target, err)
		}
	}
}
