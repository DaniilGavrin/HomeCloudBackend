package cpu

import (
	"testing"
	"time"
)

func TestCalculateTotalUsage(t *testing.T) {
	previous := Counters{TotalSeconds: 100, IdleSeconds: 60}
	current := Counters{TotalSeconds: 110, IdleSeconds: 66}
	got, ok := CalculateTotalUsage(previous, current)
	if !ok {
		t.Fatal("expected a valid interval")
	}
	if got != 40 {
		t.Fatalf("usage = %v, want 40", got)
	}
}

func TestCalculateTotalUsageRejectsInvalidBaseline(t *testing.T) {
	tests := []struct {
		name string
		prev Counters
		curr Counters
	}{
		{name: "no progress", prev: Counters{TotalSeconds: 10, IdleSeconds: 5}, curr: Counters{TotalSeconds: 10, IdleSeconds: 5}},
		{name: "reset", prev: Counters{TotalSeconds: 10, IdleSeconds: 5}, curr: Counters{TotalSeconds: 2, IdleSeconds: 1}},
		{name: "invalid idle delta", prev: Counters{TotalSeconds: 10, IdleSeconds: 5}, curr: Counters{TotalSeconds: 12, IdleSeconds: 8}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, ok := CalculateTotalUsage(test.prev, test.curr); ok {
				t.Fatal("expected invalid interval")
			}
		})
	}
}

func TestItemsForUsesCumulativeNanoseconds(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	items := itemsFor(Counters{UserSeconds: 2.5, SystemSeconds: 1.25, TotalSeconds: 10, IdleSeconds: 5}, nil, now)
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2 without a baseline", len(items))
	}
	if items[0].MetricID != MetricUser || items[0].Value != int64(2500000000) || items[0].Unit != "ns" {
		t.Fatalf("unexpected user metric: %#v", items[0])
	}
	if items[1].MetricID != MetricSystem || items[1].Value != int64(1250000000) || items[1].Unit != "ns" {
		t.Fatalf("unexpected system metric: %#v", items[1])
	}
}
