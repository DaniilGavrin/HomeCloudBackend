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
	current := Counters{
		UserSeconds: 2.5, SystemSeconds: 1.25, NiceSeconds: 0.5,
		IdleSeconds: 5, IOWaitSeconds: 0.25, IRQSeconds: 0.125,
		TotalSeconds: 10,
	}
	items := itemsFor(current, nil, now)
	if len(items) != 6 {
		t.Fatalf("items = %d, want 6 without a baseline", len(items))
	}
	expected := []struct {
		id string
		value int64
	}{
		{MetricUser, 2500000000},
		{MetricSystem, 1250000000},
		{MetricNice, 500000000},
		{MetricIdle, 5000000000},
		{MetricIOWait, 250000000},
		{MetricIRQ, 125000000},
	}
	for i, want := range expected {
		if items[i].MetricID != want.id || items[i].Value != want.value || items[i].Unit != "ns" {
			t.Errorf("item[%d] = %#v, want metric=%s value=%d unit=ns", i, items[i], want.id, want.value)
		}
	}
}

func TestItemsForAddsTotalUsageAfterBaseline(t *testing.T) {
	previous := Counters{TotalSeconds: 10, IdleSeconds: 5}
	current := Counters{TotalSeconds: 12, IdleSeconds: 6}
	items := itemsFor(current, &previous, time.Now())
	if len(items) != 7 || items[0].MetricID != MetricTotalUsage {
		t.Fatalf("items = %#v, want totalusage plus six cumulative counters", items)
	}
}
