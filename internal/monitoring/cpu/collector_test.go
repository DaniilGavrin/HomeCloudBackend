package cpu

import (
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
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

func TestCalculateTotalUsageDoesNotDoubleCountGuest(t *testing.T) {
	previous := Counters{TotalSeconds: 100, IdleSeconds: 60, UserSeconds: 40, GuestSeconds: 10}
	current := Counters{TotalSeconds: 110, IdleSeconds: 65, UserSeconds: 45, GuestSeconds: 12}
	got, ok := CalculateTotalUsage(previous, current)
	if !ok {
		t.Fatal("expected a valid interval")
	}
	if got != 50 {
		t.Fatalf("usage = %v, want 50; guest must not be added to total a second time", got)
	}
}

func TestItemsForUsesCumulativeNanosecondsAndExtendedCounters(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	current := Snapshot{Counters: Counters{
		UserSeconds: 2.5, SystemSeconds: 1.25, NiceSeconds: 0.5,
		IdleSeconds: 5, IOWaitSeconds: 0.25, IRQSeconds: 0.125,
		SoftIRQSeconds: 0.0625, StealSeconds: 0.03125,
		GuestSeconds: 0.015625, GuestNiceSeconds: 0.0078125,
		TotalSeconds: 10,
	}}
	items := itemsFor(current, nil, now)
	if len(items) != 10 {
		t.Fatalf("items = %d, want 10 without a baseline", len(items))
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
		{MetricSoftIRQ, 62500000},
		{MetricSteal, 31250000},
		{MetricGuest, 15625000},
		{MetricGuestNice, 7812500},
	}
	for i, want := range expected {
		if items[i].MetricID != want.id || items[i].Value != want.value || items[i].Unit != "ns" {
			t.Errorf("item[%d] = %#v, want metric=%s value=%d unit=ns", i, items[i], want.id, want.value)
		}
	}
}

func TestPerCoreMetricsUseObjectIDAndBaseline(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	previous := Snapshot{Cores: []CoreCounters{
		{ObjectID: "cpu0", Counters: Counters{TotalSeconds: 100, IdleSeconds: 60, UserSeconds: 30}},
		{ObjectID: "cpu2", Counters: Counters{TotalSeconds: 200, IdleSeconds: 100, UserSeconds: 70}},
	}}
	current := Snapshot{Cores: []CoreCounters{
		{ObjectID: "cpu0", Counters: Counters{TotalSeconds: 110, IdleSeconds: 65, UserSeconds: 35}},
		{ObjectID: "cpu2", Counters: Counters{TotalSeconds: 210, IdleSeconds: 105, UserSeconds: 75}},
		{ObjectID: "cpu4", Counters: Counters{TotalSeconds: 10, IdleSeconds: 5, UserSeconds: 2}},
	}}
	userItems := coreUserItems(current.Cores, now)
	if len(userItems) != 3 {
		t.Fatalf("core user items = %d, want 3", len(userItems))
	}
	if userItems[0].ObjectID != "cpu0" || userItems[2].ObjectID != "cpu4" {
		t.Fatalf("unexpected core object IDs: %#v", userItems)
	}

	usageItems := coreTotalUsageItems(previous.Cores, current.Cores, now)
	if len(usageItems) != 2 {
		t.Fatalf("core usage items = %d, want 2 (new cpu4 has no baseline)", len(usageItems))
	}
	for _, item := range usageItems {
		if item.MetricID != MetricCoreTotalUsage || item.Unit != "percent" {
			t.Fatalf("unexpected per-core usage item: %#v", item)
		}
		if item.Value != float64(50) {
			t.Fatalf("core %s usage = %v, want 50", item.ObjectID, item.Value)
		}
	}
}

func TestCountersFromTimesDoesNotDoubleCountGuest(t *testing.T) {
	got := countersFromTimes(cpu.TimesStat{
		CPU: "cpu0", User: 10, System: 5, Nice: 2, Idle: 20,
		Iowait: 3, Irq: 1, Softirq: 1, Steal: 1, Guest: 4, GuestNice: 2,
	})
	const wantTotal = 43.0
	if got.TotalSeconds != wantTotal {
		t.Fatalf("total seconds = %v, want %v (guest buckets are already included in user/nice)", got.TotalSeconds, wantTotal)
	}
	if got.GuestSeconds != 4 || got.GuestNiceSeconds != 2 {
		t.Fatalf("guest counters = (%v, %v), want (4, 2)", got.GuestSeconds, got.GuestNiceSeconds)
	}
}
