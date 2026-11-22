package cpu

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
	"runtime"

	"github.com/shirou/gopsutil/v4/cpu"
)

func TestSnapshotPreservesValidCoresWhenProviderReturnsPartialError(t *testing.T) {
	now := time.Now().UTC()
	aggregate := []cpu.TimesStat{{CPU: "cpu-total", User: 2, System: 1, Idle: 7}}
	perCPU := []cpu.TimesStat{{CPU: "cpu0", User: 1, System: 1, Idle: 3}}
	snapshot := snapshotFromStats(aggregate, nil, perCPU, errors.New("one logical CPU could not be read"), now)
	if len(snapshot.Cores) != 1 || snapshot.Cores[0].ObjectID != "cpu0" {
		t.Fatalf("valid logical CPU sample was discarded: %#v", snapshot)
	}
	foundPartial := false
	for _, issue := range snapshot.Issues {
		if issue.MetricID == MetricCoreUsage && issue.Status == StatusPartial && issue.ErrorCode == "PARTIAL_RESULT" {
			foundPartial = true
		}
	}
	if !foundPartial {
		t.Fatalf("partial per-CPU provider error was not represented: %#v", snapshot.Issues)
	}
}

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

func TestCoreTotalUsageReportsMissingCPUWithoutAssumingOffline(t *testing.T) {
	previous := []CoreCounters{
		{ObjectID: "cpu0", Counters: Counters{TotalSeconds: 10, IdleSeconds: 5}},
		{ObjectID: "cpu1", Counters: Counters{TotalSeconds: 10, IdleSeconds: 5}},
	}
	current := []CoreCounters{
		{ObjectID: "cpu0", Counters: Counters{TotalSeconds: 11, IdleSeconds: 5}},
	}
	items := coreTotalUsageItems(previous, current, time.Now().UTC())
	foundOffline := false
	for _, item := range items {
		if item.ObjectID == "cpu1" && item.MetricID == MetricCoreUsage {
			foundOffline = item.Status == StatusUnavailable && item.Reason == "source_failure"
		}
	}
	if !foundOffline {
		t.Fatalf("missing logical CPU did not receive a source-failure status: %#v", items)
	}
}

func TestCalculateTotalUsageExcludesIOWait(t *testing.T) {
	previous := Counters{TotalSeconds: 100, IdleSeconds: 60, IOWaitSeconds: 10}
	current := Counters{TotalSeconds: 110, IdleSeconds: 65, IOWaitSeconds: 13}
	got, ok := CalculateTotalUsage(previous, current)
	if !ok {
		t.Fatal("expected a valid interval")
	}
	if got != 20 {
		t.Fatalf("usage = %v, want 20; I/O wait is reported separately and excluded from busy time", got)
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

func TestCountersFromTimesUsesPlatformGuestSemantics(t *testing.T) {
	stat := cpu.TimesStat{
		User: 10, System: 2, Nice: 3, Idle: 20, Iowait: 1,
		Irq: 1, Softirq: 1, Steal: 1, Guest: 4, GuestNice: 2,
	}
	got := countersFromTimes(stat)
	want := stat.Total()
	if runtime.GOOS == "linux" {
		want -= stat.Guest + stat.GuestNice
	}
	if got.TotalSeconds != want {
		t.Fatalf("total CPU seconds = %v, want %v for %s", got.TotalSeconds, want, runtime.GOOS)
	}
	if got.GuestSeconds != stat.Guest || got.GuestNiceSeconds != stat.GuestNice {
		t.Fatalf("guest buckets were not preserved: %#v", got)
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
	if len(items) != 11 {
		t.Fatalf("items = %d, want 11 including the explicit no-baseline totalusage state", len(items))
	}
	if items[10].MetricID != MetricTotalUsage || items[10].Status != StatusUnknown || items[10].Reason != "no_baseline" {
		t.Fatalf("first totalusage sample must be unknown/no_baseline: %#v", items[10])
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
	if len(usageItems) != 6 {
		t.Fatalf("core usage items = %d, want canonical and registry aliases for cpu0/cpu1/cpu4", len(usageItems))
	}
	for _, item := range usageItems[:4] {
		if (item.MetricID != MetricCoreTotalUsage && item.MetricID != MetricCoreUsage) || item.Unit != "percent" || item.Status != StatusValid {
			t.Fatalf("unexpected per-core usage item: %#v", item)
		}
		if item.Value != float64(50) {
			t.Fatalf("core %s usage = %v, want 50", item.ObjectID, item.Value)
		}
	}
	for _, item := range usageItems[4:] {
		if item.ObjectID != "cpu4" || item.Status != StatusUnknown || item.Reason != "no_baseline" {
			t.Fatalf("new CPU must report explicit no-baseline state under both IDs: %#v", item)
		}
	}
}


func TestSnapshotFromStatsKeepsAggregateWhenOneCoreIsInvalid(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	aggregate := []cpu.TimesStat{{
		CPU: "cpu-total", User: 20, System: 10, Idle: 60, Iowait: 5,
	}}
	perCPU := []cpu.TimesStat{
		{CPU: "cpu0", User: 10, System: 5, Idle: 30, Iowait: 2},
		{CPU: "cpu1", User: -1, System: 5, Idle: 30},
	}
	snapshot := snapshotFromStats(aggregate, nil, perCPU, nil, now)
	if snapshot.AggregateStatus != StatusValid {
		t.Fatalf("aggregate status = %q, want valid", snapshot.AggregateStatus)
	}
	if len(snapshot.Cores) != 1 || snapshot.Cores[0].ObjectID != "cpu0" {
		t.Fatalf("valid core data was not preserved: %#v", snapshot.Cores)
	}
	if !hasStatusIssue(snapshot.Issues, MetricCoreUser, "cpu1", StatusInvalid) {
		t.Fatalf("invalid core was not reported independently: %#v", snapshot.Issues)
	}
}

func TestSnapshotFromStatsKeepsPerCoreWhenAggregateFails(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	perCPU := []cpu.TimesStat{
		{CPU: "cpu0", User: 10, System: 5, Idle: 30, Iowait: 2},
	}
	snapshot := snapshotFromStats(nil, errors.New("aggregate source failed"), perCPU, nil, now)
	if snapshot.AggregateStatus != StatusSourceError {
		t.Fatalf("aggregate status = %q, want source_error", snapshot.AggregateStatus)
	}
	if len(snapshot.Cores) != 1 || snapshot.Cores[0].ObjectID != "cpu0" {
		t.Fatalf("per-core data was lost after aggregate failure: %#v", snapshot.Cores)
	}
	if !hasStatusIssue(snapshot.Issues, MetricTotalUsage, "", StatusSourceError) {
		t.Fatalf("aggregate failure was not represented in metric status: %#v", snapshot.Issues)
	}
}

func TestSnapshotFromStatsKeepsAggregateWhenPerCoreSourceFails(t *testing.T) {
	now := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	aggregate := []cpu.TimesStat{{CPU: "cpu-total", User: 20, System: 10, Idle: 60, Iowait: 5}}
	snapshot := snapshotFromStats(aggregate, nil, nil, errors.New("per-core source failed"), now)
	if snapshot.AggregateStatus != StatusValid {
		t.Fatalf("aggregate status = %q, want valid", snapshot.AggregateStatus)
	}
	if !hasStatusIssue(snapshot.Issues, MetricCoreUser, "", StatusSourceError) {
		t.Fatalf("per-core source failure was not isolated: %#v", snapshot.Issues)
	}
}

func hasStatusIssue(items []MetricItem, metricID, objectID, status string) bool {
	for _, item := range items {
		if item.MetricID == metricID && item.ObjectID == objectID && item.Status == status {
			return true
		}
	}
	return false
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


func TestUsageSampleFromSnapshotsUsesObservedWindow(t *testing.T) {
	start := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	before := Snapshot{
		CollectedAt: start,
		AggregateStatus: StatusValid,
		Counters: Counters{TotalSeconds: 100, IdleSeconds: 60},
	}
	after := Snapshot{
		CollectedAt: start.Add(1250 * time.Millisecond),
		AggregateStatus: StatusValid,
		Counters: Counters{TotalSeconds: 110, IdleSeconds: 66},
	}

	sample, err := usageSampleFromSnapshots(before, after)
	if err != nil {
		t.Fatal(err)
	}
	if sample.Value != 40 {
		t.Fatalf("usage = %v, want 40", sample.Value)
	}
	if !sample.WindowStart.Equal(before.CollectedAt) || !sample.WindowEnd.Equal(after.CollectedAt) {
		t.Fatalf("sample timestamps = %v..%v, want %v..%v", sample.WindowStart, sample.WindowEnd, before.CollectedAt, after.CollectedAt)
	}
	if sample.WindowMS != 1250 {
		t.Fatalf("window_ms = %d, want actual observed 1250ms", sample.WindowMS)
	}
}

func TestUsageSampleFromSnapshotsRejectsNonPositiveWindow(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	before := Snapshot{CollectedAt: now, AggregateStatus: StatusValid, Counters: Counters{TotalSeconds: 10, IdleSeconds: 5}}
	after := Snapshot{CollectedAt: now, AggregateStatus: StatusValid, Counters: Counters{TotalSeconds: 11, IdleSeconds: 5}}
	if _, err := usageSampleFromSnapshots(before, after); err == nil {
		t.Fatal("expected non-positive observed window to be rejected")
	}
}


func TestSnapshotFromStatsPreservesPartialAggregateRow(t *testing.T) {
	now := time.Date(2026, 11, 22, 10, 0, 0, 0, time.UTC)
	aggregate := []cpu.TimesStat{{
		CPU: "cpu-total", User: 20, System: 10, Idle: 60, Iowait: 5,
	}}
	perCPU := []cpu.TimesStat{{
		CPU: "cpu0", User: 20, System: 10, Idle: 60, Iowait: 5,
	}}
	snapshot := snapshotFromStats(aggregate, errors.New("provider returned partial data"), perCPU, nil, now)
	if snapshot.AggregateStatus != StatusPartial {
		t.Fatalf("aggregate status = %q, want partial", snapshot.AggregateStatus)
	}
	if snapshot.Counters.UserSeconds != 20 {
		t.Fatalf("valid aggregate row was discarded: %#v", snapshot.Counters)
	}
	items := itemsFor(snapshot, nil, now)
	found := false
	for _, item := range items {
		if item.MetricID == MetricUser {
			found = item.Value == int64(20_000_000_000) &&
				item.Status == StatusPartial &&
				item.Reason == "partial_source" &&
				item.ErrorCode == "PARTIAL_RESULT"
		}
	}
	if !found {
		t.Fatalf("aggregate metric must preserve value with partial quality: %#v", items)
	}
}


func TestCalculateTotalUsageRejectsResetInAnyCPUState(t *testing.T) {
	previous := Counters{TotalSeconds: 100, UserSeconds: 50, IdleSeconds: 50}
	current := Counters{TotalSeconds: 110, UserSeconds: 49, SystemSeconds: 11, IdleSeconds: 50}
	if _, ok := CalculateTotalUsage(previous, current); ok {
		t.Fatal("aggregate usage must reject a reset in a component counter even when total time increased")
	}
}


func TestParseIntervalFieldsSupportsDocumentedMilliseconds(t *testing.T) {
	tests := []struct {
		name string
		sampleInterval json.RawMessage
		sampleIntervalMS json.RawMessage
		windowMS json.RawMessage
		want time.Duration
		wantErr bool
	}{
		{name: "canonical sample_interval_ms", sampleIntervalMS: json.RawMessage("500"), want: 500 * time.Millisecond},
		{name: "legacy duration string", sampleInterval: json.RawMessage(`"500ms"`), want: 500 * time.Millisecond},
		{name: "legacy window_ms", windowMS: json.RawMessage("750"), want: 750 * time.Millisecond},
		{name: "conflicting parameters", sampleInterval: json.RawMessage(`"500ms"`), sampleIntervalMS: json.RawMessage("500"), wantErr: true},
		{name: "below minimum", sampleIntervalMS: json.RawMessage("50"), wantErr: true},
		{name: "above maximum", sampleIntervalMS: json.RawMessage("11000"), wantErr: true},
		{name: "milliseconds must be integer", sampleIntervalMS: json.RawMessage(`"500"`), wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseIntervalFields(test.sampleInterval, test.sampleIntervalMS, test.windowMS)
			if (err != nil) != test.wantErr {
				t.Fatalf("parseIntervalFields() error = %v, wantErr=%v", err, test.wantErr)
			}
			if err == nil && got != test.want {
				t.Fatalf("parseIntervalFields() = %s, want %s", got, test.want)
			}
		})
	}
}


func TestCalculateTotalUsageIncludesStealInBusyFraction(t *testing.T) {
	previous := Counters{TotalSeconds: 100, UserSeconds: 25, IdleSeconds: 60, IOWaitSeconds: 10, StealSeconds: 5}
	current := Counters{TotalSeconds: 110, UserSeconds: 25, IdleSeconds: 65, IOWaitSeconds: 12, StealSeconds: 8}
	got, ok := CalculateTotalUsage(previous, current)
	if !ok {
		t.Fatal("expected valid CPU usage sample")
	}
	if got != 30 {
		t.Fatalf("CPU usage = %v, want 30%% with steal counted as busy", got)
	}
}
