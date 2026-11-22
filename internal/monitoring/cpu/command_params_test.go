package cpu

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"HomeCloudBackend/internal/commands"
)

func TestParseIntervalFieldsSupportsDocumentedSampleInterval(t *testing.T) {
	tests := []struct {
		name string
		sample json.RawMessage
		window json.RawMessage
		want time.Duration
	}{
		{name: "duration string", sample: json.RawMessage(`"250ms"`), want: 250 * time.Millisecond},
		{name: "numeric milliseconds", sample: json.RawMessage(`500`), want: 500 * time.Millisecond},
		{name: "legacy window milliseconds", window: json.RawMessage(`1000`), want: time.Second},
		{name: "default", want: DefaultWindow},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseIntervalFields(test.sample, nil, test.window)
			if err != nil || got != test.want {
				t.Fatalf("interval = %s, err=%v; want %s", got, err, test.want)
			}
		})
	}
}

func TestParseIntervalFieldsRejectsInvalidOrConflictingValues(t *testing.T) {
	tests := []struct {
		name string
		sample json.RawMessage
		window json.RawMessage
		code string
	}{
		{name: "too short", sample: json.RawMessage(`20`), code: "VALIDATION_OUT_OF_RANGE"},
		{name: "too long", sample: json.RawMessage(`"30s"`), code: "VALIDATION_OUT_OF_RANGE"},
		{name: "invalid duration", sample: json.RawMessage(`"later"`), code: "VALIDATION_INVALID_FIELD"},
		{name: "both forms", sample: json.RawMessage(`"1s"`), window: json.RawMessage(`1000`), code: "VALIDATION_INVALID_FIELD"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parseIntervalFields(test.sample, nil, test.window)
			var classified *commands.ClassifiedError
			if !errors.As(err, &classified) || classified.Code != test.code {
				t.Fatalf("error = %v, want classified code %s", err, test.code)
			}
		})
	}
}

func TestDecodeStrictParamsRejectsUnknownFields(t *testing.T) {
	var params intervalCommandParams
	err := decodeStrictParams(json.RawMessage(`{"window_ms":1000,"window":2000}`), &params, "sample_interval", "window_ms")
	var classified *commands.ClassifiedError
	if !errors.As(err, &classified) || classified.Code != "VALIDATION_UNKNOWN_PARAMETER" {
		t.Fatalf("error = %v, want VALIDATION_UNKNOWN_PARAMETER", err)
	}
}

func TestFilterCoreUsageItemsByLogicalCPU(t *testing.T) {
	items := []MetricItem{
		{MetricID: MetricCoreTotalUsage, ObjectID: "cpu0", Value: 10},
		{MetricID: MetricCoreUsage, ObjectID: "cpu0", Value: 10},
		{MetricID: MetricCoreTotalUsage, ObjectID: "cpu1", Value: 20},
		{MetricID: MetricCoreUsage, ObjectID: "cpu1", Value: 20},
	}
	cpuID := int64(1)
	filtered, err := filterCoreUsageItems(context.Background(), items, &cpuID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(filtered) != 2 || filtered[0].ObjectID != "cpu1" || filtered[1].ObjectID != "cpu1" {
		t.Fatalf("filtered core items = %#v", filtered)
	}
}
