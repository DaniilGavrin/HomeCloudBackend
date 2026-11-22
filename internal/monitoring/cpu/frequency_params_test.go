package cpu

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
)

func TestParseFrequencyCommandParams(t *testing.T) {
	tests := []struct {
		name string
		raw json.RawMessage
		want frequencyCommandParams
		wantCode string
	}{
		{name: "CPU filter", raw: json.RawMessage(`{"cpu_id":2}`), want: frequencyCommandParams{CPUId: int64Pointer(2)}},
		{name: "core filter", raw: json.RawMessage(`{"core_id":1}`), want: frequencyCommandParams{CoreID: int64Pointer(1)}},
		{name: "frequency domain", raw: json.RawMessage(`{"frequency_domain":"policy0"}`), want: frequencyCommandParams{FrequencyDomain:"policy0"}},
		{name: "invalid domain", raw: json.RawMessage(`{"frequency_domain":"cpu0"}`), wantCode:"VALIDATION_INVALID_FIELD"},
		{name: "conflicting CPU filters", raw: json.RawMessage(`{"cpu_id":0,"core_id":0}`), wantCode:"VALIDATION_INVALID_FIELD"},
		{name: "negative CPU", raw: json.RawMessage(`{"cpu_id":-1}`), wantCode:"VALIDATION_OUT_OF_RANGE"},
		{name: "unknown parameter", raw: json.RawMessage(`{"policy":"policy0"}`), wantCode:"VALIDATION_UNKNOWN_PARAMETER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseFrequencyCommandParams(test.raw)
			if test.wantCode != "" {
				var classified *commands.ClassifiedError
				if !errors.As(err, &classified) || classified.Code != test.wantCode {
					t.Fatalf("error = %v, want code %s", err, test.wantCode)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if (got.CPUId == nil) != (test.want.CPUId == nil) ||
				(got.CoreID == nil) != (test.want.CoreID == nil) ||
				got.FrequencyDomain != test.want.FrequencyDomain {
				t.Fatalf("params = %#v, want %#v", got, test.want)
			}
			if got.CPUId != nil && *got.CPUId != *test.want.CPUId {
				t.Fatalf("cpu_id = %d, want %d", *got.CPUId, *test.want.CPUId)
			}
			if got.CoreID != nil && *got.CoreID != *test.want.CoreID {
				t.Fatalf("core_id = %d, want %d", *got.CoreID, *test.want.CoreID)
			}
		})
	}
}

func TestFilterFrequencyItemsPreservesRequestedMetricStatus(t *testing.T) {
	params := frequencyCommandParams{FrequencyDomain: "policy999999"}
	input := []MetricItem{
		{MetricID: MetricFrequencyCurrent, ObjectID: "policy0", Status: StatusUnsupported},
		{MetricID: MetricCoreFrequency, ObjectID: "policy0", Status: StatusUnsupported},
	}
	filtered, err := filterFrequencyItems(context.Background(), input, params)
	if err != nil {
		t.Fatal(err)
	}
	foundCurrent := false
	for _, item := range filtered {
		if item.MetricID == MetricFrequencyCurrent && item.ObjectID == "policy999999" && item.Status == StatusUnsupported {
			foundCurrent = true
		}
	}
	if !foundCurrent {
		t.Fatalf("filtered response lost unsupported frequency status: %#v", filtered)
	}

	validInput := []MetricItem{{MetricID: MetricFrequencyCurrent, ObjectID: "policy0", Value: uint64(2_000_000), Unit: "Hz", Status: StatusValid}}
	unavailable, err := filterFrequencyItems(context.Background(), validInput, params)
	if err != nil {
		t.Fatal(err)
	}
	foundUnavailable := false
	for _, item := range unavailable {
		if item.MetricID == MetricFrequencyCurrent && item.Status == StatusUnavailable {
			foundUnavailable = true
		}
	}
	if !foundUnavailable {
		t.Fatalf("unmapped frequency domain should be unavailable: %#v", unavailable)
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}
