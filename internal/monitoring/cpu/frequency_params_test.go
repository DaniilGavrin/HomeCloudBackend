package cpu

import (
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
		{name: "CPU filter", raw: json.RawMessage(`{"cpu_id":2}`), want: frequencyCommandParams{CPUId: int64PtrForFrequencyTest(2)}},
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

func int64Pointer(value int64) *int64 {
	return &value
}
