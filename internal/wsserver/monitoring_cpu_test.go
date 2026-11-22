package wsserver

import (
	"encoding/json"
	"testing"
)

func TestParseSubscriptionCreatePayloadAcceptsDocumentedForms(t *testing.T) {
	tests := []struct {
		name string
		raw string
		wantTarget string
		wantInterval int64
	}{
		{
			name: "appendix E string target and interval",
			raw: `{"target":"system.cpu","interval_ms":1000}`,
			wantTarget: "system.cpu",
			wantInterval: 1000,
		},
		{
			name: "section 54 group target and delivery interval",
			raw: `{"target":{"group":"system.cpu"},"delivery_interval_ms":1000}`,
			wantTarget: "system.cpu",
			wantInterval: 1000,
		},
		{
			name: "metric target object",
			raw: `{"target":{"metric_id":"system.cpu.totalusage"},"interval_ms":1000}`,
			wantTarget: "system.cpu.totalusage",
			wantInterval: 1000,
		},
		{
			name: "matching interval aliases",
			raw: `{"target":"system.cpu","interval_ms":1000,"delivery_interval_ms":1000}`,
			wantTarget: "system.cpu",
			wantInterval: 1000,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target, interval, err := parseSubscriptionCreatePayload(json.RawMessage(test.raw))
			if err != nil {
				t.Fatalf("parseSubscriptionCreatePayload() error = %v", err)
			}
			if target != test.wantTarget || interval != test.wantInterval {
				t.Fatalf("got target=%q interval=%d, want target=%q interval=%d", target, interval, test.wantTarget, test.wantInterval)
			}
		})
	}
}

func TestParseSubscriptionCreatePayloadRejectsAmbiguousOrInvalidForms(t *testing.T) {
	tests := []struct {
		name string
		raw string
	}{
		{name: "conflicting interval aliases", raw: `{"target":"system.cpu","interval_ms":1000,"delivery_interval_ms":500}`},
		{name: "conflicting target object", raw: `{"target":{"group":"system.cpu","metric_id":"system.cpu.user"},"interval_ms":1000}`},
		{name: "missing interval", raw: `{"target":"system.cpu"}`},
		{name: "zero interval", raw: `{"target":"system.cpu","interval_ms":0}`},
		{name: "negative interval", raw: `{"target":"system.cpu","interval_ms":-1}`},
		{name: "missing target", raw: `{"interval_ms":1000}`},
		{name: "empty target object", raw: `{"target":{},"interval_ms":1000}`},
		{name: "invalid target type", raw: `{"target":42,"interval_ms":1000}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := parseSubscriptionCreatePayload(json.RawMessage(test.raw)); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}


func TestParseMonitoringCommandRequestAcceptsDocumentedEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		message Inbound
		wantCommand string
		wantParams string
	}{
		{
			name: "appendix E payload envelope",
			message: Inbound{Payload: json.RawMessage(`{"command_id":"system.cpu.totalusage","params":{"window_ms":1000}}`)},
			wantCommand: "system.cpu.totalusage",
			wantParams: `{"window_ms":1000}`,
		},
		{
			name: "section 54 top-level envelope",
			message: Inbound{Command: "system.cpu.totalusage", Params: json.RawMessage(`{"sample_interval_ms":1000}`)},
			wantCommand: "system.cpu.totalusage",
			wantParams: `{"sample_interval_ms":1000}`,
		},
		{
			name: "matching mixed command aliases",
			message: Inbound{Command: "system.cpu.totalusage", Payload: json.RawMessage(`{"command_id":"system.cpu.totalusage"}`)},
			wantCommand: "system.cpu.totalusage",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			commandID, params, err := parseMonitoringCommandRequest(test.message)
			if err != nil {
				t.Fatalf("parseMonitoringCommandRequest() error = %v", err)
			}
			if commandID != test.wantCommand || string(params) != test.wantParams {
				t.Fatalf("got command=%q params=%s, want command=%q params=%s", commandID, params, test.wantCommand, test.wantParams)
			}
		})
	}
}

func TestParseMonitoringCommandRequestRejectsAmbiguousEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		message Inbound
	}{
		{
			name: "conflicting command aliases",
			message: Inbound{Command: "system.cpu.user", Payload: json.RawMessage(`{"command_id":"system.cpu.totalusage"}`)},
		},
		{
			name: "conflicting params locations",
			message: Inbound{Command: "system.cpu.totalusage", Params: json.RawMessage(`{"window_ms":1000}`), Payload: json.RawMessage(`{"params":{"window_ms":2000}}`)},
		},
		{name: "missing command", message: Inbound{}},
		{name: "invalid payload", message: Inbound{Payload: json.RawMessage("not-json")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, _, err := parseMonitoringCommandRequest(test.message); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}
