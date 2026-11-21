package wsserver

import (
	"encoding/json"
	"testing"
)

func TestRedactWSValue(t *testing.T) {
	var message map[string]any
	if err := json.Unmarshal([]byte(`{
		"type":"command.request",
		"payload":{
			"access_token":"do-not-log",
			"nested":[{"password":"also-secret","value":42}],
			"safe":"visible"
		}
	}`), &message); err != nil {
		t.Fatal(err)
	}

	redactWSValue(message)
	payload := message["payload"].(map[string]any)
	if got := payload["access_token"]; got != "[REDACTED]" {
		t.Fatalf("access_token = %v, want redacted", got)
	}
	nested := payload["nested"].([]any)[0].(map[string]any)
	if got := nested["password"]; got != "[REDACTED]" {
		t.Fatalf("password = %v, want redacted", got)
	}
	if got := payload["safe"]; got != "visible" {
		t.Fatalf("safe = %v, want visible", got)
	}
}
