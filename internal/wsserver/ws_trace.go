package wsserver

import (
	"encoding/json"
	"strings"
)

var sensitiveWSFields = []string{
	"token", "secret", "password", "authorization", "api_key",
	"apikey", "private_key", "credential",
}

// traceWSMessage logs JSON frames only when logging.log_message_payloads is
// enabled. Sensitive fields are redacted unconditionally.
func (s *Server) traceWSMessage(direction, deviceID, clientType string, raw []byte) {
	if s.cfg == nil || !s.cfg.Logging.LogMessagePayloads || s.log == nil {
		return
	}

	var message map[string]any
	if err := json.Unmarshal(raw, &message); err != nil {
		s.log.Info("WS message", "direction", direction, "device_id", deviceID,
			"client_type", clientType, "message_type", "invalid_json",
			"message", "<invalid-json>")
		return
	}

	redactWSValue(message)
	encoded, err := json.Marshal(message)
	if err != nil {
		return
	}
	messageType, _ := message["type"].(string)
	s.log.Info("WS message", "direction", direction, "device_id", deviceID,
		"client_type", clientType, "message_type", messageType,
		"message", string(encoded))
}

func redactWSValue(value any) {
	switch current := value.(type) {
	case map[string]any:
		for key, child := range current {
			normalized := strings.ToLower(strings.ReplaceAll(key, "-", "_"))
			redact := false
			for _, field := range sensitiveWSFields {
				if strings.Contains(normalized, field) {
					redact = true
					break
				}
			}
			if redact {
				current[key] = "[REDACTED]"
				continue
			}
			redactWSValue(child)
		}
	case []any:
		for _, child := range current {
			redactWSValue(child)
		}
	}
}
