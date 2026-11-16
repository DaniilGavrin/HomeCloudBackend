// Package commands contains the public JSON message contract shared by all
// command domains. It does not depend on WebSocket or any device adapter.
package commands

import "encoding/json"

type Inbound struct {
	V          int             `json:"v"`
	ID         string          `json:"id"`
	Type       string          `json:"type"`
	Action     string          `json:"action,omitempty"`
	Topic      string          `json:"topic,omitempty"`
	DeviceID   string          `json:"device_id,omitempty"`
	ClientType string          `json:"client_type,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

type Outbound struct {
	V      int    `json:"v"`
	ID     string `json:"id,omitempty"`
	Type   string `json:"type"`
	Action string `json:"action,omitempty"`
	Topic  string `json:"topic,omitempty"`
	Error  string `json:"error,omitempty"`
	Data   any    `json:"data,omitempty"`
}

// ClientContext identifies the sender and its per-connection subscriptions.
// DeviceID is a routing label only: it is not an authenticated identity.
type ClientContext struct {
	DeviceID      string
	ClientType    string
	RemoteIP      string // Actual TCP peer; never X-Forwarded-For
	SessionID     int64  // Per-connection identity, for stale unregister protection
	Subscriptions map[string]struct{}
}

func (c ClientContext) Subscribed(topic string) bool {
	_, ok := c.Subscriptions[topic]
	return ok
}
