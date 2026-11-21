// Package commands contains the public JSON message contract shared by all
// command domains. It does not depend on WebSocket or any device adapter.
package commands

import "encoding/json"

type Inbound struct {
	V             int             `json:"v"`
	ID            string          `json:"id"`
	Type          string          `json:"type"`
	Action        string          `json:"action,omitempty"`
	Topic         string          `json:"topic,omitempty"`
	DeviceID      string          `json:"device_id,omitempty"`
	ClientType    string          `json:"client_type,omitempty"`
	CorrelationID string          `json:"correlation_id,omitempty"`
	NodeID        string          `json:"node_id,omitempty"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	Data          json.RawMessage `json:"data,omitempty"`
}

type Outbound struct {
	V             int    `json:"v"`
	ID            string `json:"id,omitempty"`
	Type          string `json:"type"`
	Action        string `json:"action,omitempty"`
	Topic         string `json:"topic,omitempty"`
	CorrelationID string `json:"correlation_id,omitempty"`
	NodeID        string `json:"node_id,omitempty"`
	Error         string `json:"error,omitempty"`
	ErrorCode     string `json:"-"`
	ErrorCategory string `json:"-"`
	Retryable     bool   `json:"-"`
	Data          any    `json:"data,omitempty"`
	Payload       any    `json:"payload,omitempty"`
}

// ClientContext identifies the sender and its per-connection subscriptions.
// DeviceID is a routing label only: it is not an authenticated identity.
type ClientContext struct {
	DeviceID       string
	ClientType     string
	RemoteIP       string
	SessionID      int64
	Subscriptions  map[string]struct{}
}

func (c ClientContext) Subscribed(topic string) bool {
	_, ok := c.Subscriptions[topic]
	return ok
}
