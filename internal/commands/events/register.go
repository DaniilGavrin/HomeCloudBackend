// Package events exposes user-facing event publication and introspection commands.
// The event transport and subscriptions themselves live in internal/events.
package events

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"HomeCloudBackend/internal/commands"
	eventbus "HomeCloudBackend/internal/events"
)

type Dependencies struct {
	Publish func(context.Context, string, string, any) (eventbus.PublishResult, error)
}

type publishRequest struct {
	Topic   string          `json:"topic"`
	Payload json.RawMessage `json:"payload"`
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("events.subscriptions", func(ctx context.Context, msg commands.Inbound, client commands.ClientContext) (any, error) {
		topics := make([]string, 0, len(client.Subscriptions))
		for topic := range client.Subscriptions {
			topics = append(topics, topic)
		}
		sort.Strings(topics)
		return map[string]any{"topics": topics}, nil
	})
	r.MustRegister("events.publish", func(ctx context.Context, msg commands.Inbound, client commands.ClientContext) (any, error) {
		if d.Publish == nil {
			return nil, errors.New("event publication unavailable")
		}
		var req publishRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			return nil, errors.New("invalid events.publish data")
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(msg.Data, &fields); err != nil || fields == nil {
			return nil, errors.New("events.publish data must be a JSON object")
		}
		for key := range fields {
			if key != "topic" && key != "payload" {
				return nil, fmt.Errorf("unknown events.publish field %q", key)
			}
		}
		if !eventbus.ValidTopic(req.Topic) {
			return nil, eventbus.ErrInvalidTopic
		}
		// Protocol topics are generated only by server-side Go modules.
		if strings.HasPrefix(req.Topic, "clients.") || strings.HasPrefix(req.Topic, "system.") || strings.HasPrefix(req.Topic, "backend.") || strings.HasPrefix(req.Topic, "streams.") || strings.HasPrefix(req.Topic, "cluster.") {
			return nil, errors.New("reserved event topic")
		}
		payload := bytes.TrimSpace(req.Payload)
		if len(payload) == 0 || !json.Valid(payload) {
			return nil, errors.New("payload must be valid JSON")
		}
		// The client cannot spoof "from": it is taken from the established hello.
		return d.Publish(ctx, req.Topic, client.DeviceID, json.RawMessage(payload))
	})
}
