package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/connections"
	"HomeCloudBackend/internal/events"
	cpumonitor "HomeCloudBackend/internal/monitoring/cpu"
)

func (s *Server) monitoringNodeError(nodeID string) error {
	if nodeID == "" {
		return commands.NewClassifiedError("VALIDATION_MISSING_FIELD", "validation", "node_id is required", false, nil)
	}
	if s.localNodeID == "" {
		return commands.NewClassifiedError("NODE_UNAVAILABLE", "routing", "local node identity is unavailable", true, nil)
	}
	if nodeID != s.localNodeID {
		return commands.NewClassifiedError("NODE_NOT_FOUND", "routing", "requested node_id is not available in this Backend context", false, nil)
	}
	return nil
}

func (s *Server) monitoringError(messageType, correlationID, code, category, message string, retryable bool) Outbound {
	return Outbound{
		V: 1, Type: messageType, CorrelationID: correlationID, NodeID: s.localNodeID,
		Payload: map[string]any{
			"error": map[string]any{
				"code": code, "category": category, "retryable": retryable, "message": message,
			},
		},
	}
}

func (s *Server) commandError(correlationID, commandID, code, category, message string, retryable bool) Outbound {
	payload := map[string]any{
		"error": map[string]any{
			"code": code, "category": category, "retryable": retryable, "message": message,
		},
	}
	if commandID != "" {
		payload["command_id"] = commandID
	}
	return Outbound{V: 1, Type: "command.error", CorrelationID: correlationID, NodeID: s.localNodeID, Payload: payload}
}

func (s *Server) subscriptionError(correlationID, target, code, category, message string, retryable bool) Outbound {
	payload := map[string]any{
		"error": map[string]any{
			"code": code, "category": category, "retryable": retryable, "message": message,
		},
	}
	if target != "" {
		payload["target"] = target
	}
	return Outbound{V: 1, Type: "subscription.error", CorrelationID: correlationID, NodeID: s.localNodeID, Payload: payload}
}

func classifiedErrorFields(err error) (string, string, string, bool) {
	var classified *commands.ClassifiedError
	if errors.As(err, &classified) {
		return classified.Code, classified.Category, classified.Error(), classified.Retryable
	}
	return "PROVIDER_FAILURE", "execution", "monitoring request failed", false
}

func (s *Server) handleMonitoringCommand(ctx context.Context, message Inbound, peer *connections.Peer, remoteIP string, subscriber *events.Subscriber) Outbound {
	if message.CorrelationID == "" {
		return s.commandError("", "", "VALIDATION_MISSING_FIELD", "validation", "correlation_id is required", false)
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		code, category, description, retryable := classifiedErrorFields(err)
		return s.commandError(message.CorrelationID, "", code, category, description, retryable)
	}
	var payload struct {
		CommandID string          `json:"command_id"`
		Params    json.RawMessage `json:"params,omitempty"`
	}
	if len(message.Payload) == 0 {
		return s.commandError(message.CorrelationID, "", "VALIDATION_MISSING_FIELD", "validation", "payload.command_id is required", false)
	}
	if err := json.Unmarshal(message.Payload, &payload); err != nil {
		return s.commandError(message.CorrelationID, "", "VALIDATION_INVALID_FIELD", "validation", "payload has an invalid structure", false)
	}
	if payload.CommandID == "" {
		return s.commandError(message.CorrelationID, "", "VALIDATION_MISSING_FIELD", "validation", "payload.command_id is required", false)
	}

	legacy := message
	legacy.Type = "command"
	legacy.Action = payload.CommandID
	legacy.Data = payload.Params
	subs := make(map[string]struct{})
	for _, topic := range s.events.Topics(subscriber) {
		subs[topic] = struct{}{}
	}
	result := s.registry.Dispatch(ctx, legacy, commands.ClientContext{
		DeviceID: peer.DeviceID, ClientType: peer.ClientType, RemoteIP: remoteIP,
		SessionID: peer.ConnectedAt.UnixNano(), Subscriptions: subs,
	})
	if result.Type != "result" {
		code, category := result.ErrorCode, result.ErrorCategory
		if code == "" {
			code, category = "PROVIDER_FAILURE", "execution"
		}
		return s.commandError(message.CorrelationID, payload.CommandID, code, category, result.Error, result.Retryable)
	}

	encoded, err := json.Marshal(result.Data)
	if err != nil {
		return s.commandError(message.CorrelationID, payload.CommandID, "PROVIDER_FAILURE", "execution", "failed to encode command result", false)
	}
	var metric map[string]any
	if len(encoded) > 0 && json.Unmarshal(encoded, &metric) == nil && metric != nil {
		resultPayload := make(map[string]any)
		if value, ok := metric["value"]; ok {
			resultPayload["value"] = value
		}
		if unit, ok := metric["unit"]; ok {
			resultPayload["unit"] = unit
		}
		commandPayload := map[string]any{"command_id": payload.CommandID, "result": resultPayload}
		metadata := make(map[string]any)
		for _, key := range []string{
			"observed_at", "status", "reason", "error_code", "source_id",
			"window_ms", "window_start", "window_end",
		} {
			if field, exists := metric[key]; exists {
				metadata[key] = field
			}
		}
		if len(metadata) > 0 {
			commandPayload["metadata"] = metadata
		}
		return Outbound{V: 1, Type: "command.result", CorrelationID: message.CorrelationID, NodeID: s.localNodeID, Payload: commandPayload}
	}
	return Outbound{V: 1, Type: "command.result", CorrelationID: message.CorrelationID, NodeID: s.localNodeID, Payload: map[string]any{"command_id": payload.CommandID, "result": result.Data}}
}

func (s *Server) handleSubscriptionCreate(message Inbound, peer *connections.Peer) (Outbound, func()) {
	if message.CorrelationID == "" {
		return s.subscriptionError("", "", "VALIDATION_MISSING_FIELD", "validation", "correlation_id is required", false), nil
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		code, category, description, retryable := classifiedErrorFields(err)
		return s.subscriptionError(message.CorrelationID, "", code, category, description, retryable), nil
	}
	var payload struct {
		Target     string `json:"target"`
		IntervalMS int64  `json:"interval_ms"`
	}
	if len(message.Payload) == 0 {
		return s.subscriptionError(message.CorrelationID, "", "VALIDATION_MISSING_FIELD", "validation", "target and interval_ms are required", false), nil
	}
	if err := json.Unmarshal(message.Payload, &payload); err != nil {
		return s.subscriptionError(message.CorrelationID, "", "VALIDATION_INVALID_FIELD", "validation", "subscription payload has an invalid structure", false), nil
	}
	if payload.Target == "" || payload.IntervalMS == 0 {
		return s.subscriptionError(message.CorrelationID, payload.Target, "VALIDATION_MISSING_FIELD", "validation", "target and interval_ms are required", false), nil
	}
	if payload.IntervalMS != cpumonitor.EffectiveInterval.Milliseconds() {
		return s.subscriptionError(message.CorrelationID, payload.Target, "SUBSCRIPTION_INTERVAL_UNSUPPORTED", "validation", cpumonitor.ErrInvalidInterval.Error(), false), nil
	}
	deliver := func(ctx context.Context, outbound commands.Outbound) error {
		err := peer.QueueJSON(ctx, outbound)
		if errors.Is(err, connections.ErrQueueFull) && s.cfg.MessageBus.SlowConsumerPolicy == "disconnect" {
			peer.Close()
		}
		return err
	}
	id, err := s.cpuMonitoring.Subscribe(peer.ConnectedAt.UnixNano(), payload.Target, time.Duration(payload.IntervalMS)*time.Millisecond, deliver)
	if err != nil {
		code, category, retryable := "PROVIDER_FAILURE", "execution", false
		switch {
		case errors.Is(err, cpumonitor.ErrInvalidTarget):
			code, category, retryable = "SUBSCRIPTION_TARGET_UNSUPPORTED", "validation", false
		case errors.Is(err, cpumonitor.ErrInvalidInterval):
			code, category, retryable = "SUBSCRIPTION_INTERVAL_UNSUPPORTED", "validation", false
		case errors.Is(err, cpumonitor.ErrSubscriptionLimit):
			code, category, retryable = "SUBSCRIPTION_LIMIT_REACHED", "resource_limit", true
		case errors.Is(err, cpumonitor.ErrManagerClosed):
			code, category, retryable = "NODE_UNAVAILABLE", "routing", true
		}
		return s.subscriptionError(message.CorrelationID, payload.Target, code, category, err.Error(), retryable), nil
	}
	ack := Outbound{
		V: 1, Type: "subscription.created", CorrelationID: message.CorrelationID, NodeID: s.localNodeID,
		Payload: map[string]any{
			"subscription_id": id, "target": payload.Target,
			"effective_interval_ms": cpumonitor.EffectiveInterval.Milliseconds(),
		},
	}
	return ack, s.cpuMonitoring.Wake
}

func (s *Server) handleSubscriptionCancel(message Inbound, peer *connections.Peer) Outbound {
	if message.CorrelationID == "" {
		return s.subscriptionError("", "", "VALIDATION_MISSING_FIELD", "validation", "correlation_id is required", false)
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		code, category, description, retryable := classifiedErrorFields(err)
		return s.subscriptionError(message.CorrelationID, "", code, category, description, retryable)
	}
	var payload struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if len(message.Payload) == 0 {
		return s.subscriptionError(message.CorrelationID, "", "VALIDATION_MISSING_FIELD", "validation", "payload.subscription_id is required", false)
	}
	if err := json.Unmarshal(message.Payload, &payload); err != nil {
		return s.subscriptionError(message.CorrelationID, "", "VALIDATION_INVALID_FIELD", "validation", "subscription payload has an invalid structure", false)
	}
	if payload.SubscriptionID == "" {
		return s.subscriptionError(message.CorrelationID, "", "VALIDATION_MISSING_FIELD", "validation", "payload.subscription_id is required", false)
	}
	if !s.cpuMonitoring.Cancel(peer.ConnectedAt.UnixNano(), payload.SubscriptionID) {
		return s.subscriptionError(message.CorrelationID, "", "SUBSCRIPTION_NOT_FOUND", "routing", "subscription was not found in this connection context", false)
	}
	return Outbound{
		V: 1, Type: "subscription.cancelled", CorrelationID: message.CorrelationID, NodeID: s.localNodeID,
		Payload: map[string]any{"subscription_id": payload.SubscriptionID, "state": "cancelled"},
	}
}
