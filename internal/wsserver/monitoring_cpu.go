package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/connections"
	"HomeCloudBackend/internal/events"
	cpumonitor "HomeCloudBackend/internal/monitoring/cpu"
)

func (s *Server) monitoringNodeError(nodeID string) error {
	if nodeID == "" {
		return errors.New("node_id is required")
	}
	if s.localNodeID == "" {
		return errors.New("local node identity is unavailable")
	}
	if nodeID != s.localNodeID {
		return errors.New("requested node_id does not belong to this Backend")
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

func (s *Server) handleMonitoringCommand(ctx context.Context, message Inbound, peer *connections.Peer, remoteIP string, subscriber *events.Subscriber) Outbound {
	if message.CorrelationID == "" {
		return s.commandError("", "", "CORRELATION_ID_REQUIRED", "validation", "correlation_id is required", false)
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		return s.commandError(message.CorrelationID, "", "NODE_ID_INVALID", "validation", err.Error(), false)
	}
	var payload struct {
		CommandID string          `json:"command_id"`
		Params    json.RawMessage `json:"params,omitempty"`
	}
	if len(message.Payload) == 0 || json.Unmarshal(message.Payload, &payload) != nil || payload.CommandID == "" {
		return s.commandError(message.CorrelationID, "", "INVALID_PAYLOAD", "validation", "payload.command_id is required", false)
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
		code, category, retryable := "COMMAND_FAILED", "source", true
		switch {
		case result.Error == "unknown or unsupported action":
			code, category, retryable = "COMMAND_NOT_FOUND", "command", false
		case strings.Contains(result.Error, cpumonitor.ErrMetricUnsupported.Error()):
			code, category, retryable = "METRIC_UNSUPPORTED", "capability", false
		case strings.Contains(strings.ToLower(result.Error), "permission denied"):
			code, category, retryable = "SOURCE_PERMISSION_DENIED", "access", false
		case strings.Contains(strings.ToLower(result.Error), "deadline exceeded"):
			code, category, retryable = "SOURCE_TIMEOUT", "timeout", true
		case strings.Contains(strings.ToLower(result.Error), "invalid command params"):
			code, category, retryable = "SOURCE_INVALID_DATA", "validation", false
		case strings.Contains(strings.ToLower(result.Error), "context canceled"):
			code, category, retryable = "REQUEST_CANCELLED", "cancellation", false
		}
		return s.commandError(message.CorrelationID, payload.CommandID, code, category, result.Error, retryable)
	}

	encoded, err := json.Marshal(result.Data)
	if err != nil {
		return s.commandError(message.CorrelationID, payload.CommandID, "RESULT_ENCODING_FAILED", "internal", "failed to encode command result", false)
	}
	var metric map[string]any
	if err := json.Unmarshal(encoded, &metric); err == nil {
		if value, ok := metric["value"]; ok {
			resultPayload := map[string]any{"value": value}
			if unit, exists := metric["unit"]; exists {
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
	}
	return Outbound{
		V: 1, Type: "command.result", CorrelationID: message.CorrelationID, NodeID: s.localNodeID,
		Payload: map[string]any{"command_id": payload.CommandID, "result": result.Data},
	}
}

func (s *Server) handleSubscriptionCreate(message Inbound, peer *connections.Peer) (Outbound, func()) {
	if message.CorrelationID == "" {
		return s.subscriptionError("", "", "CORRELATION_ID_REQUIRED", "validation", "correlation_id is required", false), nil
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		return s.subscriptionError(message.CorrelationID, "", "NODE_ID_INVALID", "validation", err.Error(), false), nil
	}
	var payload struct {
		Target     string `json:"target"`
		IntervalMS int64  `json:"interval_ms"`
	}
	if len(message.Payload) == 0 || json.Unmarshal(message.Payload, &payload) != nil {
		return s.subscriptionError(message.CorrelationID, "", "INVALID_PAYLOAD", "validation", "target and interval_ms are required", false), nil
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
		code, category, retryable := "SUBSCRIPTION_FAILED", "source", true
		switch {
		case errors.Is(err, cpumonitor.ErrInvalidTarget):
			code, category, retryable = "SUBSCRIPTION_TARGET_UNSUPPORTED", "validation", false
		case errors.Is(err, cpumonitor.ErrInvalidInterval):
			code, category, retryable = "INVALID_INTERVAL", "validation", false
		case errors.Is(err, cpumonitor.ErrSubscriptionLimit):
			code, category, retryable = "SUBSCRIPTION_LIMIT_REACHED", "resource_limit", true
		case errors.Is(err, cpumonitor.ErrManagerClosed):
			code, category, retryable = "BACKEND_SHUTTING_DOWN", "availability", true
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
		return s.subscriptionError("", "", "CORRELATION_ID_REQUIRED", "validation", "correlation_id is required", false)
	}
	if err := s.monitoringNodeError(message.NodeID); err != nil {
		return s.subscriptionError(message.CorrelationID, "", "NODE_ID_INVALID", "validation", err.Error(), false)
	}
	var payload struct {
		SubscriptionID string `json:"subscription_id"`
	}
	if len(message.Payload) == 0 || json.Unmarshal(message.Payload, &payload) != nil || payload.SubscriptionID == "" {
		return s.subscriptionError(message.CorrelationID, "", "INVALID_PAYLOAD", "validation", "payload.subscription_id is required", false)
	}
	s.cpuMonitoring.Cancel(peer.ConnectedAt.UnixNano(), payload.SubscriptionID)
	return Outbound{
		V: 1, Type: "subscription.cancelled", CorrelationID: message.CorrelationID, NodeID: s.localNodeID,
		Payload: map[string]any{"subscription_id": payload.SubscriptionID, "state": "cancelled"},
	}
}
