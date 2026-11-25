package wsserver

import (
	"context"
	"encoding/json"
	"errors"
	"net"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/devicebridge"
)

const (
	deviceBridgePeerID   = "homecloud-device-bridge"
	deviceBridgeClientType = "device_bridge"
	deviceContractsV1 = 1
)

type bridgeRegistration struct {
	ContractsVersion int      `json:"contracts_version"`
	BridgeVersion    string   `json:"bridge_version"`
	Transports       []string `json:"transports,omitempty"`
}

func (s *Server) registerDeviceBridgeCommands() {
	s.registry.MustRegister("device.bridge.register", func(_ context.Context, msg commands.Inbound, client commands.ClientContext) (any, error) {
		if err := validateDeviceBridgePeer(client); err != nil { return nil, err }
		var payload bridgeRegistration
		if err := json.Unmarshal(msg.Data, &payload); err != nil { return nil, errors.New("invalid device bridge registration payload") }
		if payload.ContractsVersion != deviceContractsV1 { return nil, errors.New("unsupported_contract_version") }
		if payload.BridgeVersion == "" || len(payload.BridgeVersion) > 64 { return nil, errors.New("invalid bridge_version") }
		if err := s.deviceBridge.Register(client.SessionID); err != nil { return nil, err }
		s.log.Info("Device Bridge registered", "bridge_version", payload.BridgeVersion, "transports", payload.Transports)
		_, _ = s.PublishEvent(context.Background(), "device.bridge.status", s.deviceBridge.Snapshot())
		return map[string]any{"accepted": true, "state": devicebridge.StatusRegistered, "contracts_version": deviceContractsV1}, nil
	})
	s.registry.MustRegister("device.bridge.ready", func(_ context.Context, _ commands.Inbound, client commands.ClientContext) (any, error) {
		if err := validateDeviceBridgePeer(client); err != nil { return nil, err }
		if err := s.deviceBridge.Ready(client.SessionID); err != nil { return nil, err }
		_, _ = s.PublishEvent(context.Background(), "device.bridge.status", s.deviceBridge.Snapshot())
		s.log.Info("Device Bridge ready")
		return map[string]any{"ready": true}, nil
	})
	s.registry.MustRegister("device.bridge.heartbeat", func(_ context.Context, _ commands.Inbound, client commands.ClientContext) (any, error) {
		if err := validateDeviceBridgePeer(client); err != nil { return nil, err }
		if err := s.deviceBridge.Heartbeat(client.SessionID); err != nil { return nil, errors.New("bridge_not_ready") }
		return map[string]any{"alive": true}, nil
	})
	s.registry.MustRegister("device.bridge.status", func(_ context.Context, _ commands.Inbound, client commands.ClientContext) (any, error) {
		if !s.cfg.DeviceBridge.Enabled {
			return nil, errors.New("device_bridge_disabled")
		}
		if client.DeviceID == deviceBridgePeerID || client.ClientType == deviceBridgeClientType {
			return nil, errors.New("forbidden")
		}
		return s.deviceBridge.Snapshot(), nil
	})
}

func validateDeviceBridgePeer(client commands.ClientContext) error {
	if !s.cfg.DeviceBridge.Enabled || client.DeviceID != deviceBridgePeerID || client.ClientType != deviceBridgeClientType {
		return errors.New("forbidden")
	}
	ip := net.ParseIP(client.RemoteIP)
	if ip == nil || !ip.IsLoopback() {
		return errors.New("device bridge must connect over loopback")
	}
	return nil
}
