package wsserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/url"
	"strconv"

	"HomeCloudBackend/internal/commands"
	"HomeCloudBackend/internal/mediagateway"
)

func (s *Server) SetLocalNodeID(nodeID string) { s.localNodeID = nodeID }

func (s *Server) registerMediaGatewayCommands() {
	s.registry.MustRegister("media.gateway.register", s.registerMediaGateway)
	s.registry.MustRegister("media.gateway.status", s.mediaGatewayStatus)
	s.registry.MustRegister("media.gateway.resolve", s.resolveMediaGateway)
}

func (s *Server) registerMediaGateway(_ context.Context, msg commands.Inbound, caller commands.ClientContext) (any, error) {
	// Existing HC WS v1 uses only routing IDs, not cryptographic identities.
	// Limit this new privileged companion registration to local process traffic.
	ip := net.ParseIP(caller.RemoteIP)
	if !s.cfg.MediaGateway.Enabled || ip == nil || !ip.IsLoopback() || caller.ClientType != "media_gateway" ||
		caller.DeviceID != "homecloud-media-gateway" || caller.SessionID == 0 {
		return nil, errors.New("media gateway registration requires a local companion control session")
	}
	var req struct {
		GatewayID                string                    `json:"gateway_id"`
		Endpoints                []mediagateway.Endpoint   `json:"endpoints"`
		Capabilities             mediagateway.Capabilities `json:"capabilities"`
		HeartbeatIntervalSeconds int                       `json:"heartbeat_interval_seconds"`
		HeartbeatGraceSeconds    int                       `json:"heartbeat_grace_seconds"`
	}
	dec := json.NewDecoder(bytes.NewReader(msg.Data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return nil, errors.New("invalid media gateway registration data")
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing registration payload")
	}
	u, err := url.Parse(s.cfg.MediaGateway.HealthURL)
	if err != nil {
		return nil, err
	}
	port, err := strconv.Atoi(u.Port())
	if err != nil {
		return nil, err
	}
	result, err := s.gateways.Register(mediagateway.Registration{
		GatewayID: req.GatewayID, OwnerNodeID: s.localNodeID,
		Endpoints: req.Endpoints, Capabilities: req.Capabilities, SessionID: caller.SessionID,
		HeartbeatIntervalSeconds: req.HeartbeatIntervalSeconds, HeartbeatGraceSeconds: req.HeartbeatGraceSeconds,
	}, port, mediagateway.IsLocalIP)
	if err != nil {
		return nil, err
	}
	s.log.Info("Media Gateway registered", "gateway_id", result.GatewayID,
		"owner_node_id", result.OwnerNodeID, "endpoints", result.Endpoints,
		"media_ready", result.Capabilities.DirectWS)
	return map[string]any{"gateway_id": result.GatewayID, "owner_node_id": result.OwnerNodeID,
		"status": "registered", "endpoints": result.Endpoints, "media_ready": result.Capabilities.DirectWS}, nil
}

func (s *Server) mediaGatewayStatus(_ context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
	entry, online := s.gateways.Get()
	if !online {
		return map[string]any{"online": false, "endpoints": []mediagateway.Endpoint{}, "media_ready": false}, nil
	}
	return map[string]any{"online": true, "gateway_id": entry.GatewayID,
		"owner_node_id": entry.OwnerNodeID, "registered_at": entry.RegisteredAt,
		"endpoints": entry.Endpoints, "capabilities": entry.Capabilities,
		"media_ready": entry.Capabilities.DirectWS}, nil
}
func (s *Server) resolveMediaGateway(_ context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
	var req struct {
		Network string `json:"network"`
	}
	dec := json.NewDecoder(bytes.NewReader(msg.Data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return nil, errors.New("invalid resolve data")
	}
	if err := dec.Decode(new(any)); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing resolve payload")
	}
	endpoint, err := s.gateways.Resolve(req.Network)
	if err != nil {
		return nil, err
	}
	// Status/resolve are discovery only: KAN-48 enables actual media handshake.
	return map[string]any{"network": endpoint.Network, "url": endpoint.URL,
		"media_ready": false}, nil
}
