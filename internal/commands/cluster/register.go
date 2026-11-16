// Package cluster registers the KAN-11 private-network cluster commands into
// the existing WS v1 command registry. Transport and network trust stay with
// wsserver/netbind; this package neither knows about sockets nor grants logins.
package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/commands"
)

type JoinRequest struct {
	Address string `json:"address"`
	Name    string `json:"name,omitempty"`
	HomeID  string `json:"home_id,omitempty"`
}
type RemoveRequest struct {
	NodeID string `json:"node_id"`
}
type UpdateRequest struct {
	NodeID           string              `json:"node_id"`
	Name             string              `json:"name"`
	HomeID           string              `json:"home_id,omitempty"`
	Endpoints        []registry.Endpoint `json:"endpoints,omitempty"`
	ExpectedRevision int64               `json:"expected_revision"`
	IdempotencyKey   string              `json:"idempotency_key"`
	Timestamp        time.Time           `json:"timestamp"`
}

type Dependencies struct {
	Status    func(context.Context) (any, error)
	List      func(context.Context, bool) (any, error)
	Leader    func(context.Context) (any, error)
	Bootstrap func(context.Context, string, string) (any, error)
	Join      func(context.Context, JoinRequest) (any, error)
	Remove    func(context.Context, string) (any, error)
	Update    func(context.Context, UpdateRequest) (any, error)
}

func decode(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		return errors.New("cluster_invalid_request: data object required")
	}
	if len(raw) > 32*1024 {
		return errors.New("cluster_invalid_request: data too large")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("cluster_invalid_request: %w", err)
	}
	if err := dec.Decode(new(any)); err != io.EOF {
		return errors.New("cluster_invalid_request: trailing JSON")
	}
	return nil
}
func trim(s string, limit int) (string, error) {
	s = strings.TrimSpace(s)
	if len(s) > limit {
		return "", errors.New("cluster_invalid_request: field too long")
	}
	return s, nil
}

type Registrar interface {
	Register(string, commands.Handler) error
}

func Register(r Registrar, d Dependencies) error {
	if r == nil || d.Status == nil || d.List == nil || d.Leader == nil || d.Bootstrap == nil || d.Join == nil || d.Remove == nil || d.Update == nil {
		return errors.New("cluster dependencies unavailable")
	}
	handlers := map[string]commands.Handler{
		"cluster.status": func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			return d.Status(ctx)
		},
		"cluster.nodes.list": func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			var req struct {
				IncludeRemoved bool `json:"include_removed"`
			}
			if len(msg.Data) > 0 {
				if err := decode(msg.Data, &req); err != nil {
					return nil, err
				}
			}
			return d.List(ctx, req.IncludeRemoved)
		},
		"cluster.leader.get": func(ctx context.Context, _ commands.Inbound, _ commands.ClientContext) (any, error) {
			return d.Leader(ctx)
		},
		"cluster.bootstrap": func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			var req struct {
				Name   string `json:"name"`
				HomeID string `json:"home_id"`
			}
			if err := decode(msg.Data, &req); err != nil {
				return nil, err
			}
			name, err := trim(req.Name, 160)
			if err != nil || name == "" {
				return nil, errors.New("cluster_invalid_request: name required (max 160)")
			}
			home, err := trim(req.HomeID, 128)
			if err != nil {
				return nil, err
			}
			return d.Bootstrap(ctx, name, home)
		},
		"cluster.nodes.join_by_address": func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			var req JoinRequest
			if err := decode(msg.Data, &req); err != nil {
				return nil, err
			}
			addr, err := trim(req.Address, 260)
			if err != nil || addr == "" {
				return nil, errors.New("cluster_invalid_request: address required")
			}
			req.Address = addr
			req.Name, err = trim(req.Name, 160)
			if err != nil {
				return nil, err
			}
			req.HomeID, err = trim(req.HomeID, 128)
			if err != nil {
				return nil, err
			}
			return d.Join(ctx, req)
		},
		"cluster.nodes.remove": func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			var req RemoveRequest
			if err := decode(msg.Data, &req); err != nil {
				return nil, err
			}
			id, err := trim(req.NodeID, 50)
			if err != nil || id == "" {
				return nil, errors.New("cluster_invalid_request: node_id required")
			}
			return d.Remove(ctx, id)
		},
		"cluster.nodes.update": func(ctx context.Context, msg commands.Inbound, _ commands.ClientContext) (any, error) {
			var req UpdateRequest
			if err := decode(msg.Data, &req); err != nil {
				return nil, err
			}
			var err error
			req.NodeID, err = trim(req.NodeID, 50)
			if err != nil || req.NodeID == "" {
				return nil, errors.New("cluster_invalid_request: node_id required")
			}
			req.Name, err = trim(req.Name, 160)
			if err != nil || req.Name == "" {
				return nil, errors.New("cluster_invalid_request: name required")
			}
			req.HomeID, err = trim(req.HomeID, 128)
			if err != nil {
				return nil, err
			}
			req.IdempotencyKey, err = trim(req.IdempotencyKey, 128)
			if err != nil || req.IdempotencyKey == "" {
				return nil, errors.New("cluster_invalid_request: idempotency_key required")
			}
			if req.Timestamp.IsZero() {
				return nil, errors.New("cluster_invalid_request: timestamp required (RFC3339)")
			}
			if req.ExpectedRevision < 1 {
				return nil, errors.New("cluster_invalid_request: expected_revision required")
			}
			if len(req.Endpoints) > registry.MaxEndpoints {
				return nil, errors.New("cluster_invalid_request: too many endpoints")
			}
			return d.Update(ctx, req)
		},
	}
	// Register in a fixed order for predictable behavior.
	for _, name := range []string{"cluster.status", "cluster.nodes.list", "cluster.leader.get", "cluster.bootstrap", "cluster.nodes.join_by_address", "cluster.nodes.remove", "cluster.nodes.update"} {
		if err := r.Register(name, handlers[name]); err != nil {
			return err
		}
	}
	return nil
}

// BoundWriteContext prevents indefinitely hanging a client command when the
// peer has no deadline. The handler must not signal success before Raft commit.
func BoundWriteContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, 35*time.Second)
}
