package main

// KAN-11: no second HTTP listener or owner token. Existing netbind.RestrictPeer
// protects the private /ws; whoever has network access can manage membership.
import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	clusterruntime "HomeCloudBackend/internal/cluster/runtime"
	"HomeCloudBackend/internal/commands"
	clustercommands "HomeCloudBackend/internal/commands/cluster"
	"HomeCloudBackend/internal/wsserver"
)

func installClusterWS(ctx context.Context, ws *wsserver.Server, m *clusterruntime.Manager, db *sql.DB, httpPort int, logger *slog.Logger) error {
	if ws == nil || db == nil {
		return errors.New("cluster websocket: missing server or database")
	}
	store := registry.NewStore(db)
	unavailable := func() error { return errors.New("cluster_unavailable: secure runtime not started") }
	localEndpoint := func(address string) ([]registry.Endpoint, error) {
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ip := net.ParseIP(host)
		if ip == nil {
			return nil, errors.New("cluster_invalid_address")
		}
		network := "lan"
		addr, _ := netip.ParseAddr(ip.String())
		tailV4 := netip.MustParsePrefix("100.64.0.0/10")
		tailV6 := netip.MustParsePrefix("fd7a:115c:a1e0::/48")
		if tailV4.Contains(addr) || tailV6.Contains(addr) {
			network = "tailscale"
		}
		return []registry.Endpoint{{Network: network, Protocol: "ws", Host: ip.String(), Port: httpPort, Priority: 100}}, nil
	}
	d := clustercommands.Dependencies{
		Status: func(c context.Context) (any, error) {
			if m == nil {
				return map[string]any{"mode": "unavailable", "cluster_id": "", "role": "unavailable"}, nil
			}
			return m.StatusDetail(c)
		},
		Leader: func(c context.Context) (any, error) {
			if m == nil {
				return nil, unavailable()
			}
			return m.LeaderDetail(c)
		},
		List: func(c context.Context, removed bool) (any, error) {
			snap, err := store.ReadSnapshot(c, removed)
			if err != nil {
				return nil, err
			}
			return snap, nil
		},
		Bootstrap: func(c context.Context, name, homeID string) (any, error) {
			if m == nil {
				return nil, unavailable()
			}
			ep, err := localEndpoint(m.RaftAddress())
			if err != nil {
				return nil, err
			}
			c, cancel := clustercommands.BoundWriteContext(c)
			defer cancel()
			return m.Bootstrap(c, homeID, name, ep)
		},
		Join: func(c context.Context, req clustercommands.JoinRequest) (any, error) {
			if m == nil {
				return nil, unavailable()
			}
			target, host, err := m.ParseTargetIP(req.Address)
			if err != nil {
				return nil, err
			}
			name := req.Name
			if name == "" {
				name = "HomeCloud " + host
			}
			endpoints, err := localEndpoint(target)
			if err != nil {
				return nil, err
			}
			c, cancel := clustercommands.BoundWriteContext(c)
			defer cancel()
			// First server: initialize a one-voter cluster explicitly when user clicks
			// add server. A joining fresh node does NOT bootstrap itself.
			mode, _, _ := m.Status()
			if mode == "standalone" {
				own, er := localEndpoint(m.RaftAddress())
				if er != nil {
					return nil, er
				}
				if _, er = m.Bootstrap(c, "", "HomeCloud", own); er != nil {
					return nil, fmt.Errorf("cluster_bootstrap_failed: %w", er)
				}
			}
			return m.JoinByIP(c, target, req.HomeID, name, endpoints)
		},
		Remove: func(c context.Context, id string) (any, error) {
			if m == nil {
				return nil, unavailable()
			}
			c, cancel := clustercommands.BoundWriteContext(c)
			defer cancel()
			return m.Remove(c, id)
		},
		Update: func(c context.Context, req clustercommands.UpdateRequest) (any, error) {
			if m == nil {
				return nil, unavailable()
			}
			snap, err := m.RegistrySnapshot(c, true)
			if err != nil {
				return nil, err
			}
			var node registry.NodeRecord
			found := false
			for _, n := range snap.Nodes {
				if n.NodeID == req.NodeID {
					node = n
					found = true
					break
				}
			}
			if !found || node.Removed {
				return nil, registry.ErrNotFound
			}
			// Empty endpoint array means preserve existing routes.
			if req.Endpoints != nil {
				node.Endpoints = req.Endpoints
			}
			node.Name = req.Name
			if req.HomeID != "" {
				node.HomeID = req.HomeID
			}
			c, cancel := clustercommands.BoundWriteContext(c)
			defer cancel()
			return m.UpdateNode(c, node, req.ExpectedRevision, req.IdempotencyKey, req.Timestamp)
		},
	}
	// Register without touching legacy bootstrap or altering the old envelope.
	if err := clustercommands.Register(wsCommandRegistry{ws}, d); err != nil {
		return err
	}
	go watchClusterRegistry(ctx, ws, store, logger)
	return nil
}

// The existing wsserver API exposes RegisterCommand, not a Registry. The
// adapter carries only registration, so the domain's dependency stays slim.
type wsCommandRegistry struct{ ws *wsserver.Server }

func (w wsCommandRegistry) Register(action string, h commands.Handler) error {
	return w.ws.RegisterCommand(action, h)
}

func watchClusterRegistry(ctx context.Context, ws *wsserver.Server, store *registry.Store, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	var lastCluster string
	var lastRevision int64
	ready := false
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		snap, err := store.ReadSnapshot(c, false)
		cancel()
		if err != nil {
			continue
		}
		if !ready {
			lastCluster, lastRevision, ready = snap.ClusterID, snap.Revision, true
			continue
		}
		if snap.ClusterID == lastCluster && snap.Revision <= lastRevision {
			continue
		}
		old := lastRevision
		lastCluster, lastRevision = snap.ClusterID, snap.Revision
		c, cancel = context.WithTimeout(ctx, 2*time.Second)
		_, err = ws.PublishEvent(c, "cluster.registry.changed", map[string]any{"cluster_id": snap.ClusterID, "revision": snap.Revision, "previous_revision": old, "resync_required": snap.Revision != old+1})
		cancel()
		if err != nil {
			logger.Warn("cluster registry change notification", "error", err)
		}
	}
}
