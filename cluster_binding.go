package main

// KAN-9: wire the private cluster receiver into normal HomeCloud `serve`.
// No CLI, public HTTP, WS v1 or unsafe 0.0.0.0 bind is introduced.

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"strconv"

	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/registry"
	clusterruntime "HomeCloudBackend/internal/cluster/runtime"
	"HomeCloudBackend/internal/netbind"
)

// Private cluster management is allowed only through the existing network-restricted
// WebSocket. No separate user login or owner credential is required.
type privateClusterAccess struct{}

func (privateClusterAccess) Authorize(ctx context.Context, action, peer string) error {
	return ctx.Err()
}

var _ membership.Authorizer = privateClusterAccess{}

func startClusterRuntime(ctx context.Context, root string, httpPort int, ids *identity.Store, db *sql.DB, endpoints []netbind.Endpoint, logger *slog.Logger) (*clusterruntime.Manager, error) {
	if len(endpoints) == 0 {
		return nil, fmt.Errorf("cluster: no private endpoints")
	}
	if httpPort < 1 || httpPort > 55533 {
		return nil, fmt.Errorf("cluster: HTTP port must be 1..55533 so that Raft and control ports remain valid")
	}
	// Prefer non-loopback. Tailscale is preferred when already available, then
	// LAN. KAN-10 adds multiple advertised addresses and route failover.
	chosen := endpoints[0]
	rank := func(ep netbind.Endpoint) int {
		if ep.IP.IsLoopback() {
			return 0
		}
		if ep.Allowed != nil && (ep.Allowed.String() == "100.64.0.0/10" || ep.Allowed.String() == "fd7a:115c:a1e0::/48") {
			return 3
		}
		if ep.IP.To4() != nil {
			return 2
		}
		return 1
	}
	for _, e := range endpoints[1:] {
		if rank(e) > rank(chosen) {
			chosen = e
		}
	}
	if chosen.Allowed == nil {
		return nil, fmt.Errorf("cluster: unknown interface trust subnet")
	}
	localRaft := net.JoinHostPort(chosen.IP.String(), strconv.Itoa(httpPort+10000))
	manager, err := clusterruntime.Open(ctx, clusterruntime.Options{
		DataDir: filepath.Join(root, "data", "cluster"), RaftAddress: localRaft,
		AllowedCIDRs: []string{chosen.Allowed.String()}, Identity: ids, Directory: registry.NewStore(db),
		// KAN-11: management follows the existing private-network WS trust boundary.
		OwnerAuthorizer: privateClusterAccess{},
	})
	if err != nil {
		return nil, err
	}
	mode, clusterID, nodeID := manager.Status()
	logger.Info("HomeCloud private cluster receiver ready", "node_id", nodeID, "cluster_id", clusterID,
		"mode", mode, "raft_address", localRaft, "reserved_membership_port", httpPort+10001,
		"first_use_address", net.JoinHostPort(chosen.IP.String(), strconv.Itoa(httpPort+10002)), "allowed_cidr", chosen.Allowed.String())
	return manager, nil
}
