package runtime

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/membership/raftadapter"
	"HomeCloudBackend/internal/cluster/registry"
	"github.com/hashicorp/raft"
)

// StatusDetail is a snapshot of the local node and its observed Raft state.
// It does not claim remote nodes are online or the local node has quorum.
func (m *Manager) StatusDetail(ctx context.Context) (map[string]any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	m.mu.Lock()
	mode, clusterID, nodeID, address, svc := m.statusLocked(), m.state.ClusterID, m.nodeID, m.opts.RaftAddress, m.raft
	m.mu.Unlock()
	result := map[string]any{"node_id": nodeID, "cluster_id": clusterID, "mode": mode, "raft_address": address, "role": "standalone", "leader_address": "", "voters": 0, "quorum_required": 0}
	if svc != nil {
		result["role"] = svc.RaftState().String()
		result["leader_address"] = svc.LeaderAddress()
		conf, err := svc.Configuration()
		if err != nil {
			return nil, fmt.Errorf("cluster_configuration_unavailable: %w", err)
		}
		voters := 0
		for _, server := range conf.Servers {
			if server.Suffrage == raft.Voter {
				voters++
			}
		}
		// HashiCorp Raft's voter enum is 0; no online/quorum claim is made.
		result["voters"] = voters
		if voters > 0 {
			result["quorum_required"] = voters/2 + 1
		}
	}
	return result, nil
}

func (m *Manager) LeaderDetail(ctx context.Context) (map[string]any, error) {
	s, err := m.StatusDetail(ctx)
	if err != nil {
		return nil, err
	}
	return map[string]any{"node_id": s["node_id"], "cluster_id": s["cluster_id"], "role": s["role"], "leader_address": s["leader_address"], "mode": s["mode"]}, nil
}
func (m *Manager) RegistrySnapshot(ctx context.Context, includeRemoved bool) (registry.Snapshot, error) {
	return m.directory.ReadSnapshot(ctx, includeRemoved)
}
func (m *Manager) RaftAddress() string { m.mu.Lock(); defer m.mu.Unlock(); return m.opts.RaftAddress }

// UpdateNode submits only metadata changes through the existing committed FSM.
// Caller-supplied timestamp and idempotency key must be kept identical on
// retries, including after a leader change. CAS checks prevent stale updates.
func (m *Manager) UpdateNode(ctx context.Context, node registry.NodeRecord, expected int64, key string, when time.Time) (registry.ApplyResult, error) {
	if err := m.requireOwner(ctx, "cluster.node.update", node.NodeID); err != nil {
		return registry.ApplyResult{}, err
	}
	if node.Removed || node.Membership != "active" {
		return registry.ApplyResult{}, errors.New("cluster_invalid_state: only active nodes may be edited")
	}
	if when.IsZero() || strings.TrimSpace(key) == "" || len(key) > 128 {
		return registry.ApplyResult{}, errors.New("cluster_invalid_request: timestamp and idempotency key required")
	}
	m.mu.Lock()
	svc, clusterID, closed := m.raft, m.state.ClusterID, m.closed
	m.mu.Unlock()
	if closed || svc == nil || clusterID == "" {
		return registry.ApplyResult{}, errors.New("cluster_not_initialized")
	}
	if !(raftadapter.Adapter{Service: svc}).IsLeader() {
		return registry.ApplyResult{}, membership.ErrNotLeader
	}
	node.ClusterID = clusterID
	node.Membership = "active"
	node.Revision = 0
	node.Removed = false
	node.CreatedAt = time.Time{}
	node.UpdatedAt = time.Time{}
	sum := sha256.Sum256([]byte(node.NodeID + "\x00" + key))
	op := "ws-" + hex.EncodeToString(sum[:16])
	cmd := registry.Command{OperationID: op, ClusterID: clusterID, ExpectedRevision: expected, Type: registry.ChangeUpdate, Node: node, Timestamp: when.UTC()}
	return svc.Apply(ctx, cmd)
}

// ParseTargetIP accepts literal IPv4, IPv6, bracketed IPv6 or private IP:port.
// DNS and alternate routes will be handled by KAN-10. No public IP is accepted
// because JoinByIP re-checks the configured allowed private CIDR.
func (m *Manager) ParseTargetIP(input string) (string, string, error) {
	input = strings.TrimSpace(input)
	if input == "" || len(input) > 260 {
		return "", "", errors.New("cluster_invalid_address")
	}
	var host, port string
	if ip := net.ParseIP(input); ip != nil {
		host = ip.String()
	} else if h, p, e := net.SplitHostPort(input); e == nil {
		host, port = h, p
	} else {
		return "", "", errors.New("cluster_invalid_address: literal IP required; DNS comes in KAN-10")
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return "", "", errors.New("cluster_invalid_address: IP required")
	}
	_, localPort, e := net.SplitHostPort(m.RaftAddress())
	if e != nil {
		return "", "", e
	}
	if port == "" {
		port = localPort
	}
	n, e := strconv.Atoi(port)
	if e != nil || n < 1 || n > 65535 {
		return "", "", errors.New("cluster_invalid_address: invalid port")
	}
	addr := net.JoinHostPort(ip.String(), port)
	if err := CheckPrivateRaftAddress(addr, m.networks); err != nil {
		return "", "", err
	}
	return addr, ip.String(), nil
}
