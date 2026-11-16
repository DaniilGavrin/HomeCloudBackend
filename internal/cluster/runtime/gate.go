package runtime

import (
	"HomeCloudBackend/internal/cluster/membership/preparedrpc"
	"HomeCloudBackend/internal/cluster/membership/raftadapter"
	"context"
	"errors"
	"time"
)

// dynamicGate switches from automatic candidate approval to voter authorization
// only after the actual Raft/registry state confirms active membership.
// Unknown or inactive states always reject instead of granting general admin.
type dynamicGate struct{ owner *Manager }

func (g dynamicGate) Authorize(ctx context.Context, caller, clusterID, action, target string) error {
	m := g.owner
	m.mu.Lock()
	state := m.state
	svc := m.raft
	dir := m.directory
	until := m.invitationExpires
	leader := state.LeaderID
	m.mu.Unlock()
	if svc == nil || clusterID != state.ClusterID || dir == nil {
		return errors.New("cluster not initialized")
	}
	snap, err := dir.ReadSnapshot(ctx, true)
	if err != nil {
		return err
	}
	// A candidate becomes a voter only once the shared registry confirms active.
	for _, n := range snap.Nodes {
		if n.NodeID == m.nodeID && n.Membership == "active" && !n.Removed {
			return (preparedrpc.VoterGate{ClusterID: clusterID, Raft: raftadapter.Adapter{Service: svc}, Directory: dir}).Authorize(ctx, caller, clusterID, action, target)
		}
	}
	if state.Mode == "joining" && leader != "" && time.Now().Before(until) {
		return (preparedrpc.CandidateGate{ClusterID: clusterID, ApprovedLeaderID: leader, EnrollmentExpiresAt: until}).Authorize(ctx, caller, clusterID, action, target)
	}
	return errors.New("membership control gate not active")
}

var _ preparedrpc.Gate = dynamicGate{}
