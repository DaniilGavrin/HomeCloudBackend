package preparedrpc

import (
	"context"
	"errors"
	"fmt"
	"time"

	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/registry"
)

// VoterRaftView is a READ-ONLY view of actual Raft consensus. The leader must
// be found by its REAL Raft transport address in current voter config.
type VoterRaftView interface {
	LeaderAddress() string
	Members(context.Context) ([]membership.Voter, error)
}
type VoterGate struct {
	ClusterID string
	Raft      VoterRaftView
	Directory membership.Directory
}

func (g VoterGate) Authorize(ctx context.Context, caller, clusterID, action, target string) error {
	if g.Raft == nil || g.Directory == nil || g.ClusterID == "" || clusterID != g.ClusterID {
		return errors.New("invalid voter cluster context")
	}
	members, err := g.Raft.Members(ctx)
	if err != nil {
		return err
	}
	address := g.Raft.LeaderAddress()
	isLeader := false
	for _, m := range members {
		if m.Voting && m.NodeID == caller && m.RaftAddress == address {
			isLeader = true
			break
		}
	}
	if !isLeader || address == "" {
		return errors.New("caller is not the current Raft leader")
	}
	snap, err := g.Directory.ReadSnapshot(ctx, true)
	if err != nil {
		return err
	}
	if snap.ClusterID != clusterID || snap.Revision == 0 {
		return registry.ErrClusterMismatch
	}
	switch action {
	case actionPreparePeer:
		if target == "" {
			return errors.New("missing peer")
		}
		// An authenticated leader alone is NOT sufficient to grant TLS trust:
		// require a pending/committed admission in the replicated registry.
		for _, n := range snap.Nodes {
			if n.NodeID != target {
				continue
			}
			if n.Removed {
				return registry.ErrNodeRemoved
			}
			if n.Membership != "pending" && n.Membership != "active" {
				return errors.New("candidate has no valid membership state")
			}
			return nil
		}
		return errors.New("candidate has no committed pending admission")
	case actionRevoke:
		for _, n := range snap.Nodes {
			if n.NodeID == target && n.Removed {
				return nil
			}
		}
		return fmt.Errorf("no committed tombstone for %s", target)
	default:
		return errors.New("operation not permitted on voter")
	}
}

// CandidateGate is enabled only after *local* owner approval of the joining
// cluster and explicit pinning of the known leader. It must have a short expiry;
// no remote command can create or extend this authorization window.
type CandidateGate struct {
	ClusterID           string
	ApprovedLeaderID    string
	EnrollmentExpiresAt time.Time
}

func (g CandidateGate) Authorize(ctx context.Context, caller, clusterID, action, target string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if clusterID == "" || clusterID != g.ClusterID || caller == "" || caller != g.ApprovedLeaderID || action != actionPrepareVoters || time.Now().After(g.EnrollmentExpiresAt) {
		return errors.New("candidate enrollment not locally approved")
	}
	return nil
}
