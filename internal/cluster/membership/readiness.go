package membership

import (
	"context"
	"errors"
	"fmt"

	"HomeCloudBackend/internal/cluster/security"
)

// PreparedRPC is a future mutually authenticated Node-to-Node control channel.
// Each method MUST validate the remote peer key, current Raft membership,
// effective owner approval, target ID and endpoint. Never implement with an
// unauthenticated HTTP/WebSocket request or a constant `return nil`.
type PreparedRPC interface {
	AssertTrustedOnVoter(context.Context, Voter, security.Peer) error
	AssertVoterTrustOnCandidate(context.Context, security.Peer, []Voter) error
	RevokeOnVoter(context.Context, Voter, string) error
}

// StrictReadiness enforces local peer-pinning, all-**voter** acknowledgements,
// and candidate readiness. Its RPC is intentionally REQUIRED for any remote
// peer; until the encrypted control plane is wired, it FAILS CLOSED.
type StrictReadiness struct {
	LocalNodeID string
	Trust       *security.TrustStore
	RPC         PreparedRPC
}

func (g StrictReadiness) VerifyPrepared(ctx context.Context, candidate security.Peer, voters []Voter) error {
	if g.Trust == nil || g.LocalNodeID == "" || len(voters) == 0 {
		return ErrUnprepared
	}
	local, ok := g.Trust.Peer(candidate.NodeID)
	if !ok || local.PublicKey != candidate.PublicKey {
		return security.ErrUntrusted
	}
	if g.RPC == nil {
		return fmt.Errorf("%w: remote peer RPC not configured", ErrUnprepared)
	}
	foundSelf := false
	for _, v := range voters {
		if !v.Voting {
			continue
		}
		if v.NodeID == g.LocalNodeID {
			foundSelf = true
			continue
		}
		if err := g.RPC.AssertTrustedOnVoter(ctx, v, candidate); err != nil {
			return fmt.Errorf("%w: voter %s: %v", ErrUnprepared, v.NodeID, err)
		}
	}
	if !foundSelf {
		return errors.New("not a voter on local Raft leader")
	}
	if err := g.RPC.AssertVoterTrustOnCandidate(ctx, candidate, voters); err != nil {
		return fmt.Errorf("%w: candidate has not trusted all voters: %v", ErrUnprepared, err)
	}
	return nil
}

func (g StrictReadiness) RevokeEverywhere(ctx context.Context, nodeID string, remaining []Voter) error {
	for _, v := range remaining {
		if !v.Voting || v.NodeID == g.LocalNodeID {
			continue
		}
		if g.RPC == nil {
			return fmt.Errorf("%w: remote revocation RPC not configured", ErrUnprepared)
		}
		if err := g.RPC.RevokeOnVoter(ctx, v, nodeID); err != nil {
			return fmt.Errorf("revocation on %s: %w", v.NodeID, err)
		}
	}
	return nil
}
