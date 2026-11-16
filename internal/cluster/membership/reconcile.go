package membership

import (
	"HomeCloudBackend/internal/cluster/security"
	"context"
	"errors"
)

// ReconcileRemoved applies Raft-committed tombstones to a node's LOCAL TLS
// trust database. Call on startup and after registry revision changes; this
// prevents previously-offline voters from retaining removed peers indefinitely.
// It only revokes, never auto-approves any peer.
func ReconcileRemoved(ctx context.Context, dir Directory, trust *security.TrustStore) error {
	if dir == nil || trust == nil {
		return ErrUnprepared
	}
	snap, err := dir.ReadSnapshot(ctx, true)
	if err != nil {
		return err
	}
	for _, n := range snap.Nodes {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !n.Removed {
			continue
		}
		if err = trust.Revoke(n.NodeID); err != nil && !errors.Is(err, security.ErrUntrusted) {
			return err
		}
	}
	return nil
}
