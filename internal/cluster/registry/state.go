package registry

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// AppliedOperation is kept in snapshots so command retries remain idempotent
// after Raft has compacted earlier log entries. Never prune without a policy.
type AppliedOperation struct {
	OperationID string `json:"operation_id"`
	CommandHash string `json:"command_hash"`
	Revision    int64  `json:"revision"`
	AppliedAt   string `json:"applied_at"`
}

// DurableState is a COMPLETE snapshot of the Raft-projected registry,
// including tombstones and the idempotency ledger. It is NOT a backup of the
// device/storage databases. Use this only from Raft's FSM, not public HTTP/WS.
type DurableState struct {
	Version   int                `json:"version"`
	Registry  Snapshot           `json:"registry"`
	Applied   []AppliedOperation `json:"applied"`
	UpdatedAt string             `json:"updated_at,omitempty"`
}

// ReadDurableState obtains one transactionally consistent SQLite snapshot.
// It locks the store's writer mutex so the FSM can snapshot without racing
// against ApplyCommitted in this process.
func (s *Store) ReadDurableState(ctx context.Context) (DurableState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return DurableState{}, err
	}
	defer tx.Rollback()
	state := DurableState{Version: 1, Registry: Snapshot{Nodes: []NodeRecord{}}, Applied: []AppliedOperation{}}
	e := tx.QueryRowContext(ctx, `SELECT cluster_id,revision,updated_at FROM hc_cluster_registry_meta WHERE singleton=1`).Scan(&state.Registry.ClusterID, &state.Registry.Revision, &state.UpdatedAt)
	if e != nil && !errors.Is(e, sql.ErrNoRows) {
		return DurableState{}, e
	}
	rows, err := tx.QueryContext(ctx, `SELECT node_id,cluster_id,home_id,display_name,membership,revision,removed,created_at,updated_at FROM hc_cluster_registry_nodes ORDER BY node_id`)
	if err != nil {
		return DurableState{}, err
	}
	for rows.Next() {
		var n NodeRecord
		var removed int
		var created, updated string
		if err = rows.Scan(&n.NodeID, &n.ClusterID, &n.HomeID, &n.Name, &n.Membership, &n.Revision, &removed, &created, &updated); err != nil {
			rows.Close()
			return DurableState{}, err
		}
		n.Removed = removed != 0
		n.CreatedAt, err = time.Parse(time.RFC3339Nano, created)
		if err != nil {
			rows.Close()
			return DurableState{}, err
		}
		n.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated)
		if err != nil {
			rows.Close()
			return DurableState{}, err
		}
		n.Endpoints = []Endpoint{}
		state.Registry.Nodes = append(state.Registry.Nodes, n)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return DurableState{}, err
	}
	if err = rows.Close(); err != nil {
		return DurableState{}, err
	}
	indices := make(map[string]int, len(state.Registry.Nodes))
	for i, n := range state.Registry.Nodes {
		indices[n.NodeID] = i
	}
	erows, err := tx.QueryContext(ctx, `SELECT node_id,network,protocol,host,port,priority FROM hc_cluster_registry_endpoints ORDER BY node_id,priority DESC,network,host,port`)
	if err != nil {
		return DurableState{}, err
	}
	for erows.Next() {
		var id string
		var ep Endpoint
		if err = erows.Scan(&id, &ep.Network, &ep.Protocol, &ep.Host, &ep.Port, &ep.Priority); err != nil {
			erows.Close()
			return DurableState{}, err
		}
		i, exists := indices[id]
		if !exists {
			erows.Close()
			return DurableState{}, fmt.Errorf("orphan registry endpoint for %q", id)
		}
		state.Registry.Nodes[i].Endpoints = append(state.Registry.Nodes[i].Endpoints, ep)
	}
	if err = erows.Err(); err != nil {
		erows.Close()
		return DurableState{}, err
	}
	if err = erows.Close(); err != nil {
		return DurableState{}, err
	}
	ops, err := tx.QueryContext(ctx, `SELECT operation_id,command_hash,revision,applied_at FROM hc_cluster_registry_applied ORDER BY revision`)
	if err != nil {
		return DurableState{}, err
	}
	for ops.Next() {
		var o AppliedOperation
		if err = ops.Scan(&o.OperationID, &o.CommandHash, &o.Revision, &o.AppliedAt); err != nil {
			ops.Close()
			return DurableState{}, err
		}
		state.Applied = append(state.Applied, o)
	}
	if err = ops.Err(); err != nil {
		ops.Close()
		return DurableState{}, err
	}
	if err = ops.Close(); err != nil {
		return DurableState{}, err
	}
	if err = tx.Commit(); err != nil {
		return DurableState{}, err
	}
	return state, nil
}

// RestoreDurableState atomically replaces ONLY the five registry-projection
// tables. Raft snapshots already validate checksum; here we also reject an
// inconsistent version and revision. This must run under FSM coordination.
func (s *Store) RestoreDurableState(ctx context.Context, state DurableState) error {
	if state.Version != 1 || state.Registry.Revision < 0 {
		return errors.New("unsupported registry snapshot")
	}
	if state.Registry.Revision == 0 {
		if state.Registry.ClusterID != "" || len(state.Registry.Nodes) != 0 || len(state.Applied) != 0 {
			return errors.New("nonempty initial registry snapshot")
		}
	} else if !idPattern.MatchString(state.Registry.ClusterID) {
		return ErrClusterMismatch
	}
	if state.Registry.Revision > 0 {
		if _, err := time.Parse(time.RFC3339Nano, state.UpdatedAt); err != nil {
			return fmt.Errorf("invalid snapshot meta timestamp: %w", err)
		}
	}
	revisions := make(map[int64]bool, len(state.Applied))
	operationIDs := make(map[string]bool, len(state.Applied))
	for _, o := range state.Applied {
		if !idPattern.MatchString(o.OperationID) || len(o.CommandHash) != 64 || o.Revision <= 0 || o.Revision > state.Registry.Revision || revisions[o.Revision] || operationIDs[o.OperationID] {
			return errors.New("corrupt snapshot idempotency ledger")
		}
		revisions[o.Revision], operationIDs[o.OperationID] = true, true
	}
	// KAN-6 doesn't support pruning operation IDs. Preserve every revision.
	if int64(len(state.Applied)) != state.Registry.Revision {
		return errors.New("incomplete snapshot idempotency ledger")
	}
	seen := make(map[string]bool, len(state.Registry.Nodes))
	for _, n := range state.Registry.Nodes {
		if !nodeIDPattern.MatchString(n.NodeID) || n.ClusterID != state.Registry.ClusterID || n.Revision < 1 || n.Revision > state.Registry.Revision || seen[n.NodeID] {
			return errors.New("corrupt snapshot node")
		}
		if (n.Removed && n.Membership != "removed") || (!n.Removed && n.Membership != "pending" && n.Membership != "active") {
			return errors.New("corrupt snapshot membership")
		}
		if n.CreatedAt.IsZero() || n.UpdatedAt.IsZero() || n.Name == "" {
			return errors.New("corrupt snapshot node metadata")
		}
		if n.Removed && len(n.Endpoints) != 0 {
			return errors.New("removed node has endpoints")
		}
		if len(n.Endpoints) > MaxEndpoints {
			return errors.New("too many restored endpoints")
		}
		for _, ep := range n.Endpoints {
			copy := ep
			if err := validateEndpoint(&copy); err != nil {
				return err
			}
		}
		seen[n.NodeID] = true
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, table := range []string{"hc_cluster_registry_endpoints", "hc_cluster_registry_nodes", "hc_cluster_registry_applied", "hc_cluster_registry_meta"} {
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+table); err != nil {
			return fmt.Errorf("reset %s: %w", table, err)
		}
	}
	if state.Registry.Revision > 0 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_meta(singleton,cluster_id,revision,updated_at) VALUES(1,?,?,?)`, state.Registry.ClusterID, state.Registry.Revision, state.UpdatedAt); err != nil {
			return err
		}
	}
	for _, n := range state.Registry.Nodes {
		removed := 0
		if n.Removed {
			removed = 1
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_nodes(node_id,cluster_id,home_id,display_name,membership,revision,removed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)`, n.NodeID, n.ClusterID, n.HomeID, n.Name, n.Membership, n.Revision, removed, n.CreatedAt.UTC().Format(time.RFC3339Nano), n.UpdatedAt.UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
		if err = writeEndpoints(ctx, tx, n.NodeID, n.Endpoints); err != nil {
			return err
		}
	}
	for _, o := range state.Applied {
		if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_applied(operation_id,command_hash,revision,applied_at) VALUES(?,?,?,?)`, o.OperationID, o.CommandHash, o.Revision, o.AppliedAt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// VerifySchema allows the consensus layer to check a fully migrated local SQLite store
// without accessing its underlying db handle.
func (s *Store) VerifySchema(ctx context.Context) error { return VerifySchema(ctx, s.db) }
