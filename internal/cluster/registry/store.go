package registry

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

type Store struct {
	db *sql.DB
	// Serialize writers in a Go process. SQLite transactions also protect
	// atomicity. Do NOT share one local SQLite database across physical Nodes.
	mu sync.Mutex
}

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// ApplyCommitted is the *only* mutator of the canonical directory. It must
// consume an authenticated, already-committed ordered command from KAN-7.
// KAN-6 does NOT offer a public WS endpoint or perform distributed commits.
// A standalone direct call here is only for deterministic initialization/tests.
func (s *Store) ApplyCommitted(ctx context.Context, cmd Command) (ApplyResult, error) {
	if err := validateCommand(&cmd); err != nil {
		return ApplyResult{}, err
	}
	data, err := json.Marshal(cmd)
	if err != nil {
		return ApplyResult{}, err
	}
	sum := sha256.Sum256(data)
	fingerprint := hex.EncodeToString(sum[:])
	s.mu.Lock()
	defer s.mu.Unlock()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ApplyResult{}, err
	}
	defer tx.Rollback()

	var prevHash string
	var prevRevision int64
	err = tx.QueryRowContext(ctx, `SELECT command_hash,revision FROM hc_cluster_registry_applied WHERE operation_id=?`, cmd.OperationID).Scan(&prevHash, &prevRevision)
	if err == nil {
		if prevHash != fingerprint {
			return ApplyResult{}, ErrOperationIDReuse
		}
		return ApplyResult{Revision: prevRevision, Replayed: true}, nil
	}
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return ApplyResult{}, fmt.Errorf("read operation: %w", err)
	}

	var clusterID string
	var revision int64
	err = tx.QueryRowContext(ctx, `SELECT cluster_id, revision FROM hc_cluster_registry_meta WHERE singleton=1`).Scan(&clusterID, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		if cmd.Type != ChangeAdd {
			return ApplyResult{}, ErrNotFound
		}
		clusterID, revision = cmd.ClusterID, 0
	} else if err != nil {
		return ApplyResult{}, fmt.Errorf("read registry meta: %w", err)
	}
	if clusterID != cmd.ClusterID {
		return ApplyResult{}, ErrClusterMismatch
	}
	if revision != cmd.ExpectedRevision {
		return ApplyResult{}, ErrRevisionConflict
	}
	next := revision + 1
	when := cmd.Timestamp.Format(time.RFC3339Nano)
	switch cmd.Type {
	case ChangeAdd:
		var present int
		e := tx.QueryRowContext(ctx, `SELECT removed FROM hc_cluster_registry_nodes WHERE node_id=?`, cmd.Node.NodeID).Scan(&present)
		if e == nil {
			if present != 0 {
				return ApplyResult{}, ErrNodeRemoved
			}
			return ApplyResult{}, ErrNodeExists
		}
		if !errors.Is(e, sql.ErrNoRows) {
			return ApplyResult{}, e
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_nodes(node_id,cluster_id,home_id,display_name,membership,revision,removed,created_at,updated_at) VALUES(?,?,?,?,?,?,0,?,?)`, cmd.Node.NodeID, cmd.ClusterID, cmd.Node.HomeID, cmd.Node.Name, cmd.Node.Membership, next, when, when); err != nil {
			return ApplyResult{}, err
		}
		if err = writeEndpoints(ctx, tx, cmd.Node.NodeID, cmd.Node.Endpoints); err != nil {
			return ApplyResult{}, err
		}
	case ChangeUpdate:
		var present int
		e := tx.QueryRowContext(ctx, `SELECT removed FROM hc_cluster_registry_nodes WHERE node_id=? AND cluster_id=?`, cmd.Node.NodeID, cmd.ClusterID).Scan(&present)
		if errors.Is(e, sql.ErrNoRows) {
			return ApplyResult{}, ErrNotFound
		}
		if e != nil {
			return ApplyResult{}, e
		}
		if present != 0 {
			return ApplyResult{}, ErrNodeRemoved
		}
		if _, err = tx.ExecContext(ctx, `UPDATE hc_cluster_registry_nodes SET home_id=?, display_name=?, membership=?, revision=?, updated_at=? WHERE node_id=?`, cmd.Node.HomeID, cmd.Node.Name, cmd.Node.Membership, next, when, cmd.Node.NodeID); err != nil {
			return ApplyResult{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM hc_cluster_registry_endpoints WHERE node_id=?`, cmd.Node.NodeID); err != nil {
			return ApplyResult{}, err
		}
		if err = writeEndpoints(ctx, tx, cmd.Node.NodeID, cmd.Node.Endpoints); err != nil {
			return ApplyResult{}, err
		}
	case ChangeRemove:
		var present int
		e := tx.QueryRowContext(ctx, `SELECT removed FROM hc_cluster_registry_nodes WHERE node_id=? AND cluster_id=?`, cmd.NodeID, cmd.ClusterID).Scan(&present)
		if errors.Is(e, sql.ErrNoRows) {
			return ApplyResult{}, ErrNotFound
		}
		if e != nil {
			return ApplyResult{}, e
		}
		if present != 0 {
			return ApplyResult{}, ErrNodeRemoved
		}
		if _, err = tx.ExecContext(ctx, `UPDATE hc_cluster_registry_nodes SET membership='removed',removed=1,revision=?,updated_at=? WHERE node_id=?`, next, when, cmd.NodeID); err != nil {
			return ApplyResult{}, err
		}
		if _, err = tx.ExecContext(ctx, `DELETE FROM hc_cluster_registry_endpoints WHERE node_id=?`, cmd.NodeID); err != nil {
			return ApplyResult{}, err
		}
	}
	if revision == 0 {
		if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_meta(singleton,cluster_id,revision,updated_at) VALUES(1,?,?,?)`, cmd.ClusterID, next, when); err != nil {
			return ApplyResult{}, err
		}
	} else {
		result, e := tx.ExecContext(ctx, `UPDATE hc_cluster_registry_meta SET revision=?, updated_at=? WHERE singleton=1 AND revision=?`, next, when, revision)
		if e != nil {
			return ApplyResult{}, e
		}
		n, e := result.RowsAffected()
		if e != nil {
			return ApplyResult{}, e
		}
		if n != 1 {
			return ApplyResult{}, ErrRevisionConflict
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_applied(operation_id,command_hash,revision,applied_at) VALUES(?,?,?,?)`, cmd.OperationID, fingerprint, next, when); err != nil {
		return ApplyResult{}, err
	}
	if err = tx.Commit(); err != nil {
		return ApplyResult{}, err
	}
	return ApplyResult{Revision: next}, nil
}

func writeEndpoints(ctx context.Context, tx *sql.Tx, nodeID string, endpoints []Endpoint) error {
	for _, ep := range endpoints {
		_, err := tx.ExecContext(ctx, `INSERT INTO hc_cluster_registry_endpoints(node_id,network,protocol,host,port,priority) VALUES(?,?,?,?,?,?)`, nodeID, ep.Network, ep.Protocol, ep.Host, ep.Port, ep.Priority)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) ReadSnapshot(ctx context.Context, includeRemoved bool) (Snapshot, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback()
	snap := Snapshot{Nodes: []NodeRecord{}}
	err = tx.QueryRowContext(ctx, `SELECT cluster_id,revision FROM hc_cluster_registry_meta WHERE singleton=1`).Scan(&snap.ClusterID, &snap.Revision)
	if errors.Is(err, sql.ErrNoRows) {
		return snap, nil
	} // freshly migrated; no cluster yet
	if err != nil {
		return Snapshot{}, err
	}
	query := `SELECT node_id,cluster_id,home_id,display_name,membership,revision,removed,created_at,updated_at FROM hc_cluster_registry_nodes`
	if !includeRemoved {
		query += ` WHERE removed=0`
	}
	query += ` ORDER BY node_id`
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return Snapshot{}, err
	}
	for rows.Next() {
		var n NodeRecord
		var deleted int
		var created, updated string
		if err = rows.Scan(&n.NodeID, &n.ClusterID, &n.HomeID, &n.Name, &n.Membership, &n.Revision, &deleted, &created, &updated); err != nil {
			rows.Close()
			return Snapshot{}, err
		}
		n.Removed = deleted != 0
		if n.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
			rows.Close()
			return Snapshot{}, err
		}
		if n.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
			rows.Close()
			return Snapshot{}, err
		}
		snap.Nodes = append(snap.Nodes, n)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return Snapshot{}, err
	}
	if err = rows.Close(); err != nil {
		return Snapshot{}, err
	}
	// Read endpoints in the SAME transaction for a consistent revision.
	eRows, err := tx.QueryContext(ctx, `SELECT node_id,network,protocol,host,port,priority FROM hc_cluster_registry_endpoints ORDER BY node_id,priority DESC,network,host,port`)
	if err != nil {
		return Snapshot{}, err
	}
	indices := make(map[string]int, len(snap.Nodes))
	for i := range snap.Nodes {
		indices[snap.Nodes[i].NodeID] = i
		snap.Nodes[i].Endpoints = []Endpoint{}
	}
	for eRows.Next() {
		var id string
		var ep Endpoint
		if err = eRows.Scan(&id, &ep.Network, &ep.Protocol, &ep.Host, &ep.Port, &ep.Priority); err != nil {
			eRows.Close()
			return Snapshot{}, err
		}
		if i, ok := indices[id]; ok {
			snap.Nodes[i].Endpoints = append(snap.Nodes[i].Endpoints, ep)
		}
	}
	if err = eRows.Err(); err != nil {
		eRows.Close()
		return Snapshot{}, err
	}
	if err = eRows.Close(); err != nil {
		return Snapshot{}, err
	}
	if err = tx.Commit(); err != nil {
		return Snapshot{}, err
	}
	return snap, nil
}

// FindNode only returns a registered node (unless caller explicitly requests
// tombstones). It is a convenience over the consistent full snapshot.
func (s *Store) FindNode(ctx context.Context, nodeID string, includeRemoved bool) (NodeRecord, error) {
	snap, err := s.ReadSnapshot(ctx, includeRemoved)
	if err != nil {
		return NodeRecord{}, err
	}
	for _, n := range snap.Nodes {
		if n.NodeID == nodeID {
			return n, nil
		}
	}
	return NodeRecord{}, ErrNotFound
}

// Stable sort is useful when callers construct a canonical snapshot for logs.
func SortNodesByID(nodes []NodeRecord) {
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].NodeID < nodes[j].NodeID })
}
