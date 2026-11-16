// Package membership coordinates owner-authorized HomeCloud cluster membership.
// It never exposes a network/WS endpoint. KAN-11 may add an authenticated
// adapter; anonymous WS v1 MUST NOT call this package.
package membership

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
)

var (
	ErrUnauthorized  = errors.New("cluster membership: owner authorization required")
	ErrNotLeader     = errors.New("cluster membership: not leader")
	ErrUnprepared    = errors.New("cluster membership: peer trust is not ready on every voter and joining node")
	ErrIncomplete    = errors.New("cluster membership: committed partial operation; retry/reconcile required")
	ErrUnsafeRemoval = errors.New("cluster membership: cannot remove leader or last voter")
	ErrAlreadyMember = errors.New("cluster membership: node is already registered")
)

// Voter is a configuration member; address is the secure Raft address,
// NOT a client WebSocket endpoint. Voter=false means nonvoter.
type Voter struct {
	NodeID      string
	RaftAddress string
	Voting      bool
}

// Consensus is intentionally a small interface. Its real implementation is
// raftadapter.Service, which uses KAN-7/8 HashiCorp Raft.
type Consensus interface {
	NodeID() string
	IsLeader() bool
	Members(context.Context) ([]Voter, error)
	Apply(context.Context, registry.Command) (registry.ApplyResult, error)
	AddVoter(context.Context, string, string) error
	RemoveVoter(context.Context, string) error
}

type Directory interface {
	ReadSnapshot(context.Context, bool) (registry.Snapshot, error)
}

// Authorizer MUST verify an authenticated OWNER/admin credential through a
// privileged surface. Never create an always-allow authorizer for production.
type Authorizer interface {
	Authorize(context.Context, string, string) error
}

// Readiness must obtain *authenticated* acknowledgements from EVERY existing
// voting peer and from the candidate (candidate has the voter trust list).
// An unimplemented/nil readiness MUST deny enrollment, not pretend success.
// It must be idempotent and able to repair an interrupted trust distribution.
type Readiness interface {
	VerifyPrepared(context.Context, security.Peer, []Voter) error
	RevokeEverywhere(context.Context, string, []Voter) error
}

type Controller struct {
	raft       Consensus
	directory  Directory
	localTrust *security.TrustStore
	pairing    *security.PairingManager
	admin      Authorizer
	readiness  Readiness
	mu         sync.Mutex // serializes this leader's local admin operations
}

func NewController(raft Consensus, dir Directory, trust *security.TrustStore, pairing *security.PairingManager, admin Authorizer, readiness Readiness) (*Controller, error) {
	if raft == nil || dir == nil || trust == nil || pairing == nil || admin == nil || readiness == nil {
		return nil, ErrUnprepared
	}
	return &Controller{raft: raft, directory: dir, localTrust: trust, pairing: pairing, admin: admin, readiness: readiness}, nil
}

func (c *Controller) authorize(ctx context.Context, action, node string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := c.admin.Authorize(ctx, action, node); err != nil {
		return fmt.Errorf("%w: %v", ErrUnauthorized, err)
	}
	return nil
}
func (c *Controller) leader() error {
	if !c.raft.IsLeader() {
		return ErrNotLeader
	}
	return nil
}
func newOperation() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return "op-" + hex.EncodeToString(bytes[:]), nil
}

// Pairing flow: the future UI first invokes BeginPairing through an OWNER
// authenticated control channel, then transports the nonce to the candidate.
// A candidate's signed proof can be submitted over the future limited pairing
// channel; approval remains an explicit owner action.
func (c *Controller) BeginPairing(ctx context.Context) (security.Challenge, error) {
	if err := c.authorize(ctx, "cluster.pair.begin", ""); err != nil {
		return security.Challenge{}, err
	}
	return c.pairing.Begin()
}
func (c *Controller) VerifyPairingProof(ctx context.Context, challengeID string, peer security.Peer, signature []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.pairing.VerifyProof(challengeID, peer, signature)
}
func (c *Controller) DecidePairing(ctx context.Context, challengeID string, approve bool) error {
	peer, err := c.pairing.VerifiedPeer(challengeID)
	if err != nil {
		return err
	}
	if err := c.authorize(ctx, "cluster.pair.approve", peer.NodeID); err != nil {
		return err
	}
	return c.pairing.Approve(challengeID, approve)
}

// MemberStatus is a read-only projection for the future Servers UI. It never
// claims a node is ONLINE: availability comes from a separate health channel.
type MemberStatus struct {
	Node        registry.NodeRecord `json:"node"`
	RaftVoting  bool                `json:"raft_voting"`
	RaftAddress string              `json:"raft_address,omitempty"`
	Phase       string              `json:"phase"` // active|awaiting_voter|awaiting_activation|removed|awaiting_raft_removal
}

func (c *Controller) List(ctx context.Context, includeRemoved bool) (registry.Snapshot, []MemberStatus, error) {
	if err := c.authorize(ctx, "cluster.nodes.list", ""); err != nil {
		return registry.Snapshot{}, nil, err
	}
	snap, err := c.directory.ReadSnapshot(ctx, includeRemoved)
	if err != nil {
		return registry.Snapshot{}, nil, err
	}
	config, err := c.raft.Members(ctx)
	if err != nil {
		return registry.Snapshot{}, nil, err
	}
	out := make([]MemberStatus, 0, len(snap.Nodes))
	for _, n := range snap.Nodes {
		item := MemberStatus{Node: n, Phase: n.Membership}
		if v, ok := findVoter(config, n.NodeID); ok {
			item.RaftVoting = v.Voting
			item.RaftAddress = v.RaftAddress
		}
		switch {
		case n.Removed && item.RaftVoting:
			item.Phase = "awaiting_raft_removal"
		case n.Removed:
			item.Phase = "removed"
		case n.Membership == "pending" && item.RaftVoting:
			item.Phase = "awaiting_activation"
		case n.Membership == "pending":
			item.Phase = "awaiting_voter"
		case n.Membership == "active" && !item.RaftVoting:
			item.Phase = "configuration_mismatch"
		}
		out = append(out, item)
	}
	return snap, out, nil
}

type BootstrapRequest struct {
	ClusterID, HomeID, Name string
	Endpoints               []registry.Endpoint
}
type JoinRequest struct {
	ClusterID, HomeID, Name string
	Peer                    security.Peer
	RaftAddress             string
	Endpoints               []registry.Endpoint
}

type Outcome struct {
	ClusterID, NodeID string
	Revision          int64
	State             string
	Recovered         bool
}

// BootstrapLocal writes the self entry only for a fresh, single-voter cluster
// ALREADY bootstrapped by OpenSecure. It cannot bootstrap arbitrary IPs from WS.
func (c *Controller) BootstrapLocal(ctx context.Context, req BootstrapRequest) (Outcome, error) {
	if err := c.authorize(ctx, "cluster.bootstrap", c.raft.NodeID()); err != nil {
		return Outcome{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.leader(); err != nil {
		return Outcome{}, err
	}
	members, err := c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, err
	}
	if len(members) != 1 || !members[0].Voting || members[0].NodeID != c.raft.NodeID() {
		return Outcome{}, errors.New("bootstrap requires one local raft voter")
	}
	snap, err := c.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return Outcome{}, err
	}
	if snap.Revision != 0 || snap.ClusterID != "" || len(snap.Nodes) != 0 {
		return Outcome{}, errors.New("registry is already initialized")
	}
	op, err := newOperation()
	if err != nil {
		return Outcome{}, err
	}
	cmd := registry.Command{OperationID: op, ClusterID: req.ClusterID, ExpectedRevision: 0, Type: registry.ChangeAdd, Timestamp: time.Now().UTC(), Node: registry.NodeRecord{NodeID: c.raft.NodeID(), ClusterID: req.ClusterID, HomeID: req.HomeID, Name: req.Name, Membership: "active", Endpoints: req.Endpoints}}
	res, err := c.raft.Apply(ctx, cmd)
	if err != nil {
		return Outcome{}, err
	}
	return Outcome{ClusterID: req.ClusterID, NodeID: c.raft.NodeID(), Revision: res.Revision, State: "active"}, nil
}

// Join is an idempotent, resumable two-phase membership operation:
// verified trust barrier -> Raft pending record -> AddVoter -> active record.
// A committed pending entry is deliberate and must not be deleted on timeout.
func (c *Controller) Join(ctx context.Context, req JoinRequest) (Outcome, error) {
	if err := c.authorize(ctx, "cluster.node.join", req.Peer.NodeID); err != nil {
		return Outcome{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.leader(); err != nil {
		return Outcome{}, err
	}
	if req.Peer.NodeID == c.raft.NodeID() {
		return Outcome{}, errors.New("cannot join self")
	}
	pinned, ok := c.localTrust.Peer(req.Peer.NodeID)
	if !ok || pinned.PublicKey != req.Peer.PublicKey || !contains(pinned.Addresses, req.RaftAddress) {
		return Outcome{}, security.ErrUntrusted
	}
	if req.ClusterID == "" || req.Name == "" || len(req.Endpoints) == 0 {
		return Outcome{}, errors.New("incomplete join request")
	}
	members, err := c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, err
	}
	snap, err := c.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return Outcome{}, err
	}
	if snap.ClusterID != req.ClusterID || snap.Revision == 0 {
		return Outcome{}, registry.ErrClusterMismatch
	}
	n, exists := find(snap, req.Peer.NodeID)
	if exists && n.Removed {
		return Outcome{}, registry.ErrNodeRemoved
	}
	if exists && (n.Membership != "pending" && n.Membership != "active") {
		return Outcome{}, errors.New("unexpected member state")
	}
	if exists && !sameJoinRequest(n, req) {
		return Outcome{}, errors.New("pending/active member metadata differ; use explicit update instead")
	}
	if !exists {
		res, e := c.apply(ctx, registry.Command{ClusterID: req.ClusterID, ExpectedRevision: snap.Revision, Type: registry.ChangeAdd, Node: nodeFrom(req, "pending")})
		if e != nil {
			return Outcome{}, e
		}
		snap.Revision = res.Revision
	}
	// The pending record MUST be committed BEFORE remote trust preparation.
	// Each follower independently verifies this Raft-committed admission intent
	// before pinning a candidate. If a voter is offline or a trust RPC fails,
	// keep pending and resume safely on a retry, never pretend admission succeeded.
	if err = c.readiness.VerifyPrepared(ctx, req.Peer, members); err != nil {
		return Outcome{ClusterID: req.ClusterID, NodeID: req.Peer.NodeID, Revision: snap.Revision, State: "pending"}, fmt.Errorf("%w: %v", ErrUnprepared, err)
	}
	// A follower's configuration should not be rewritten; only the leader can
	// add a voter. Retry after outage reads the current Raft configuration.
	members, err = c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("%w: read raft configuration: %v", ErrIncomplete, err)
	}
	if m, ok := findVoter(members, req.Peer.NodeID); ok {
		if !m.Voting || m.RaftAddress != req.RaftAddress {
			return Outcome{}, fmt.Errorf("%w: unexpected raft member configuration", ErrIncomplete)
		}
	} else if err = c.raft.AddVoter(ctx, req.Peer.NodeID, req.RaftAddress); err != nil {
		return Outcome{ClusterID: req.ClusterID, NodeID: req.Peer.NodeID, Revision: snap.Revision, State: "pending"}, fmt.Errorf("%w: AddVoter: %v", ErrIncomplete, err)
	}
	fresh, err := c.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return Outcome{}, fmt.Errorf("%w: read pending registry: %v", ErrIncomplete, err)
	}
	n, exists = find(fresh, req.Peer.NodeID)
	if !exists || n.Removed {
		return Outcome{}, fmt.Errorf("%w: pending record was changed", ErrIncomplete)
	}
	if n.Membership == "active" {
		return Outcome{ClusterID: req.ClusterID, NodeID: req.Peer.NodeID, Revision: fresh.Revision, State: "active", Recovered: true}, nil
	}
	res, err := c.apply(ctx, registry.Command{ClusterID: req.ClusterID, ExpectedRevision: fresh.Revision, Type: registry.ChangeUpdate, Node: nodeFrom(req, "active")})
	if err != nil {
		return Outcome{ClusterID: req.ClusterID, NodeID: req.Peer.NodeID, Revision: fresh.Revision, State: "pending"}, fmt.Errorf("%w: activation: %v", ErrIncomplete, err)
	}
	return Outcome{ClusterID: req.ClusterID, NodeID: req.Peer.NodeID, Revision: res.Revision, State: "active"}, nil
}

// Remove tombstones the shared directory FIRST, removes the voting member
// through Raft, and finally revokes trust. Retrying resumes remaining phases.
// Offline peer removal only needs quorum among surviving voters; it does not
// wait for the removed node's acknowledgement.
func (c *Controller) Remove(ctx context.Context, clusterID, nodeID string) (Outcome, error) {
	if err := c.authorize(ctx, "cluster.node.remove", nodeID); err != nil {
		return Outcome{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.leader(); err != nil {
		return Outcome{}, err
	}
	if nodeID == c.raft.NodeID() {
		return Outcome{}, ErrUnsafeRemoval
	}
	members, err := c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, err
	}
	if m, ok := findVoter(members, nodeID); ok && m.Voting {
		voters := 0
		for _, x := range members {
			if x.Voting {
				voters++
			}
		}
		if voters <= 1 {
			return Outcome{}, ErrUnsafeRemoval
		}
	}
	snap, err := c.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return Outcome{}, err
	}
	if snap.ClusterID != clusterID || snap.Revision == 0 {
		return Outcome{}, registry.ErrClusterMismatch
	}
	n, exists := find(snap, nodeID)
	if !exists {
		return Outcome{}, registry.ErrNotFound
	}
	if !n.Removed {
		res, e := c.apply(ctx, registry.Command{ClusterID: clusterID, ExpectedRevision: snap.Revision, Type: registry.ChangeRemove, NodeID: nodeID})
		if e != nil {
			return Outcome{}, e
		}
		snap.Revision = res.Revision
	}
	members, err = c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("%w: read raft config: %v", ErrIncomplete, err)
	}
	if _, ok := findVoter(members, nodeID); ok {
		if err = c.raft.RemoveVoter(ctx, nodeID); err != nil {
			return Outcome{ClusterID: clusterID, NodeID: nodeID, Revision: snap.Revision, State: "removing"}, fmt.Errorf("%w: RemoveServer: %v", ErrIncomplete, err)
		}
	}
	if err = c.localTrust.Revoke(nodeID); err != nil && !errors.Is(err, security.ErrUntrusted) {
		return Outcome{}, fmt.Errorf("%w: local revoke: %v", ErrIncomplete, err)
	}
	remaining, err := c.raft.Members(ctx)
	if err != nil {
		return Outcome{}, fmt.Errorf("%w: read remaining voters: %v", ErrIncomplete, err)
	}
	if err = c.readiness.RevokeEverywhere(ctx, nodeID, remaining); err != nil {
		return Outcome{ClusterID: clusterID, NodeID: nodeID, Revision: snap.Revision, State: "revoking"}, fmt.Errorf("%w: peer revoke: %v", ErrIncomplete, err)
	}
	return Outcome{ClusterID: clusterID, NodeID: nodeID, Revision: snap.Revision, State: "removed", Recovered: n.Removed}, nil
}

func (c *Controller) apply(ctx context.Context, cmd registry.Command) (registry.ApplyResult, error) {
	id, err := newOperation()
	if err != nil {
		return registry.ApplyResult{}, err
	}
	cmd.OperationID = id
	cmd.Timestamp = time.Now().UTC()
	return c.raft.Apply(ctx, cmd)
}
func nodeFrom(req JoinRequest, membership string) registry.NodeRecord {
	return registry.NodeRecord{NodeID: req.Peer.NodeID, ClusterID: req.ClusterID, HomeID: req.HomeID, Name: req.Name, Membership: membership, Endpoints: req.Endpoints}
}
func find(s registry.Snapshot, id string) (registry.NodeRecord, bool) {
	for _, n := range s.Nodes {
		if n.NodeID == id {
			return n, true
		}
	}
	return registry.NodeRecord{}, false
}
func findVoter(m []Voter, id string) (Voter, bool) {
	for _, v := range m {
		if v.NodeID == id {
			return v, true
		}
	}
	return Voter{}, false
}
func contains(s []string, v string) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}
func sameJoinRequest(n registry.NodeRecord, r JoinRequest) bool {
	return n.NodeID == r.Peer.NodeID && n.ClusterID == r.ClusterID && n.HomeID == r.HomeID && n.Name == r.Name && reflect.DeepEqual(n.Endpoints, r.Endpoints)
}
