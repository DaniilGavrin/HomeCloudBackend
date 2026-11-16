package membership

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
)

type testAdmin struct {
	deny    bool
	actions []string
}

func (a *testAdmin) Authorize(_ context.Context, action, id string) error {
	a.actions = append(a.actions, action+":"+id)
	if a.deny {
		return errors.New("not owner")
	}
	return nil
}

type testBarrier struct {
	ready       bool
	revoke      bool
	calls       int
	failsRevoke bool
}

func (b *testBarrier) VerifyPrepared(_ context.Context, _ security.Peer, voters []Voter) error {
	b.calls++
	if !b.ready || len(voters) == 0 {
		return errors.New("missing trust proof")
	}
	return nil
}
func (b *testBarrier) RevokeEverywhere(_ context.Context, _ string, _ []Voter) error {
	if b.failsRevoke {
		return errors.New("remote peer unreachable")
	}
	b.revoke = true
	return nil
}

type testDir struct{ snap registry.Snapshot }

func (d *testDir) ReadSnapshot(_ context.Context, _ bool) (registry.Snapshot, error) {
	return d.snap, nil
}

type testRaft struct {
	id            string
	leader        bool
	quorum        bool
	members       []Voter
	dir           *testDir
	failAdd       bool
	failActivate  bool
	failRemove    bool
	adds, removes int
}

func (r *testRaft) NodeID() string { return r.id }
func (r *testRaft) IsLeader() bool { return r.leader }
func (r *testRaft) Members(_ context.Context) ([]Voter, error) {
	return append([]Voter{}, r.members...), nil
}
func (r *testRaft) Apply(_ context.Context, c registry.Command) (registry.ApplyResult, error) {
	if !r.quorum {
		return registry.ApplyResult{}, errors.New("no quorum")
	}
	if r.failActivate && c.Type == registry.ChangeUpdate && c.Node.Membership == "active" {
		return registry.ApplyResult{}, errors.New("temporary log outage")
	}
	if c.ClusterID != r.dir.snap.ClusterID && r.dir.snap.Revision != 0 {
		return registry.ApplyResult{}, registry.ErrClusterMismatch
	}
	if c.ExpectedRevision != r.dir.snap.Revision {
		return registry.ApplyResult{}, registry.ErrRevisionConflict
	}
	r.dir.snap.Revision++
	r.dir.snap.ClusterID = c.ClusterID
	switch c.Type {
	case registry.ChangeAdd:
		if _, ok := find(r.dir.snap, c.Node.NodeID); ok {
			return registry.ApplyResult{}, registry.ErrNodeExists
		}
		c.Node.Revision = r.dir.snap.Revision
		r.dir.snap.Nodes = append(r.dir.snap.Nodes, c.Node)
	case registry.ChangeUpdate:
		for i, n := range r.dir.snap.Nodes {
			if n.NodeID == c.Node.NodeID {
				c.Node.Revision = r.dir.snap.Revision
				r.dir.snap.Nodes[i] = c.Node
			}
		}
	case registry.ChangeRemove:
		for i, n := range r.dir.snap.Nodes {
			if n.NodeID == c.NodeID {
				r.dir.snap.Nodes[i].Removed = true
				r.dir.snap.Nodes[i].Membership = "removed"
				r.dir.snap.Nodes[i].Revision = r.dir.snap.Revision
				r.dir.snap.Nodes[i].Endpoints = nil
			}
		}
	}
	return registry.ApplyResult{Revision: r.dir.snap.Revision}, nil
}
func (r *testRaft) AddVoter(_ context.Context, id, addr string) error {
	r.adds++
	if r.failAdd {
		return errors.New("simulated add failure")
	}
	r.members = append(r.members, Voter{NodeID: id, RaftAddress: addr, Voting: true})
	return nil
}
func (r *testRaft) RemoveVoter(_ context.Context, id string) error {
	r.removes++
	if r.failRemove {
		return errors.New("simulated remove failure")
	}
	for i, v := range r.members {
		if v.NodeID == id {
			r.members = append(r.members[:i], r.members[i+1:]...)
			break
		}
	}
	return nil
}

type fixture struct {
	ctl     *Controller
	dir     *testDir
	raft    *testRaft
	admin   *testAdmin
	barrier *testBarrier
	peer    security.Peer
	trust   *security.TrustStore
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	p0, _, _ := ed25519.GenerateKey(rand.Reader)
	p1, _, _ := ed25519.GenerateKey(rand.Reader)
	selfID := security.NodeID(p0)
	trust, err := security.OpenTrustStore(filepath.Join(t.TempDir(), "trust"), selfID)
	if err != nil {
		t.Fatal(err)
	}
	peer := security.Peer{NodeID: security.NodeID(p1), PublicKey: base64.StdEncoding.EncodeToString(p1), Addresses: []string{"127.0.0.1:19002"}}
	if err := trust.Approve(peer); err != nil {
		t.Fatal(err)
	}
	dir := &testDir{registry.Snapshot{ClusterID: "demo-cluster", Revision: 1, Nodes: []registry.NodeRecord{{NodeID: selfID, ClusterID: "demo-cluster", Name: "Kostroma", Membership: "active"}}}}
	r := &testRaft{id: selfID, leader: true, quorum: true, dir: dir, members: []Voter{{NodeID: selfID, RaftAddress: "127.0.0.1:19001", Voting: true}}}
	admin := &testAdmin{}
	barrier := &testBarrier{ready: true}
	ctl, err := NewController(r, dir, trust, security.NewPairingManager(trust), admin, barrier)
	if err != nil {
		t.Fatal(err)
	}
	return &fixture{ctl: ctl, dir: dir, raft: r, admin: admin, barrier: barrier, peer: peer, trust: trust}
}
func (f *fixture) request() JoinRequest {
	return JoinRequest{ClusterID: "demo-cluster", HomeID: "moscow", Name: "Moscow", Peer: f.peer, RaftAddress: "127.0.0.1:19002", Endpoints: []registry.Endpoint{{Network: "tailscale", Protocol: "ws", Host: "100.90.1.3", Port: 8080, Priority: 10}}}
}
func TestJoinAndOfflineRemoval(t *testing.T) {
	f := newFixture(t)
	out, err := f.ctl.Join(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	if out.State != "active" || f.raft.adds != 1 || f.dir.snap.Revision != 3 {
		t.Fatalf("join failed: %+v; raft %#v", out, f.raft)
	}
	removed, err := f.ctl.Remove(context.Background(), "demo-cluster", f.peer.NodeID)
	if err != nil {
		t.Fatal(err)
	}
	if removed.State != "removed" || f.raft.removes != 1 || !f.barrier.revoke {
		t.Fatalf("remove failed %+v", removed)
	}
	node, ok := find(f.dir.snap, f.peer.NodeID)
	if !ok || !node.Removed || len(node.Endpoints) != 0 {
		t.Fatalf("no tombstone %+v", node)
	}
	if _, ok = f.trust.Peer(f.peer.NodeID); ok {
		t.Fatal("trust was not revoked")
	}
	if _, err = f.ctl.Join(context.Background(), f.request()); !errors.Is(err, security.ErrUntrusted) {
		t.Fatalf("removed peer still trusted: %v", err)
	}
}
func TestJoinRequiresOwnerAndAllVoterTrust(t *testing.T) {
	f := newFixture(t)
	f.admin.deny = true
	if _, err := f.ctl.Join(context.Background(), f.request()); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unauthorized join: %v", err)
	}
	if f.dir.snap.Revision != 1 {
		t.Fatal("unauthorized write")
	}
	f.admin.deny = false
	f.barrier.ready = false
	if _, err := f.ctl.Join(context.Background(), f.request()); !errors.Is(err, ErrUnprepared) {
		t.Fatalf("trust not checked: %v", err)
	}
	if f.raft.adds != 0 {
		t.Fatal("Raft add despite missing trust")
	}
}
func TestJoinRecoversAfterRaftAddFailure(t *testing.T) {
	f := newFixture(t)
	f.raft.failAdd = true
	result, err := f.ctl.Join(context.Background(), f.request())
	if !errors.Is(err, ErrIncomplete) || result.State != "pending" {
		t.Fatalf("expected pending: %+v, %v", result, err)
	}
	f.raft.failAdd = false
	result, err = f.ctl.Join(context.Background(), f.request())
	if err != nil || result.State != "active" || f.raft.adds != 2 {
		t.Fatalf("recovery failed: %+v %v", result, err)
	}
}
func TestJoinRecoversAfterActivationFailure(t *testing.T) {
	f := newFixture(t)
	f.raft.failActivate = true
	result, err := f.ctl.Join(context.Background(), f.request())
	if !errors.Is(err, ErrIncomplete) || result.State != "pending" {
		t.Fatalf("activation failure not handled: %+v %v", result, err)
	}
	f.raft.failActivate = false
	result, err = f.ctl.Join(context.Background(), f.request())
	if err != nil || result.State != "active" || f.raft.adds != 1 {
		t.Fatalf("duplicate voter add or no activation: %+v %v", result, err)
	}
}
func TestRemoveResumesAfterConfigFailure(t *testing.T) {
	f := newFixture(t)
	_, err := f.ctl.Join(context.Background(), f.request())
	if err != nil {
		t.Fatal(err)
	}
	f.raft.failRemove = true
	r, err := f.ctl.Remove(context.Background(), "demo-cluster", f.peer.NodeID)
	if !errors.Is(err, ErrIncomplete) || r.State != "removing" {
		t.Fatalf("expected incomplete removal: %+v %v", r, err)
	}
	f.raft.failRemove = false
	r, err = f.ctl.Remove(context.Background(), "demo-cluster", f.peer.NodeID)
	if err != nil || r.State != "removed" || !r.Recovered {
		t.Fatalf("recovery failed %+v %v", r, err)
	}
}
func TestRemoveCannotRemoveSelfAndRejectsNoQuorum(t *testing.T) {
	f := newFixture(t)
	if _, err := f.ctl.Remove(context.Background(), "demo-cluster", f.raft.id); !errors.Is(err, ErrUnsafeRemoval) {
		t.Fatal(err)
	}
	f.raft.quorum = false
	if _, err := f.ctl.Join(context.Background(), f.request()); err == nil {
		t.Fatal("missing quorum accepted")
	}
	if f.raft.adds != 0 {
		t.Fatal("add voter without quorum")
	}
}
func TestBootstrapOnlySingleLeader(t *testing.T) {
	f := newFixture(t)
	f.dir.snap = registry.Snapshot{}
	r, err := f.ctl.BootstrapLocal(context.Background(), BootstrapRequest{ClusterID: "demo-cluster", Name: "Kostroma", Endpoints: []registry.Endpoint{{Network: "lan", Protocol: "ws", Host: "127.0.0.1", Port: 8080}}})
	if err != nil || r.State != "active" {
		t.Fatalf("bootstrap: %+v %v", r, err)
	}
	if _, err = f.ctl.BootstrapLocal(context.Background(), BootstrapRequest{ClusterID: "demo-cluster", Name: "Again"}); err == nil {
		t.Fatal("reinitialized cluster")
	}
}
func TestControllerRejectsMissingGuards(t *testing.T) {
	if _, err := NewController(nil, nil, nil, nil, nil, nil); !errors.Is(err, ErrUnprepared) {
		t.Fatalf("nil guards accepted: %v", err)
	}
	f := newFixture(t)
	f.raft.leader = false
	if _, err := f.ctl.Join(context.Background(), f.request()); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("nonleader accepted: %v", err)
	}
}
func TestPairingRequiresProofAndOwnerReview(t *testing.T) {
	f := newFixture(t)
	challenge, err := f.ctl.BeginPairing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err = f.ctl.DecidePairing(context.Background(), challenge.ID, true); err == nil {
		t.Fatal("approved without proof")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	peer := security.Peer{NodeID: security.NodeID(pub), PublicKey: base64.StdEncoding.EncodeToString(pub), Addresses: []string{"127.0.0.1:19004"}}
	challenge, err = f.ctl.BeginPairing(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := base64.StdEncoding.DecodeString(challenge.Nonce)
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(priv, append([]byte("HomeCloud/cluster/node-proof/v1\x00"), raw...))
	if err = f.ctl.VerifyPairingProof(context.Background(), challenge.ID, peer, signature); err != nil {
		t.Fatal(err)
	}
	f.admin.deny = true
	if err = f.ctl.DecidePairing(context.Background(), challenge.ID, true); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("approved without owner: %v", err)
	}
	f.admin.deny = false
	if err = f.ctl.DecidePairing(context.Background(), challenge.ID, true); err != nil {
		t.Fatal(err)
	}
	if _, ok := f.trust.Peer(peer.NodeID); !ok {
		t.Fatal("no trust grant")
	}
	if len(f.admin.actions) < 2 || f.admin.actions[len(f.admin.actions)-1] != fmt.Sprintf("cluster.pair.approve:%s", peer.NodeID) {
		t.Fatalf("authorization not tied to proven peer: %v", f.admin.actions)
	}
}

type mockRPC struct {
	reject                      bool
	voters, candidates, revokes int
}

func (m *mockRPC) AssertTrustedOnVoter(_ context.Context, _ Voter, _ security.Peer) error {
	m.voters++
	if m.reject {
		return errors.New("not prepared")
	}
	return nil
}
func (m *mockRPC) AssertVoterTrustOnCandidate(_ context.Context, _ security.Peer, _ []Voter) error {
	m.candidates++
	if m.reject {
		return errors.New("candidate not ready")
	}
	return nil
}
func (m *mockRPC) RevokeOnVoter(_ context.Context, _ Voter, _ string) error {
	m.revokes++
	if m.reject {
		return errors.New("remote revoke refused")
	}
	return nil
}
func TestStrictReadinessRequiresReciprocalRPC(t *testing.T) {
	f := newFixture(t)
	voters := []Voter{{NodeID: f.raft.id, RaftAddress: "127.0.0.1:19001", Voting: true}, {NodeID: "other", RaftAddress: "127.0.0.1:19003", Voting: true}}
	gate := StrictReadiness{LocalNodeID: f.raft.id, Trust: f.trust}
	if err := gate.VerifyPrepared(context.Background(), f.peer, voters); !errors.Is(err, ErrUnprepared) {
		t.Fatalf("accepted missing RPC: %v", err)
	}
	rpc := &mockRPC{reject: true}
	gate.RPC = rpc
	if err := gate.VerifyPrepared(context.Background(), f.peer, voters); !errors.Is(err, ErrUnprepared) {
		t.Fatalf("accepted unprepared voter: %v", err)
	}
	rpc.reject = false
	if err := gate.VerifyPrepared(context.Background(), f.peer, voters); err != nil {
		t.Fatal(err)
	}
	if rpc.voters != 2 || rpc.candidates != 1 {
		t.Fatalf("not all peers checked: %+v", rpc)
	}
	if err := gate.RevokeEverywhere(context.Background(), f.peer.NodeID, voters); err != nil {
		t.Fatal(err)
	}
	if rpc.revokes != 1 {
		t.Fatal("remaining voter not notified")
	}
}
