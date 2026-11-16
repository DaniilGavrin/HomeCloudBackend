package preparedrpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"math/big"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
)

type fakeView struct {
	leader string
	voters []membership.Voter
}

func (v *fakeView) LeaderAddress() string                                 { return v.leader }
func (v *fakeView) Members(_ context.Context) ([]membership.Voter, error) { return v.voters, nil }

type fakeDir struct{ snap registry.Snapshot }

func (d *fakeDir) ReadSnapshot(_ context.Context, _ bool) (registry.Snapshot, error) {
	return d.snap, nil
}
func reserveControl(t *testing.T) (string, string) {
	t.Helper()
	for i := 0; i < 80; i++ {
		a, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			t.Fatal(e)
		}
		n, _ := strconv.Atoi(func() string { _, p, _ := net.SplitHostPort(a.Addr().String()); return p }())
		if n >= 65534 {
			a.Close()
			continue
		}
		b, e := net.Listen("tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(n+1)))
		if e != nil {
			a.Close()
			continue
		}
		_ = b.Close()
		_ = a.Close()
		return net.JoinHostPort("127.0.0.1", strconv.Itoa(n)), net.JoinHostPort("127.0.0.1", strconv.Itoa(n+1))
	}
	t.Fatal("no free paired ports")
	return "", ""
}
func keyPeer(t *testing.T, raft string) (security.Peer, tls.Certificate) {
	t.Helper()
	pub, priv, e := ed25519.GenerateKey(rand.Reader)
	if e != nil {
		t.Fatal(e)
	}
	id := security.NodeID(pub)
	cert := &x509.Certificate{SerialNumber: big.NewInt(99), Subject: pkix.Name{CommonName: id}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(8 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth}}
	der, e := x509.CreateCertificate(rand.Reader, cert, cert, priv.Public(), priv)
	if e != nil {
		t.Fatal(e)
	}
	return security.Peer{NodeID: id, PublicKey: base64.StdEncoding.EncodeToString(pub), Addresses: []string{raft}}, tls.Certificate{Certificate: [][]byte{der}, PrivateKey: priv}
}
func trust(t *testing.T, local string) *security.TrustStore {
	t.Helper()
	s, e := security.OpenTrustStore(filepath.Join(t.TempDir(), "trust"), local)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func approve(t *testing.T, s *security.TrustStore, p security.Peer) {
	t.Helper()
	if e := s.Approve(p); e != nil {
		t.Fatal(e)
	}
}
func launch(t *testing.T, addr string, c tls.Certificate, trustStore *security.TrustStore, g Gate) (*security.Listener, context.CancelFunc) {
	t.Helper()
	l, e := security.NewMembershipListener(addr, c, trustStore)
	if e != nil {
		t.Fatal(e)
	}
	s, e := NewServer(l, trustStore, g)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = s.Serve(ctx) }()
	t.Cleanup(func() {
		cancel()
		_ = l.Close()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("server didn't shut down")
		}
	})
	return l, cancel
}
func TestReciprocalPreparationAndRevocationOverMTLS(t *testing.T) {
	aRaft, aCtrl := reserveControl(t)
	bRaft, bCtrl := reserveControl(t)
	cRaft, cCtrl := reserveControl(t)
	a, aCert := keyPeer(t, aRaft)
	b, bCert := keyPeer(t, bRaft)
	c, cCert := keyPeer(t, cRaft)
	at, bt, ct := trust(t, a.NodeID), trust(t, b.NodeID), trust(t, c.NodeID)
	approve(t, at, b)
	approve(t, at, c)
	approve(t, bt, a)
	approve(t, ct, a) // candidate local owner approved the leader
	voters := []membership.Voter{{NodeID: a.NodeID, RaftAddress: aRaft, Voting: true}, {NodeID: b.NodeID, RaftAddress: bRaft, Voting: true}}
	view := &fakeView{leader: aRaft, voters: voters}
	d := &fakeDir{snap: registry.Snapshot{ClusterID: "test-cluster", Revision: 1, Nodes: []registry.NodeRecord{{NodeID: a.NodeID, Membership: "active"}, {NodeID: b.NodeID, Membership: "active"}, {NodeID: c.NodeID, Membership: "pending"}}}}
	la, _ := launch(t, aCtrl, aCert, at, VoterGate{ClusterID: "test-cluster", Raft: view, Directory: d})
	_, _ = launch(t, bCtrl, bCert, bt, VoterGate{ClusterID: "test-cluster", Raft: view, Directory: d})
	_, _ = launch(t, cCtrl, cCert, ct, CandidateGate{ClusterID: "test-cluster", ApprovedLeaderID: a.NodeID, EnrollmentExpiresAt: time.Now().Add(time.Minute)})
	cl := &Client{Layer: la, Trust: at, Self: a, ClusterID: "test-cluster", Timeout: 3 * time.Second}
	barrier := membership.StrictReadiness{LocalNodeID: a.NodeID, Trust: at, RPC: cl}
	if e := barrier.VerifyPrepared(context.Background(), c, voters); e != nil {
		t.Fatal(e)
	}
	if _, ok := bt.Peer(c.NodeID); !ok {
		t.Fatal("follower did not pin candidate")
	}
	if _, ok := ct.Peer(b.NodeID); !ok {
		t.Fatal("candidate did not pin voter")
	}
	// No tombstone = no remote revocation, despite fully authenticated leader.
	if e := cl.RevokeOnVoter(context.Background(), voters[1], c.NodeID); e == nil {
		t.Fatal("revoke without tombstone accepted")
	}
	d.snap.Nodes = append(d.snap.Nodes, registry.NodeRecord{NodeID: c.NodeID, Removed: true, Membership: "removed"})
	if e := barrier.RevokeEverywhere(context.Background(), c.NodeID, voters); e != nil {
		t.Fatal(e)
	}
	if _, ok := bt.Peer(c.NodeID); ok {
		t.Fatal("removed candidate remains trusted")
	}
	// An old leader with a valid TLS certificate is NOT authorized after election.
	view.leader = bRaft
	if e := cl.AssertTrustedOnVoter(context.Background(), voters[1], c); e == nil {
		t.Fatal("former leader was accepted")
	}
}
func TestCandidateWithoutLocalAuthorizationDenies(t *testing.T) {
	aRaft, aCtrl := reserveControl(t)
	cRaft, cCtrl := reserveControl(t)
	a, aCert := keyPeer(t, aRaft)
	c, cCert := keyPeer(t, cRaft)
	at, ct := trust(t, a.NodeID), trust(t, c.NodeID)
	approve(t, at, c)
	approve(t, ct, a)
	la, _ := launch(t, aCtrl, aCert, at, CandidateGate{ClusterID: "test-cluster", ApprovedLeaderID: c.NodeID, EnrollmentExpiresAt: time.Now().Add(time.Minute)})
	_, _ = launch(t, cCtrl, cCert, ct, CandidateGate{ClusterID: "test-cluster", ApprovedLeaderID: a.NodeID, EnrollmentExpiresAt: time.Now().Add(-time.Minute)})
	cl := &Client{Layer: la, Trust: at, Self: a, ClusterID: "test-cluster", Timeout: 2 * time.Second}
	err := cl.AssertVoterTrustOnCandidate(context.Background(), c, []membership.Voter{{NodeID: a.NodeID, RaftAddress: aRaft, Voting: true}})
	if err == nil {
		t.Fatal("expired candidate permission accepted")
	}
}
func TestUnauthorisedVoterCannotDistributeTrust(t *testing.T) {
	aRaft, _ := reserveControl(t)
	bRaft, bCtrl := reserveControl(t)
	cRaft, _ := reserveControl(t)
	a, _ := keyPeer(t, aRaft)
	b, bCert := keyPeer(t, bRaft)
	c, _ := keyPeer(t, cRaft)
	bt := trust(t, b.NodeID)
	approve(t, bt, a)
	view := &fakeView{leader: aRaft, voters: []membership.Voter{{NodeID: a.NodeID, RaftAddress: aRaft, Voting: true}, {NodeID: b.NodeID, RaftAddress: bRaft, Voting: true}}}
	d := &fakeDir{snap: registry.Snapshot{ClusterID: "test-cluster", Revision: 1}}
	l, _ := launch(t, bCtrl, bCert, bt, VoterGate{ClusterID: "test-cluster", Raft: view, Directory: d})
	_ = l
	// If a caller is not the Raft leader it must fail before TrustStore.Approve.
	g := VoterGate{ClusterID: "test-cluster", Raft: view, Directory: d}
	if e := g.Authorize(context.Background(), b.NodeID, "test-cluster", actionPreparePeer, c.NodeID); e == nil {
		t.Fatal("nonleader authorised")
	}
	if _, ok := bt.Peer(c.NodeID); ok {
		t.Fatal("candidate unexpectedly trusted")
	}
}

type integrationAdmin struct{}

func (integrationAdmin) Authorize(context.Context, string, string) error { return nil } // isolated test-only owner adapter

type integrationRaft struct {
	id         string
	members    []membership.Voter
	leader     bool
	directory  *fakeDir
	replicated *fakeDir
}

func (r *integrationRaft) NodeID() string { return r.id }
func (r *integrationRaft) IsLeader() bool { return r.leader }
func (r *integrationRaft) Members(context.Context) ([]membership.Voter, error) {
	return append([]membership.Voter{}, r.members...), nil
}
func (r *integrationRaft) Apply(_ context.Context, cmd registry.Command) (registry.ApplyResult, error) {
	snap := r.directory.snap
	if cmd.ExpectedRevision != snap.Revision {
		return registry.ApplyResult{}, registry.ErrRevisionConflict
	}
	snap.Revision++
	switch cmd.Type {
	case registry.ChangeAdd:
		cmd.Node.Revision = snap.Revision
		snap.Nodes = append(snap.Nodes, cmd.Node)
	case registry.ChangeUpdate:
		for i := range snap.Nodes {
			if snap.Nodes[i].NodeID == cmd.Node.NodeID {
				snap.Nodes[i] = cmd.Node
				snap.Nodes[i].Revision = snap.Revision
			}
		}
	case registry.ChangeRemove:
		for i := range snap.Nodes {
			if snap.Nodes[i].NodeID == cmd.NodeID {
				snap.Nodes[i].Removed = true
				snap.Nodes[i].Membership = "removed"
				snap.Nodes[i].Endpoints = nil
			}
		}
	}
	r.directory.snap = snap
	r.replicated.snap = snap // test-only simulation of committed replicated snapshot
	return registry.ApplyResult{Revision: snap.Revision}, nil
}
func (r *integrationRaft) AddVoter(_ context.Context, id, addr string) error {
	r.members = append(r.members, membership.Voter{NodeID: id, RaftAddress: addr, Voting: true})
	return nil
}
func (r *integrationRaft) RemoveVoter(_ context.Context, id string) error {
	for i, p := range r.members {
		if p.NodeID == id {
			r.members = append(r.members[:i], r.members[i+1:]...)
			return nil
		}
	}
	return nil
}
func TestControllerJoinAndRemoveThroughAuthenticatedRPC(t *testing.T) {
	aRaft, aCtrl := reserveControl(t)
	bRaft, bCtrl := reserveControl(t)
	cRaft, cCtrl := reserveControl(t)
	a, aCert := keyPeer(t, aRaft)
	b, bCert := keyPeer(t, bRaft)
	c, cCert := keyPeer(t, cRaft)
	at, bt, ct := trust(t, a.NodeID), trust(t, b.NodeID), trust(t, c.NodeID)
	approve(t, at, b)
	approve(t, at, c)
	approve(t, bt, a)
	approve(t, ct, a)
	members := []membership.Voter{{NodeID: a.NodeID, RaftAddress: aRaft, Voting: true}, {NodeID: b.NodeID, RaftAddress: bRaft, Voting: true}}
	view := &fakeView{leader: aRaft, voters: members}
	snap := registry.Snapshot{ClusterID: "test-cluster", Revision: 1, Nodes: []registry.NodeRecord{{NodeID: a.NodeID, ClusterID: "test-cluster", Name: "Kostroma", Membership: "active"}, {NodeID: b.NodeID, ClusterID: "test-cluster", Name: "Yaroslavl", Membership: "active"}}}
	leaderDir := &fakeDir{snap: snap}
	followerDir := &fakeDir{snap: snap}
	layer, _ := launch(t, aCtrl, aCert, at, VoterGate{ClusterID: "test-cluster", Raft: view, Directory: leaderDir})
	_, _ = launch(t, bCtrl, bCert, bt, VoterGate{ClusterID: "test-cluster", Raft: view, Directory: followerDir})
	_, _ = launch(t, cCtrl, cCert, ct, CandidateGate{ClusterID: "test-cluster", ApprovedLeaderID: a.NodeID, EnrollmentExpiresAt: time.Now().Add(time.Minute)})
	client := &Client{Layer: layer, Trust: at, Self: a, ClusterID: "test-cluster", Timeout: 3 * time.Second}
	raft := &integrationRaft{id: a.NodeID, members: members, leader: true, directory: leaderDir, replicated: followerDir}
	pairing := security.NewPairingManager(at)
	gate := membership.StrictReadiness{LocalNodeID: a.NodeID, Trust: at, RPC: client}
	ctrl, err := membership.NewController(raft, leaderDir, at, pairing, integrationAdmin{}, gate)
	if err != nil {
		t.Fatal(err)
	}
	joined, err := ctrl.Join(context.Background(), membership.JoinRequest{ClusterID: "test-cluster", HomeID: "moscow", Name: "Moscow", Peer: c, RaftAddress: cRaft, Endpoints: []registry.Endpoint{{Network: "tailscale", Protocol: "ws", Host: "100.81.2.3", Port: 8080}}})
	if err != nil || joined.State != "active" {
		t.Fatalf("join=%+v err=%v", joined, err)
	}
	if _, ok := bt.Peer(c.NodeID); !ok {
		t.Fatal("join did not prepare follower")
	}
	if _, ok := ct.Peer(b.NodeID); !ok {
		t.Fatal("join did not prepare candidate")
	}
	removed, err := ctrl.Remove(context.Background(), "test-cluster", c.NodeID)
	if err != nil || removed.State != "removed" {
		t.Fatalf("remove=%+v err=%v", removed, err)
	}
	if _, ok := bt.Peer(c.NodeID); ok {
		t.Fatal("removed peer remains trusted on follower")
	}
	if _, ok := at.Peer(c.NodeID); ok {
		t.Fatal("removed peer remains trusted on leader")
	}
	if leaderDir.snap.Revision != 4 {
		t.Fatalf("unexpected registry rev %d", leaderDir.snap.Revision)
	}
}
