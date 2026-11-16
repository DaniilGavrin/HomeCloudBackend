package runtime

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"HomeCloudBackend/internal/cluster/consensus"
	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/membership/preparedrpc"
	"HomeCloudBackend/internal/cluster/membership/raftadapter"
	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
)

type Options struct {
	DataDir      string   // absolute local data/cluster directory; never shared with another node
	RaftAddress  string   // concrete private interface IP:port; port+1 control, +2 first use
	AllowedCIDRs []string // explicit private local/Tailscale networks
	Identity     *identity.Store
	Directory    *registry.Store
	// OwnerAuthorizer is supplied by the future AUTHENTICATED UI/owner API.
	// Nil prevents cluster modifications; it does NOT grant anonymous WS privileges.
	OwnerAuthorizer membership.Authorizer
}

type Manager struct {
	mu                sync.Mutex
	opts              Options
	nodeID            string
	local             identity.Identity
	trust             *security.TrustStore
	networks          security.PrivateNetworks
	enroll            *security.FirstUseServer
	raft              *consensus.Service
	control           *preparedrpc.Node
	directory         *registry.Store
	state             persisted
	invitationExpires time.Time
	cancel            context.CancelFunc
	closed            bool
}

// Open starts first-use TLS on a PRIVATE interface and resumes previously
// persisted Raft state. A fresh device remains standalone; it must not create
// an incompatible Raft singleton merely because `serve` was launched.
func Open(ctx context.Context, opts Options) (_ *Manager, err error) {
	if opts.Identity == nil || opts.Directory == nil || !filepath.IsAbs(opts.DataDir) {
		return nil, errors.New("cluster runtime requires persistent identity, registry and absolute data directory")
	}
	networks, err := security.NewPrivateNetworks(opts.AllowedCIDRs...)
	if err != nil {
		return nil, err
	}
	// Verify both the local bound address and all advertised allowed subnets.
	if err := CheckPrivateRaftAddress(opts.RaftAddress, networks); err != nil {
		return nil, err
	}
	if err = opts.Directory.VerifySchema(ctx); err != nil {
		return nil, err
	}
	local, err := opts.Identity.LoadOrCreate()
	if err != nil {
		return nil, err
	}
	trust, err := security.OpenTrustStore(filepath.Join(opts.DataDir, "trust"), local.NodeID)
	if err != nil {
		return nil, err
	}
	cert, err := opts.Identity.RaftTLSCertificate()
	if err != nil {
		return nil, err
	}
	first, err := security.OpenFirstUseServer(opts.RaftAddress, cert, trust, networks)
	if err != nil {
		return nil, err
	}
	first.SetCertificateProvider(opts.Identity.RaftTLSCertificate)
	runCtx, cancel := context.WithCancel(ctx)
	m := &Manager{opts: opts, nodeID: local.NodeID, local: local, trust: trust, networks: networks, enroll: first, directory: opts.Directory, cancel: cancel}
	first.SetJoinHandler(m.onInvitation)
	previous, exists, e := readState(m.statePath(), opts.RaftAddress)
	if e != nil {
		cancel()
		first.Close()
		return nil, e
	}
	if exists {
		m.state = previous
		if e = m.startLocked(previous.Mode == "bootstrapping" && !filePresent(filepath.Join(m.raftPath(), "raft.db"))); e != nil {
			cancel()
			first.Close()
			return nil, e
		}
	}
	go func() { _ = first.Serve(runCtx) }()
	go m.maintenance(runCtx)
	return m, nil
}

func CheckPrivateRaftAddress(address string, n security.PrivateNetworks) error {
	// NewPrivateNetworks' checkEndpoint is deliberately private to security.
	// Use explicit exported validation below to avoid bypassing CIDR policy.
	return security.ValidatePrivateRaftEndpoint(address, n)
}

func (m *Manager) statePath() string { return filepath.Join(m.opts.DataDir, "runtime.json") }
func (m *Manager) raftPath() string  { return filepath.Join(m.opts.DataDir, "raft-secure") }
func (m *Manager) statusLocked() string {
	if m.raft == nil {
		return "standalone"
	}
	return m.state.Mode
}
func (m *Manager) Status() (string, string, string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.statusLocked(), m.state.ClusterID, m.nodeID
}
func (m *Manager) startLocked(bootstrap bool) error {
	if m.raft != nil {
		return errors.New("Raft runtime already started")
	}
	service, e := consensus.OpenSecure(consensus.SecureOptions{
		NodeID: m.nodeID, BindAddr: m.opts.RaftAddress, DataDir: m.raftPath(), Bootstrap: bootstrap,
		Identity: m.opts.Identity, Trust: m.trust,
	}, m.directory)
	if e != nil {
		return e
	}
	m.raft = service
	control, e := preparedrpc.Open(context.Background(), m.opts.RaftAddress, m.state.ClusterID, m.opts.Identity, m.trust, dynamicGate{owner: m})
	if e != nil {
		_ = service.Close()
		m.raft = nil
		return e
	}
	m.control = control
	return nil
}

// onInvitation is called from mTLS FirstUse after transport peer identity is
// verified, pinned, and the inviter supplies a cluster ID. It provisions a
// NONBOOTSTRAPPED candidate with a short-lived automatically trusted leader.
func (m *Manager) onInvitation(ctx context.Context, inviter security.Peer, clusterID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(clusterID) < 3 || len(clusterID) > 128 {
		return errors.New("invalid invited cluster ID")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("node is shutting down")
	}
	if m.trust.IsRevoked(inviter.NodeID) {
		return security.ErrUntrusted
	}
	if m.state.ClusterID != "" {
		if m.state.ClusterID != clusterID || m.state.Mode != "joining" || m.state.LeaderID != inviter.NodeID {
			return errors.New("already belongs to another cluster or invitation")
		}
		m.invitationExpires = time.Now().Add(4 * time.Minute)
		return nil
	}
	snapshot, err := m.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return err
	}
	if snapshot.Revision != 0 || snapshot.ClusterID != "" {
		return errors.New("cannot join: registry already initialized")
	}
	if err := m.trust.Approve(inviter); err != nil {
		return err
	}
	m.state = persisted{Version: stateVersion, ClusterID: clusterID, Mode: "joining", RaftAddress: m.opts.RaftAddress, LeaderID: inviter.NodeID}
	if err = saveState(m.statePath(), m.state); err != nil {
		m.state = persisted{}
		return err
	}
	m.invitationExpires = time.Now().Add(4 * time.Minute)
	if err = m.startLocked(false); err != nil {
		return fmt.Errorf("start unbootstrapped candidate: %w", err)
	}
	return nil
}

func (m *Manager) controllerLocked() (*membership.Controller, error) {
	if m.raft == nil || m.control == nil {
		return nil, errors.New("Raft not running")
	}
	// PairingManager exists for backward compatibility, but the IP-only flow
	// never calls BeginPairing, VerifyPairingProof or DecidePairing.
	return membership.NewController(raftadapter.Adapter{Service: m.raft}, m.directory, m.trust, security.NewPairingManager(m.trust), m.opts.OwnerAuthorizer, m.control.Readiness(m.nodeID, m.trust))
}

func (m *Manager) requireOwner(ctx context.Context, action, peer string) error {
	if m.opts.OwnerAuthorizer == nil {
		return membership.ErrUnauthorized
	}
	return m.opts.OwnerAuthorizer.Authorize(ctx, action, peer)
}

// Bootstrap creates a singleton Raft cluster only when an authenticated
// owner explicitly creates their FIRST HomeCloud cluster, not on every serve.
func (m *Manager) Bootstrap(ctx context.Context, homeID, name string, endpoints []registry.Endpoint) (membership.Outcome, error) {
	if err := m.requireOwner(ctx, "cluster.bootstrap", m.nodeID); err != nil {
		return membership.Outcome{}, err
	}
	m.mu.Lock()
	if m.closed || m.state.ClusterID != "" {
		m.mu.Unlock()
		return membership.Outcome{}, errors.New("cluster already initialized or closing")
	}
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		m.mu.Unlock()
		return membership.Outcome{}, err
	}
	id := "cluster-" + hex.EncodeToString(idBytes)
	m.state = persisted{Version: stateVersion, ClusterID: id, Mode: "bootstrapping", RaftAddress: m.opts.RaftAddress, BootstrapHomeID: homeID, BootstrapName: name, BootstrapEndpoints: endpoints}
	if err := saveState(m.statePath(), m.state); err != nil {
		m.state = persisted{}
		m.mu.Unlock()
		return membership.Outcome{}, err
	}
	err := m.startLocked(true)
	m.mu.Unlock()
	if err != nil {
		return membership.Outcome{}, err
	}
	waitCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	for {
		m.mu.Lock()
		raftSvc := m.raft
		m.mu.Unlock()
		if (raftadapter.Adapter{Service: raftSvc}).IsLeader() {
			return m.resumeBootstrap(waitCtx)
		}
		select {
		case <-waitCtx.Done():
			return membership.Outcome{}, waitCtx.Err()
		case <-time.After(150 * time.Millisecond):
		}
	}
}

// JoinByIP is an INTERNAL owner-authenticated Go operation. KAN-11 will give
// the frontend an authenticated control-plane call; anonymous /ws never gets it.
// It must be executed on the current leader or fail closed with ErrNotLeader.
func (m *Manager) JoinByIP(ctx context.Context, targetRaft, homeID, name string, endpoints []registry.Endpoint) (membership.Outcome, error) {
	if err := m.requireOwner(ctx, "cluster.node.join", targetRaft); err != nil {
		return membership.Outcome{}, err
	}
	m.mu.Lock()
	if m.closed || m.raft == nil || m.state.ClusterID == "" {
		m.mu.Unlock()
		return membership.Outcome{}, errors.New("cluster is not initialized")
	}
	id, clusterID, localAddr := m.nodeID, m.state.ClusterID, m.opts.RaftAddress
	svc := m.raft
	m.mu.Unlock()
	if !(raftadapter.Adapter{Service: svc}).IsLeader() {
		return membership.Outcome{}, membership.ErrNotLeader
	}
	if err := CheckPrivateRaftAddress(targetRaft, m.networks); err != nil {
		return membership.Outcome{}, err
	}
	certificate, err := m.opts.Identity.RaftTLSCertificate()
	if err != nil {
		return membership.Outcome{}, err
	}
	peer, err := security.TrustFirstUseWithCluster(ctx, localAddr, targetRaft, clusterID, certificate, m.trust, m.networks)
	if err != nil {
		return membership.Outcome{}, err
	}
	if peer.NodeID == id {
		return membership.Outcome{}, errors.New("cannot add self")
	}
	m.mu.Lock()
	controller, e := m.controllerLocked()
	m.mu.Unlock()
	if e != nil {
		return membership.Outcome{}, e
	}
	joinCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		joinCtx, cancel = context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
	}
	req := membership.JoinRequest{ClusterID: clusterID, HomeID: homeID, Name: name, Peer: peer, RaftAddress: targetRaft, Endpoints: endpoints}
	var result membership.Outcome
	for {
		result, err = controller.Join(joinCtx, req)
		if err == nil {
			return result, nil
		}
		if !errors.Is(err, membership.ErrUnprepared) && !errors.Is(err, membership.ErrIncomplete) {
			return result, err
		}
		select {
		case <-joinCtx.Done():
			return result, fmt.Errorf("join is incomplete (%s), safe to retry: %w", result.State, err)
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func (m *Manager) Remove(ctx context.Context, nodeID string) (membership.Outcome, error) {
	if err := m.requireOwner(ctx, "cluster.node.remove", nodeID); err != nil {
		return membership.Outcome{}, err
	}
	m.mu.Lock()
	controller, e := m.controllerLocked()
	clusterID := m.state.ClusterID
	m.mu.Unlock()
	if e != nil {
		return membership.Outcome{}, e
	}
	return controller.Remove(ctx, clusterID, nodeID)
}
func (m *Manager) maintenance(ctx context.Context) {
	ticker := time.NewTicker(7 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.mu.Lock()
			recovering := m.state.Mode == "bootstrapping"
			m.mu.Unlock()
			if recovering {
				recoverCtx, recoverCancel := context.WithTimeout(ctx, 7*time.Second)
				_, _ = m.resumeBootstrap(recoverCtx)
				recoverCancel()
			}
			m.mu.Lock()
			svc := m.raft
			trust := m.trust
			dir := m.directory
			state := m.state
			m.mu.Unlock()
			if svc == nil || state.ClusterID == "" {
				continue
			}
			c, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = membership.ReconcileRemoved(c, dir, trust)
			snap, err := dir.ReadSnapshot(c, true)
			cancel()
			if err != nil {
				continue
			}
			for _, n := range snap.Nodes {
				if n.NodeID == m.nodeID && !n.Removed && n.Membership == "active" && state.Mode == "joining" {
					m.mu.Lock()
					m.state.Mode = "active"
					_ = saveState(m.statePath(), m.state)
					m.mu.Unlock()
				}
			}
		}
	}
}

func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	m.closed = true
	m.cancel()
	first, control, svc := m.enroll, m.control, m.raft
	m.mu.Unlock()
	if first != nil {
		_ = first.Close()
	}
	if control != nil {
		_ = control.Close()
	}
	if svc != nil {
		return svc.Close()
	}
	return nil
}

// Helpers for future UI: non-sensitive read-only server identity metadata.
func (m *Manager) LocalIdentity() security.Peer {
	return security.Peer{NodeID: m.local.NodeID, PublicKey: m.local.PublicKey, Addresses: []string{m.opts.RaftAddress}}
}

func filePresent(p string) bool { _, err := os.Stat(p); return err == nil }

// resumeBootstrap completes an owner-initiated durable single-voter intent
// after a crash. It never initiates a fresh cluster on a standalone server.
func (m *Manager) resumeBootstrap(ctx context.Context) (membership.Outcome, error) {
	m.mu.Lock()
	state := m.state
	raftSvc := m.raft
	m.mu.Unlock()
	if state.Mode != "bootstrapping" || raftSvc == nil || state.BootstrapName == "" || strings.TrimSpace(state.ClusterID) == "" {
		return membership.Outcome{}, errors.New("no pending owner-approved bootstrap intent")
	}
	a := raftadapter.Adapter{Service: raftSvc}
	if !a.IsLeader() {
		return membership.Outcome{}, membership.ErrNotLeader
	}
	voters, err := a.Members(ctx)
	if err != nil {
		return membership.Outcome{}, err
	}
	if len(voters) != 1 || !voters[0].Voting || voters[0].NodeID != m.nodeID {
		return membership.Outcome{}, errors.New("bootstrap recovery requires only the local voter")
	}
	snap, err := m.directory.ReadSnapshot(ctx, true)
	if err != nil {
		return membership.Outcome{}, err
	}
	if snap.Revision == 0 && snap.ClusterID == "" && len(snap.Nodes) == 0 {
		cmd := registry.Command{OperationID: "bootstrap-" + state.ClusterID, ClusterID: state.ClusterID, ExpectedRevision: 0, Type: registry.ChangeAdd, Timestamp: time.Now().UTC(), Node: registry.NodeRecord{NodeID: m.nodeID, ClusterID: state.ClusterID, HomeID: state.BootstrapHomeID, Name: state.BootstrapName, Membership: "active", Endpoints: state.BootstrapEndpoints}}
		if _, err = a.Apply(ctx, cmd); err != nil {
			return membership.Outcome{}, err
		}
		snap, err = m.directory.ReadSnapshot(ctx, true)
		if err != nil {
			return membership.Outcome{}, err
		}
	}
	if snap.ClusterID != state.ClusterID || len(snap.Nodes) != 1 || snap.Nodes[0].NodeID != m.nodeID || snap.Nodes[0].Removed || snap.Nodes[0].Membership != "active" {
		return membership.Outcome{}, errors.New("bootstrap registry state conflicts with durable intent")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state.ClusterID != state.ClusterID || m.state.Mode != "bootstrapping" {
		return membership.Outcome{}, errors.New("bootstrap state changed")
	}
	m.state.Mode = "active"
	if err = saveState(m.statePath(), m.state); err != nil {
		return membership.Outcome{}, err
	}
	return membership.Outcome{NodeID: m.nodeID, ClusterID: state.ClusterID, State: "active", Revision: snap.Revision}, nil
}
