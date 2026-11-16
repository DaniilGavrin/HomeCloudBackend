// Package consensus replicates the KAN-6 registry using HashiCorp Raft.
// KAN-7 deliberately restricts Raft TCP to LOOPBACK for integration testing.
// Protected inter-city networking, pairing, membership authorization and UI
// commands are later KAN-8/9/10/11 tasks and MUST NOT use this raw TCP transport.
package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

var ErrNotLeader = errors.New("homecloud cluster: not the raft leader")

// Options are ONLY for a local developer cluster. Never accept these
// directly from the existing unauthenticated WebSocket or remote clients.
type Options struct {
	NodeID    string
	BindAddr  string // must be loopback IP:port, e.g. 127.0.0.1:18331
	DataDir   string // per-node local storage, NOT NFS
	Bootstrap bool   // true on exactly ONE fresh node, explicit action
}

type Service struct {
	raft        *raft.Raft
	transport   *raft.NetworkTransport
	bolt        *raftboltdb.BoltStore
	address     raft.ServerAddress
	nodeID      string
	secureTrust *security.TrustStore
	secureLayer *security.Listener
}

func loopbackAddress(addr string, allowZero bool) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("raft address %q: %w", addr, err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("KAN-7 raft transport is loopback-only until KAN-8: %q", addr)
	}
	if port == "0" && allowZero {
		return nil
	}
	_, err = net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	p, err := net.LookupPort("tcp", port)
	if err != nil || p < 1 || p > 65535 {
		return errors.New("raft requires a valid nonzero TCP port")
	}
	return nil
}

// Open starts one local Raft server. With Bootstrap=true it creates a
// singleton voting cluster ONLY if the Raft log/stable/snapshot stores are
// initially empty. Existing Raft state is never silently re-bootstrapped.
func Open(opts Options, store *registry.Store) (_ *Service, err error) {
	if store == nil || !strings.HasPrefix(opts.NodeID, "hc-") || len(opts.NodeID) != 35 {
		return nil, errors.New("persistent KAN-5 node identity required")
	}
	if opts.DataDir == "" || !filepath.IsAbs(opts.DataDir) {
		return nil, errors.New("raft DataDir must be an absolute local path")
	}
	if err = loopbackAddress(opts.BindAddr, true); err != nil {
		return nil, err
	}
	if err = os.MkdirAll(opts.DataDir, 0700); err != nil {
		return nil, err
	}
	if err = store.VerifySchema(context.Background()); err != nil {
		return nil, err
	}

	bolt, err := raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("open raft bolt store: %w", err)
	}
	var tr *raft.NetworkTransport
	defer func() {
		if err != nil {
			if tr != nil {
				_ = tr.Close()
			}
			_ = bolt.Close()
		}
	}()
	snapshots, err := raft.NewFileSnapshotStore(filepath.Join(opts.DataDir, "snapshots"), 3, io.Discard)
	if err != nil {
		return nil, err
	}
	tr, err = raft.NewTCPTransport(opts.BindAddr, nil, 3, 5*time.Second, io.Discard)
	if err != nil {
		return nil, err
	}
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(opts.NodeID)
	cfg.SnapshotThreshold = 64
	cfg.SnapshotInterval = 30 * time.Second
	cfg.LogOutput = io.Discard
	hadState, err := raft.HasExistingState(bolt, bolt, snapshots)
	if err != nil {
		return nil, err
	}
	if opts.Bootstrap && hadState {
		return nil, errors.New("refusing to bootstrap: node already contains raft state; restart without Bootstrap=true")
	}
	fsm := NewFSM(store)
	r, err := raft.NewRaft(cfg, fsm, bolt, bolt, snapshots, tr)
	if err != nil {
		return nil, err
	}
	if opts.Bootstrap {
		config := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(opts.NodeID), Address: tr.LocalAddr(), Suffrage: raft.Voter}}}
		if e := r.BootstrapCluster(config).Error(); e != nil {
			_ = r.Shutdown().Error()
			return nil, fmt.Errorf("bootstrap raft: %w", e)
		}
	}
	return &Service{raft: r, transport: tr, bolt: bolt, address: tr.LocalAddr(), nodeID: opts.NodeID}, nil
}

func (s *Service) Address() string           { return string(s.address) }
func (s *Service) NodeID() string            { return s.nodeID }
func (s *Service) RaftState() raft.RaftState { return s.raft.State() }
func (s *Service) LeaderAddress() string     { return string(s.raft.Leader()) }
func (s *Service) CurrentTerm() uint64       { return s.raft.CurrentTerm() }
func (s *Service) Configuration() (raft.Configuration, error) {
	f := s.raft.GetConfiguration()
	if err := f.Error(); err != nil {
		return raft.Configuration{}, err
	}
	return f.Configuration(), nil
}

// Apply is an internal, AUTHORIZATION-REQUIRING entry point. Its caller must
// validate membership and permissions (KAN-8/9) before use in production.
// A successful response means consensus committed AND the FSM applied it.
func (s *Service) Apply(ctx context.Context, cmd registry.Command) (registry.ApplyResult, error) {
	if s.raft.State() != raft.Leader {
		return registry.ApplyResult{}, ErrNotLeader
	}
	payload, err := json.Marshal(cmd)
	if err != nil {
		return registry.ApplyResult{}, err
	}
	timeout := 8 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return registry.ApplyResult{}, ctx.Err()
		}
	}
	f := s.raft.Apply(payload, timeout)
	// Raft.Apply timeout only covers enqueueing, NOT waiting for quorum.
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	select {
	case err = <-done:
		if err != nil {
			return registry.ApplyResult{}, err
		}
	case <-ctx.Done():
		return registry.ApplyResult{}, ctx.Err()
	}
	response, ok := f.Response().(applyResponse)
	if !ok {
		return registry.ApplyResult{}, errors.New("invalid raft FSM response")
	}
	return response.Result, response.Err
}

// AddVoter is for an explicitly authorized, prepared peer ONLY. In KAN-7 it
// is used by trusted local integration tests, not exposed over network APIs.
// Membership lifecycle, approvals and ID-key proof belong to KAN-8/9.
func (s *Service) AddVoter(ctx context.Context, id, address string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	if !strings.HasPrefix(id, "hc-") || len(id) != 35 {
		return errors.New("invalid node id")
	}
	if s.secureTrust != nil {
		peer, err := s.secureTrust.PeerForAddress(address)
		if err != nil {
			return fmt.Errorf("peer is not approved for Raft membership: %w", err)
		}
		if peer.NodeID != id {
			return errors.New("requested voter ID is not pinned to this Raft address")
		}
	} else {
		// KAN-7 developer transport stays loopback-only.
		if err := loopbackAddress(address, false); err != nil {
			return err
		}
	}
	timeout := 8 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return ctx.Err()
		}
	}
	future := s.raft.AddVoter(raft.ServerID(id), raft.ServerAddress(address), 0, timeout)
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Snapshot forces a Raft-managed snapshot. Raft controls log compaction.
func (s *Service) Snapshot() error { return s.raft.Snapshot().Error() }
func (s *Service) Close() error {
	if s == nil {
		return nil
	}
	var err error
	if s.raft != nil {
		err = s.raft.Shutdown().Error()
	}
	if s.transport != nil {
		if e := s.transport.Close(); err == nil {
			err = e
		}
	}
	if s.secureLayer != nil {
		if e := s.secureLayer.Close(); err == nil {
			err = e
		}
	}
	if s.bolt != nil {
		if e := s.bolt.Close(); err == nil {
			err = e
		}
	}
	return err
}
