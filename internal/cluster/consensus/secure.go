package consensus

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/registry"
	"HomeCloudBackend/internal/cluster/security"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
)

// SecureOptions are supplied from a trusted LOCAL service, never WS v1.
// The trust store must be populated only after owner-approved proof-of-key
// pairing. Identity must match NodeID; one physical device has one key.
type SecureOptions struct {
	NodeID    string
	BindAddr  string // a real interface IP:port, loopback/LAN/Tailscale, never wildcard
	DataDir   string // separate from KAN-7 plaintext developer Raft state
	Bootstrap bool   // only on a NEW, single initial node
	Identity  *identity.Store
	Trust     *security.TrustStore
}

type raftTLSLayer struct{ *security.Listener }

func (r *raftTLSLayer) Dial(target raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return r.Listener.Dial(string(target), timeout)
}

// Keeping the adapter below isolated allows security to stay stdlib-only.

// OpenSecure starts Raft over pinned-key mutual TLS. It never downgrades to
// plaintext. A missing trust store or local identity aborts startup. Until
// KAN-9/11 implement secure onboarding, this is a Go-internal entry point.
func OpenSecure(opts SecureOptions, store *registry.Store) (_ *Service, err error) {
	if opts.Identity == nil || opts.Trust == nil || store == nil {
		return nil, errors.New("mTLS Raft requires persistent identity, trust and registry")
	}
	id, err := opts.Identity.LoadOrCreate()
	if err != nil {
		return nil, err
	}
	if id.NodeID != opts.NodeID {
		return nil, errors.New("Raft node ID differs from authenticated persistent identity")
	}
	if !filepath.IsAbs(opts.DataDir) {
		return nil, errors.New("Raft DataDir must be an absolute local path")
	}
	if err = store.VerifySchema(context.Background()); err != nil {
		return nil, err
	}
	cert, err := opts.Identity.RaftTLSCertificate()
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(opts.DataDir, 0700); err != nil {
		return nil, err
	}
	layer, err := security.NewRaftListener(opts.BindAddr, cert, opts.Trust)
	if err != nil {
		return nil, fmt.Errorf("secure raft listen: %w", err)
	}
	layer.SetCertificateProvider(opts.Identity.RaftTLSCertificate)
	var bolt *raftboltdb.BoltStore
	var tr *raft.NetworkTransport
	defer func() {
		if err != nil {
			if tr != nil {
				_ = tr.Close()
			}
			_ = layer.Close()
			if bolt != nil {
				_ = bolt.Close()
			}
		}
	}()
	bolt, err = raftboltdb.NewBoltStore(filepath.Join(opts.DataDir, "raft.db"))
	if err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStore(filepath.Join(opts.DataDir, "snapshots"), 3, io.Discard)
	if err != nil {
		return nil, err
	}
	tr = raft.NewNetworkTransport(&raftTLSLayer{Listener: layer}, 3, 5*time.Second, io.Discard)
	cfg := raft.DefaultConfig()
	cfg.LocalID = raft.ServerID(opts.NodeID)
	cfg.LogOutput = io.Discard
	cfg.SnapshotThreshold = 64
	cfg.SnapshotInterval = 30 * time.Second
	prior, err := raft.HasExistingState(bolt, bolt, snaps)
	if err != nil {
		return nil, err
	}
	if opts.Bootstrap && prior {
		return nil, errors.New("refusing to re-bootstrap existing secure Raft state")
	}
	r, err := raft.NewRaft(cfg, NewFSM(store), bolt, bolt, snaps, tr)
	if err != nil {
		return nil, err
	}
	if opts.Bootstrap {
		peers := raft.Configuration{Servers: []raft.Server{{ID: raft.ServerID(opts.NodeID), Address: tr.LocalAddr(), Suffrage: raft.Voter}}}
		if e := r.BootstrapCluster(peers).Error(); e != nil {
			_ = r.Shutdown().Error()
			return nil, e
		}
	}
	return &Service{raft: r, transport: tr, bolt: bolt, address: tr.LocalAddr(), nodeID: opts.NodeID, secureTrust: opts.Trust, secureLayer: layer}, nil
}
