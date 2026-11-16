package preparedrpc

import (
	"context"
	"errors"
	"net"
	"strconv"
	"time"

	"HomeCloudBackend/internal/cluster/identity"
	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/security"
)

// Node is the opt-in Go integration for a separate authenticated membership
// control socket. NOT connected to unauthenticated /ws or the default serve.
type Node struct {
	Layer  *security.Listener
	Client *Client
	server *Server
	cancel context.CancelFunc
}

func ControlBindAddress(raftAddress string) (string, error) {
	host, portText, err := net.SplitHostPort(raftAddress)
	if err != nil {
		return "", err
	}
	if net.ParseIP(host) == nil {
		return "", errors.New("control listen requires a concrete local IP")
	}
	n, err := strconv.Atoi(portText)
	if err != nil || n <= 0 || n >= 65535 {
		return "", errors.New("Raft port must be 1..65534")
	}
	return net.JoinHostPort(host, strconv.Itoa(n+1)), nil
}

// Open sets up the mTLS control listener and client using existing KAN-5
// identity (no new keys) and KAN-8 trust. Gate must enforce leader/owner
// authorization. To enroll a candidate, its *local owner* explicitly pins
// the elected leader and enables a short-lived CandidateGate.
func Open(ctx context.Context, raftAddress, clusterID string, ids *identity.Store, trust *security.TrustStore, gate Gate) (*Node, error) {
	if ids == nil || trust == nil || gate == nil || clusterID == "" {
		return nil, errors.New("incomplete membership control configuration")
	}
	bind, err := ControlBindAddress(raftAddress)
	if err != nil {
		return nil, err
	}
	id, err := ids.LoadOrCreate()
	if err != nil {
		return nil, err
	}
	cert, err := ids.RaftTLSCertificate()
	if err != nil {
		return nil, err
	}
	layer, err := security.NewMembershipListener(bind, cert, trust)
	if err != nil {
		return nil, err
	}
	layer.SetCertificateProvider(ids.RaftTLSCertificate)
	srv, err := NewServer(layer, trust, gate)
	if err != nil {
		_ = layer.Close()
		return nil, err
	}
	runCtx, cancel := context.WithCancel(ctx)
	node := &Node{Layer: layer, Client: &Client{Layer: layer, Trust: trust, Self: security.Peer{NodeID: id.NodeID, PublicKey: id.PublicKey, Addresses: []string{raftAddress}}, ClusterID: clusterID, Timeout: 5 * time.Second}, server: srv, cancel: cancel}
	go func() { _ = srv.Serve(runCtx) }()
	return node, nil
}
func (n *Node) Close() error {
	if n == nil {
		return nil
	}
	n.cancel()
	return n.Layer.Close()
}
func (n *Node) Readiness(localNodeID string, trust *security.TrustStore) membership.StrictReadiness {
	return membership.StrictReadiness{LocalNodeID: localNodeID, Trust: trust, RPC: n.Client}
}
