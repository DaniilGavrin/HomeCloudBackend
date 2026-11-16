// Package preparedrpc supplies KAN-9 reciprocal trust preparation and revoke
// over a separate, mutually authenticated, pinned-key TLS 1.3 control channel.
// Not an HTTP endpoint; never attach to the anonymous /ws handler.
package preparedrpc

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/security"
)

const (
	version             = 1
	maxWireBytes        = 32768
	actionPreparePeer   = "prepare_peer"
	actionPrepareVoters = "prepare_voters"
	actionRevoke        = "revoke_peer"
)

// Gate validates authorization based on the *authenticated TLS peer*, current
// Raft leader and persisted cluster state. The caller cannot assert its own ID.
// NEVER use an unconditional allow Gate with real cluster credentials.
type Gate interface {
	Authorize(context.Context, string, string, string, string) error
}

type request struct {
	Version   int             `json:"v"`
	ID        string          `json:"id"`
	ClusterID string          `json:"cluster_id"`
	Action    string          `json:"action"`
	Peer      security.Peer   `json:"peer,omitempty"`
	Peers     []security.Peer `json:"peers,omitempty"`
	NodeID    string          `json:"node_id,omitempty"`
}
type response struct {
	Version int    `json:"v"`
	ID      string `json:"id"`
	OK      bool   `json:"ok"`
	Error   string `json:"error,omitempty"`
}

// Server serves one bounded request per mTLS channel. Key revocation
// automatically terminates active connections via security.Listener.
type Server struct {
	Layer *security.Listener
	Trust *security.TrustStore
	Gate  Gate
	mu    sync.Mutex
	seen  map[string]time.Time
}

func NewServer(layer *security.Listener, store *security.TrustStore, gate Gate) (*Server, error) {
	if layer == nil || store == nil || gate == nil {
		return nil, errors.New("membership control requires TLS, pinned trust and authorization gate")
	}
	return &Server{Layer: layer, Trust: store, Gate: gate, seen: make(map[string]time.Time)}, nil
}
func (s *Server) Serve(ctx context.Context) error {
	// Closing the listener unblocks Accept. Callers own the listener lifecycle.
	go func() { <-ctx.Done(); _ = s.Layer.Close() }()
	slots := make(chan struct{}, 32)
	for {
		conn, err := s.Layer.Accept()
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		select {
		case slots <- struct{}{}:
			go func() { defer func() { <-slots }(); s.handle(ctx, conn) }()
		default:
			_ = conn.Close()
		}
	}
}
func (s *Server) handle(ctx context.Context, conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(8 * time.Second))
	caller, err := security.AuthenticatedPeerID(conn)
	if err != nil {
		return
	}
	var req request
	dec := json.NewDecoder(io.LimitReader(conn, maxWireBytes+1))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&req); err != nil {
		return
	}
	out := response{Version: version, ID: req.ID}
	if err = s.execute(ctx, caller, req); err != nil {
		out.Error = err.Error()
	} else {
		out.OK = true
	}
	_ = json.NewEncoder(conn).Encode(out)
}
func (s *Server) execute(ctx context.Context, caller string, req request) error {
	if req.Version != version || len(req.ID) != 32 || len(req.ClusterID) < 3 || len(req.ClusterID) > 128 {
		return errors.New("invalid membership message")
	}
	if _, err := hex.DecodeString(req.ID); err != nil {
		return errors.New("invalid request ID")
	}
	if req.Action != actionPreparePeer && req.Action != actionPrepareVoters && req.Action != actionRevoke {
		return errors.New("unknown membership action")
	}
	// Authenticate/authorize before writing anything. Never trust a payload's source ID.
	target := req.NodeID
	if req.Action == actionPreparePeer {
		target = req.Peer.NodeID
	}
	if req.Action == actionPrepareVoters {
		target = ""
	}
	if err := s.Gate.Authorize(ctx, caller, req.ClusterID, req.Action, target); err != nil {
		return fmt.Errorf("membership operation denied: %w", err)
	}
	s.mu.Lock()
	now := time.Now()
	for id, t := range s.seen {
		if now.Sub(t) > 10*time.Minute {
			delete(s.seen, id)
		}
	}
	if _, found := s.seen[caller+":"+req.ID]; found {
		s.mu.Unlock()
		return errors.New("membership request replay")
	}
	if len(s.seen) >= 1024 {
		s.mu.Unlock()
		return errors.New("membership replay cache full")
	}
	s.seen[caller+":"+req.ID] = now
	s.mu.Unlock()
	switch req.Action {
	case actionPreparePeer:
		if req.Peer.NodeID == "" || len(req.Peers) > 0 || req.NodeID != "" {
			return errors.New("invalid candidate record")
		}
		return s.Trust.Approve(req.Peer)
	case actionPrepareVoters:
		if len(req.Peers) == 0 || len(req.Peers) > 16 || req.NodeID != "" || req.Peer.NodeID != "" {
			return errors.New("invalid voter set")
		}
		// Must carry the authenticated leader's own record, already pinned locally.
		var leaderCount int
		known := make(map[string]bool, len(req.Peers))
		for _, p := range req.Peers {
			if known[p.NodeID] || p.NodeID == "" {
				return errors.New("duplicate/missing voter identity")
			}
			known[p.NodeID] = true
			if p.NodeID == caller {
				leaderCount++
				pinned, ok := s.Trust.Peer(caller)
				if !ok || pinned.PublicKey != p.PublicKey {
					return errors.New("leader key not pinned")
				}
			}
		}
		if leaderCount != 1 {
			return errors.New("authorized leader not included in voters")
		}
		// No partial writes on malformed identities. Cross-entry address collisions
		// are rejected by TrustStore.Approve; retry idempotently after interruption.
		for _, p := range req.Peers {
			if p.NodeID == caller {
				continue
			}
			if err := s.Trust.Approve(p); err != nil {
				return err
			}
		}
		return nil
	case actionRevoke:
		if req.NodeID == "" || len(req.Peers) > 0 || req.Peer.NodeID != "" {
			return errors.New("invalid revoked identity")
		}
		err := s.Trust.Revoke(req.NodeID)
		if errors.Is(err, security.ErrUntrusted) {
			return nil
		}
		return err
	}
	return errors.New("invalid action")
}

// Client implements membership.PreparedRPC; every target address must be
// present in the local pinned TRUST STORE. Membership TLS is on Raft port + 1.
type Client struct {
	Layer     *security.Listener
	Trust     *security.TrustStore
	Self      security.Peer
	ClusterID string
	Timeout   time.Duration
}

var _ membership.PreparedRPC = (*Client)(nil)

func (c *Client) send(ctx context.Context, raftAddress string, r request) error {
	if c.Layer == nil || c.Trust == nil || c.ClusterID == "" {
		return errors.New("unconfigured secure control RPC")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	r.Version = version
	r.ID = hex.EncodeToString(nonce[:])
	r.ClusterID = c.ClusterID
	timeout := c.Timeout
	if timeout <= 0 || timeout > 15*time.Second {
		timeout = 5 * time.Second
	}
	if d, ok := ctx.Deadline(); ok && time.Until(d) < timeout {
		timeout = time.Until(d)
	}
	if timeout <= 0 {
		return context.DeadlineExceeded
	}
	conn, err := c.Layer.DialMembership(raftAddress, timeout)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))
	if err = json.NewEncoder(conn).Encode(r); err != nil {
		return err
	}
	var reply response
	dec := json.NewDecoder(io.LimitReader(conn, maxWireBytes))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&reply); err != nil {
		return err
	}
	if reply.Version != version || reply.ID != r.ID {
		return errors.New("invalid membership response correlation")
	}
	if !reply.OK {
		if reply.Error == "" {
			return errors.New("membership RPC denied")
		}
		return errors.New(reply.Error)
	}
	return nil
}
func (c *Client) AssertTrustedOnVoter(ctx context.Context, v membership.Voter, p security.Peer) error {
	if !v.Voting {
		return errors.New("target is not a voter")
	}
	return c.send(ctx, v.RaftAddress, request{Action: actionPreparePeer, Peer: p})
}
func (c *Client) AssertVoterTrustOnCandidate(ctx context.Context, p security.Peer, voters []membership.Voter) error {
	if len(p.Addresses) == 0 {
		return errors.New("candidate has no pinned Raft address")
	}
	peers := make([]security.Peer, 0, len(voters))
	for _, v := range voters {
		if !v.Voting {
			continue
		}
		var peer security.Peer
		if v.NodeID == c.Self.NodeID {
			peer = c.Self
		} else {
			var ok bool
			peer, ok = c.Trust.Peer(v.NodeID)
			if !ok {
				return security.ErrUntrusted
			}
		}
		if len(peer.Addresses) == 0 {
			return errors.New("voter missing approved endpoints")
		}
		peers = append(peers, peer)
	}
	if len(peers) == 0 {
		return errors.New("no known voters")
	}
	return c.send(ctx, p.Addresses[0], request{Action: actionPrepareVoters, Peers: peers})
}
func (c *Client) RevokeOnVoter(ctx context.Context, v membership.Voter, nodeID string) error {
	if !v.Voting {
		return nil
	}
	return c.send(ctx, v.RaftAddress, request{Action: actionRevoke, NodeID: nodeID})
}
