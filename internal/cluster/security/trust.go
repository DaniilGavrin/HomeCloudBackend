// Package security stores pinned Ed25519 peers and durable revocations.
// First-use enrollment is allowed only by the separate private-network service,
// never by the legacy unauthenticated HomeCloud WS v1 API.
package security

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var (
	ErrUntrusted   = errors.New("homecloud cluster: peer is not approved")
	ErrKeyMismatch = errors.New("homecloud cluster: peer key does not match pinned node identity")
)

const trustVersion = 1

// Peer represents an APPROVED cryptographic identity and the Raft endpoints
// it is explicitly allowed to occupy. Addresses are not proof of identity.
type Peer struct {
	NodeID    string   `json:"node_id"`
	PublicKey string   `json:"public_key"` // standard base64 Ed25519
	Addresses []string `json:"addresses"`
}

type trustFile struct {
	Version     int      `json:"version"`
	LocalNodeID string   `json:"local_node_id"`
	Peers       []Peer   `json:"peers"`
	Revoked     []string `json:"revoked_node_ids,omitempty"`
}

type TrustStore struct {
	mu           sync.RWMutex
	path         string
	localID      string
	peers        map[string]Peer
	revoked      map[string]bool
	listeners    map[uint64]func(string)
	nextListener uint64
}

// NodeID derives the same stable ID as KAN-5 identity without exposing keys.
func NodeID(pub ed25519.PublicKey) string {
	h := sha256.Sum256(pub)
	return "hc-" + hex.EncodeToString(h[:16])
}
func validNodeID(id string) bool {
	if len(id) != 35 || !strings.HasPrefix(id, "hc-") {
		return false
	}
	_, err := hex.DecodeString(id[3:])
	return err == nil
}
func parsePublicKey(raw string) (ed25519.PublicKey, error) {
	b, err := base64.StdEncoding.Strict().DecodeString(raw)
	if err != nil || len(b) != ed25519.PublicKeySize {
		return nil, errors.New("invalid Ed25519 public key")
	}
	return ed25519.PublicKey(b), nil
}
func normalizedAddress(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" || port == "" {
		return "", fmt.Errorf("invalid peer endpoint %q", addr)
	}
	n, err := strconv.Atoi(port)
	if err != nil || n <= 0 || n > 65535 {
		return "", fmt.Errorf("invalid peer port %q", port)
	}
	if strings.ContainsAny(host, " /\\@%") || host == "0.0.0.0" || host == "::" {
		return "", fmt.Errorf("invalid peer host %q", host)
	}
	if ip := net.ParseIP(host); ip != nil {
		if ip.IsUnspecified() || ip.IsMulticast() {
			return "", errors.New("unspecified/multicast peer host")
		}
		host = ip.String()
	} else {
		if len(host) > 253 || strings.HasPrefix(host, "-") || strings.HasSuffix(host, "-") || strings.Contains(host, "..") || strings.Contains(host, ":") {
			return "", errors.New("invalid DNS name")
		}
		for _, c := range host {
			if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '.') {
				return "", errors.New("invalid DNS name")
			}
		}
		host = strings.ToLower(host)
	}
	return net.JoinHostPort(host, strconv.Itoa(n)), nil
}
func normalizePeer(p Peer) (Peer, error) {
	pub, err := parsePublicKey(p.PublicKey)
	if err != nil {
		return Peer{}, err
	}
	if !validNodeID(p.NodeID) || NodeID(pub) != p.NodeID {
		return Peer{}, ErrKeyMismatch
	}
	if len(p.Addresses) == 0 || len(p.Addresses) > 16 {
		return Peer{}, errors.New("peer must have 1..16 approved Raft addresses")
	}
	seen := map[string]bool{}
	out := make([]string, 0, len(p.Addresses))
	for _, a := range p.Addresses {
		normalized, err := normalizedAddress(a)
		if err != nil {
			return Peer{}, err
		}
		if !seen[normalized] {
			seen[normalized] = true
			out = append(out, normalized)
		}
	}
	p.Addresses = out
	return p, nil
}

// OpenTrustStore fails closed on corruption, path substitution, or ID mismatch.
// Empty store allows no network peers. Trust is local and NEVER Raft-replicated.
func OpenTrustStore(dir, localNodeID string) (*TrustStore, error) {
	if !validNodeID(localNodeID) {
		return nil, errors.New("invalid local node identity")
	}
	if !filepath.IsAbs(dir) {
		return nil, errors.New("trust directory must be absolute")
	}
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, "trusted-peers.json")
	s := &TrustStore{path: path, localID: localNodeID, peers: map[string]Peer{}, revoked: map[string]bool{}, listeners: map[uint64]func(string){}}
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, err
	}
	if !st.Mode().IsRegular() {
		return nil, errors.New("unsafe trust database path")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var state trustFile
	if err = json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("corrupt trust db: %w", err)
	}
	if state.Version != trustVersion || state.LocalNodeID != localNodeID {
		return nil, errors.New("trust db owner or version mismatch")
	}
	for _, id := range state.Revoked {
		if !validNodeID(id) || id == localNodeID || s.revoked[id] {
			return nil, errors.New("invalid or duplicate revoked peer identity")
		}
		s.revoked[id] = true
	}
	for _, p := range state.Peers {
		p, err = normalizePeer(p)
		if err != nil {
			return nil, fmt.Errorf("corrupt trust db peer: %w", err)
		}
		if s.revoked[p.NodeID] {
			return nil, errors.New("peer is both trusted and revoked")
		}
		if p.NodeID == localNodeID {
			return nil, errors.New("self trust entry invalid")
		}
		if _, ok := s.peers[p.NodeID]; ok {
			return nil, errors.New("duplicate trusted node")
		}
		for _, old := range s.peers {
			for _, a := range old.Addresses {
				for _, b := range p.Addresses {
					if a == b {
						return nil, errors.New("duplicate trusted Raft endpoint")
					}
				}
			}
		}
		s.peers[p.NodeID] = p
	}
	return s, nil
}

// Approve pins a verified peer. The privileged caller must enforce the
// approved onboarding policy (private-network TOFU or an explicit owner action).
// Never expose this method to unauthenticated WebSocket clients.
func (s *TrustStore) Approve(p Peer) error {
	p, err := normalizePeer(p)
	if err != nil {
		return err
	}
	if p.NodeID == s.localID {
		return errors.New("cannot trust self as peer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.revoked[p.NodeID] {
		return ErrUntrusted
	}
	if old, ok := s.peers[p.NodeID]; ok && old.PublicKey != p.PublicKey {
		return ErrKeyMismatch
	}
	for id, old := range s.peers {
		if id == p.NodeID {
			continue
		}
		for _, a := range old.Addresses {
			for _, b := range p.Addresses {
				if a == b {
					return fmt.Errorf("address %q already assigned to another peer", a)
				}
			}
		}
	}
	copyPeers := make(map[string]Peer, len(s.peers)+1)
	for id, old := range s.peers {
		copyPeers[id] = old
	}
	copyPeers[p.NodeID] = p
	if err := s.save(copyPeers, s.revoked); err != nil {
		return err
	}
	s.peers = copyPeers
	return nil
}

// Revoke durably tombstones a peer. Auto-enrollment MUST NOT resurrect a
// revoked identity. Previously revoked IDs remain denied on repeated calls.
func (s *TrustStore) Revoke(id string) error {
	if !validNodeID(id) || id == s.localID {
		return ErrUntrusted
	}
	s.mu.Lock()
	if s.revoked[id] {
		s.mu.Unlock()
		return ErrUntrusted
	}
	_, hadPeer := s.peers[id]
	peers := make(map[string]Peer, len(s.peers)-1)
	for k, p := range s.peers {
		if k != id {
			peers[k] = p
		}
	}
	revoked := make(map[string]bool, len(s.revoked)+1)
	for k := range s.revoked {
		revoked[k] = true
	}
	revoked[id] = true
	if err := s.save(peers, revoked); err != nil {
		s.mu.Unlock()
		return err
	}
	s.peers = peers
	s.revoked = revoked
	listeners := make([]func(string), 0, len(s.listeners))
	for _, f := range s.listeners {
		listeners = append(listeners, f)
	}
	s.mu.Unlock()
	for _, f := range listeners {
		f(id)
	}
	if !hadPeer { // An offline unknown removed voter must still stay tombstoned.
		return ErrUntrusted
	}
	return nil
}

// IsRevoked is checked before and during first-use enrollment. The final
// Approve operation checks the same map under lock to prevent a race.
func (s *TrustStore) IsRevoked(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.revoked[id]
}

// ReauthorizeRevoked is a LOCAL-only recovery primitive for a future
// authenticated owner workflow. It must never be called by network RPC.
func (s *TrustStore) ReauthorizeRevoked(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.revoked[id] {
		return ErrUntrusted
	}
	newRevoked := make(map[string]bool, len(s.revoked))
	for k := range s.revoked {
		if k != id {
			newRevoked[k] = true
		}
	}
	if err := s.save(s.peers, newRevoked); err != nil {
		return err
	}
	s.revoked = newRevoked
	return nil
}

func (s *TrustStore) save(peers map[string]Peer, revoked map[string]bool) error {
	list := make([]Peer, 0, len(peers))
	for _, p := range peers {
		list = append(list, p)
	}
	sort.Slice(list, func(i, j int) bool { return list[i].NodeID < list[j].NodeID })
	ids := make([]string, 0, len(revoked))
	for id := range revoked {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	buf, err := json.MarshalIndent(trustFile{Version: trustVersion, LocalNodeID: s.localID, Peers: list, Revoked: ids}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".trust-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if err = tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err = tmp.Write(append(buf, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err = tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), s.path)
}
func (s *TrustStore) Peer(id string) (Peer, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.peers[id]
	p.Addresses = append([]string(nil), p.Addresses...)
	return p, ok
}
func (s *TrustStore) PeerForAddress(addr string) (Peer, error) {
	a, err := normalizedAddress(addr)
	if err != nil {
		return Peer{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, p := range s.peers {
		for _, v := range p.Addresses {
			if v == a {
				return p, nil
			}
		}
	}
	return Peer{}, ErrUntrusted
}
func (s *TrustStore) Verify(id string, pub ed25519.PublicKey) error {
	if !validNodeID(id) || NodeID(pub) != id {
		return ErrKeyMismatch
	}
	p, ok := s.Peer(id)
	if !ok {
		return ErrUntrusted
	}
	expected, err := parsePublicKey(p.PublicKey)
	if err != nil {
		return err
	}
	if !ed25519.PublicKey(expected).Equal(pub) {
		return ErrKeyMismatch
	}
	return nil
}

// OnRevoke registers a callback. Returned function must be called on transport Close.
func (s *TrustStore) OnRevoke(callback func(string)) func() {
	s.mu.Lock()
	s.nextListener++
	id := s.nextListener
	s.listeners[id] = callback
	s.mu.Unlock()
	return func() { s.mu.Lock(); delete(s.listeners, id); s.mu.Unlock() }
}
