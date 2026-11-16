// Package identity manages a permanent, local HomeCloud node identity.
// It does not implement cluster membership, client authentication, or Raft.
package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	challengeDomain = "HomeCloud/cluster/node-proof/v1\x00"
	currentVersion  = 1
	identityFile    = "identity.json"
	privateKeyFile  = "identity.key"
)

// Role is an administrator's preferred/bootstrap role, not the current Raft role.
// Actual Master election is outside this package and will be implemented later.
type Role string

const (
	RoleStandalone Role = "standalone"
	RoleMaster     Role = "master"
	RoleNode       Role = "node"
)

func (r Role) Valid() bool {
	switch r {
	case RoleStandalone, RoleMaster, RoleNode:
		return true
	default:
		return false
	}
}

// Identity contains only PUBLIC data. Never put the private key in this struct.
// node_id is derived from the public key. It does not change when an IP changes.
type Identity struct {
	Version       int       `json:"version"`
	NodeID        string    `json:"node_id"`
	PublicKey     string    `json:"public_key"` // base64, standard encoding
	PreferredRole Role      `json:"preferred_role"`
	CreatedAt     time.Time `json:"created_at"`
}

type Store struct {
	dir string
	mu  sync.Mutex
}

// NewStore requires a persistent, local directory. Never store it on a shared
// NFS mount, or reuse the same directory between two physical HomeCloud nodes.
func NewStore(dir string) *Store { return &Store{dir: dir} }

func (s *Store) LoadOrCreate() (Identity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := os.MkdirAll(s.dir, 0700); err != nil {
		return Identity{}, fmt.Errorf("create identity directory: %w", err)
	}
	keyPath := filepath.Join(s.dir, privateKeyFile)
	infoPath := filepath.Join(s.dir, identityFile)

	keyExists, err := regularFileExists(keyPath)
	if err != nil {
		return Identity{}, err
	}
	infoExists, err := regularFileExists(infoPath)
	if err != nil {
		return Identity{}, err
	}
	if keyExists != infoExists {
		return Identity{}, errors.New("incomplete node identity: identity.key and identity.json must both exist; restore from backup, do not generate a replacement")
	}
	if !keyExists {
		return s.create(keyPath, infoPath)
	}
	return load(keyPath, infoPath)
}

// SetPreferredRole is a local application API for future authenticated UI
// onboarding. DO NOT expose it directly through the current unauthenticated WS.
func (s *Store) SetPreferredRole(role Role) (Identity, error) {
	if !role.Valid() {
		return Identity{}, fmt.Errorf("invalid preferred role %q", role)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	keyPath := filepath.Join(s.dir, privateKeyFile)
	infoPath := filepath.Join(s.dir, identityFile)
	identity, err := load(keyPath, infoPath)
	if err != nil {
		return Identity{}, err
	}
	if identity.PreferredRole == role {
		return identity, nil
	}
	identity.PreferredRole = role
	if err := writeAtomic(infoPath, identity); err != nil {
		return Identity{}, err
	}
	return identity, nil
}

// SignChallenge is reserved for a later authenticated Node-to-Node handshake.
// Signing alone does NOT grant membership or administrative privileges.
func (s *Store) SignChallenge(challenge []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keyPath := filepath.Join(s.dir, privateKeyFile)
	infoPath := filepath.Join(s.dir, identityFile)
	if _, err := load(keyPath, infoPath); err != nil {
		return nil, err
	}
	seed, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, err
	}
	msg := append([]byte(challengeDomain), challenge...)
	return ed25519.Sign(ed25519.NewKeyFromSeed(seed), msg), nil
}

func (s *Store) create(keyPath, infoPath string) (Identity, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return Identity{}, fmt.Errorf("generate node key: %w", err)
	}
	identity := Identity{
		Version:       currentVersion,
		NodeID:        idFromPublicKey(pub),
		PublicKey:     base64.StdEncoding.EncodeToString(pub),
		PreferredRole: RoleStandalone,
		CreatedAt:     time.Now().UTC(),
	}
	// Create the key with no overwrite. If a crash occurs before metadata is
	// written, the next start fails closed instead of minting a new identity.
	if err := writeExclusive(keyPath, priv.Seed()); err != nil {
		return Identity{}, fmt.Errorf("persist private key: %w", err)
	}
	if err := writeExclusiveJSON(infoPath, identity); err != nil {
		// Do not silently delete a durable private key and rotate node_id.
		return Identity{}, fmt.Errorf("private key saved but identity metadata failed (manual recovery required): %w", err)
	}
	return identity, nil
}

func load(keyPath, infoPath string) (Identity, error) {
	if ok, err := regularFileExists(keyPath); err != nil || !ok {
		return Identity{}, fmt.Errorf("private identity key missing or unsafe: %w", missingOrErr(err))
	}
	if ok, err := regularFileExists(infoPath); err != nil || !ok {
		return Identity{}, fmt.Errorf("identity metadata missing or unsafe: %w", missingOrErr(err))
	}
	seed, err := os.ReadFile(keyPath)
	if err != nil {
		return Identity{}, fmt.Errorf("read node key: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return Identity{}, errors.New("invalid node key length; restore the original key, do not regenerate")
	}
	infoBytes, err := os.ReadFile(infoPath)
	if err != nil {
		return Identity{}, err
	}
	var identity Identity
	if err := json.Unmarshal(infoBytes, &identity); err != nil {
		return Identity{}, fmt.Errorf("invalid identity metadata: %w", err)
	}
	if identity.Version != currentVersion || !identity.PreferredRole.Valid() || identity.CreatedAt.IsZero() {
		return Identity{}, errors.New("unsupported or invalid identity metadata")
	}
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
	if identity.PublicKey != base64.StdEncoding.EncodeToString(pub) || identity.NodeID != idFromPublicKey(pub) {
		return Identity{}, errors.New("identity/key mismatch: restore the original files; refusing to rotate node_id")
	}
	return identity, nil
}

func missingOrErr(err error) error {
	if err != nil {
		return err
	}
	return os.ErrNotExist
}

func idFromPublicKey(pub ed25519.PublicKey) string {
	sum := sha256.Sum256(pub)
	return "hc-" + hex.EncodeToString(sum[:16])
}

func regularFileExists(path string) (bool, error) {
	st, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !st.Mode().IsRegular() {
		return false, fmt.Errorf("unsafe identity path %q: not a regular file", path)
	}
	return true, nil
}

func writeExclusive(path string, data []byte) (err error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	if _, err = f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func writeExclusiveJSON(path string, v Identity) error {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeExclusive(path, append(b, '\n'))
}

func writeAtomic(path string, v Identity) (err error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".identity-*.tmp")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	if err = f.Chmod(0600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}
