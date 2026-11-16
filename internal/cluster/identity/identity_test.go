package identity

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCreateAndPersistIdentity(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cluster")
	store := NewStore(dir)
	first, err := store.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewStore(dir).LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("identity changed on restart: %+v %+v", first, second)
	}
	if !strings.HasPrefix(first.NodeID, "hc-") || len(first.NodeID) != 35 {
		t.Fatalf("unexpected node ID: %q", first.NodeID)
	}
	if first.PreferredRole != RoleStandalone {
		t.Fatalf("default role: %q", first.PreferredRole)
	}
	metadata, err := os.ReadFile(filepath.Join(dir, identityFile))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metadata), "private_key") {
		t.Fatal("private key leaked into identity JSON")
	}
	seed, err := os.ReadFile(filepath.Join(dir, privateKeyFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(seed) != ed25519.SeedSize {
		t.Fatalf("key has %d bytes", len(seed))
	}
}

func TestDifferentServersHaveDifferentIDs(t *testing.T) {
	a, err := NewStore(t.TempDir()).LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewStore(t.TempDir()).LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if a.NodeID == b.NodeID {
		t.Fatal("different servers have the same node_id")
	}
}

func TestPersistPreferredRoleAndRejectUnknownRole(t *testing.T) {
	dir := t.TempDir()
	store := NewStore(dir)
	original, err := store.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.SetPreferredRole(Role("leader")); err == nil {
		t.Fatal("accepted invalid role")
	}
	for _, role := range []Role{RoleMaster, RoleNode, RoleStandalone} {
		changed, err := store.SetPreferredRole(role)
		if err != nil {
			t.Fatal(err)
		}
		reloaded, err := NewStore(dir).LoadOrCreate()
		if err != nil {
			t.Fatal(err)
		}
		if changed.PreferredRole != role || reloaded.PreferredRole != role {
			t.Fatalf("role did not persist: %q", role)
		}
		if reloaded.NodeID != original.NodeID || reloaded.PublicKey != original.PublicKey {
			t.Fatal("identity rotated after role change")
		}
	}
}

func TestMissingAndCorruptedKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	_, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	keyPath := filepath.Join(dir, privateKeyFile)
	if err := os.Remove(keyPath); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadOrCreate(); err == nil {
		t.Fatal("silently rotated lost identity key")
	}
}

func TestMissingMetadataFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	_, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, identityFile)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadOrCreate(); err == nil {
		t.Fatal("silently regenerated missing identity metadata")
	}
}

func TestTamperedMetadataFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	_, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(dir, identityFile)
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var ident Identity
	if err := json.Unmarshal(b, &ident); err != nil {
		t.Fatal(err)
	}
	ident.NodeID = "hc-attacker"
	b, _ = json.Marshal(ident)
	if err := os.WriteFile(p, b, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadOrCreate(); err == nil {
		t.Fatal("accepted metadata not matching key")
	}
}

func TestSignChallengeUsesPersistentKey(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	ident, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	challenge := []byte("example challenge for test only")
	sig, err := NewStore(dir).SignChallenge(challenge)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := base64.StdEncoding.DecodeString(ident.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(pub, append([]byte(challengeDomain), challenge...), sig) {
		t.Fatal("identity signature failed verification")
	}
	if ed25519.Verify(pub, append([]byte(challengeDomain), []byte("different challenge")...), sig) {
		t.Fatal("signature verified wrong message")
	}
}

func TestRefuseSymlinkIdentity(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	_, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	key := filepath.Join(dir, privateKeyFile)
	real := filepath.Join(dir, "real.key")
	if err := os.Rename(key, real); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(real, key); err != nil {
		t.Skipf("symlinks unsupported by test environment: %v", err)
	}
	if _, err := s.LoadOrCreate(); err == nil {
		t.Fatal("accepted symlink key")
	}
}

func TestRoleCannotSetBeforeIdentityExists(t *testing.T) {
	_, err := NewStore(t.TempDir()).SetPreferredRole(RoleMaster)
	if err == nil {
		t.Fatal("role update silently created an identity")
	}
}

func TestCorruptedPrivateKeyFailsClosed(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	_, err := s.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, privateKeyFile), []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LoadOrCreate(); err == nil {
		t.Fatal("accepted corrupt private key")
	}
}
