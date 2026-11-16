package storage

import (
	"os"
	"path/filepath"
	"testing"

	"HomeCloudBackend/wsconfig"
)

func TestAllowedRootAndTraversal(t *testing.T) {
	root := t.TempDir()
	allowed := filepath.Join(root, "files")
	if err := os.MkdirAll(allowed, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "other"), 0700); err != nil {
		t.Fatal(err)
	}
	m, err := New(wsconfig.StorageConfig{Enabled: true, AllowedRoots: []string{"./files"}, MaxUploadSizeMB: 100}, nil, root)
	if err == nil || m != nil {
		t.Fatal("nil db must be rejected")
	}
	m = &Manager{root: root, allowed: []string{allowed}}
	for _, p := range []string{"./files", "./files/sub/.."} {
		if _, err := m.safeRoot(p); err != nil {
			t.Errorf("should accept %s: %v", p, err)
		}
	}
	for _, p := range []string{"./other", "./files/../other"} {
		if _, err := m.safeRoot(p); err == nil {
			t.Errorf("outside root accepted: %s", p)
		}
	}
}
func TestIDFormatAndContainment(t *testing.T) {
	id, err := newID()
	if err != nil || !validID.MatchString(id) {
		t.Fatalf("id %q %v", id, err)
	}
	parent := filepath.Join("a", "b")
	if !isWithin(parent, filepath.Join(parent, "c")) {
		t.Fatal("child rejected")
	}
	if isWithin(parent, filepath.Join("a", "bad")) {
		t.Fatal("sibling accepted")
	}
}
