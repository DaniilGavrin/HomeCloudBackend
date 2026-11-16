package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIndexExclusions(t *testing.T) {
	patterns := []string{".homecloud/**", "backups/jupyter/**", "*.tmp", "Certificate/private/**"}
	for _, c := range []struct {
		rel  string
		skip bool
	}{
		{".homecloud", true}, {".homecloud/objects/file.bin", true},
		{"backups/jupyter", true}, {"backups/jupyter/secret.sql", true},
		{"backups/2026/backup.pdf", false}, {"report.tmp", true},
		{"Certificate/private/x.pdf", true}, {"Certificate/normal.pdf", false},
		{"ready.pdf", false},
	} {
		if got := excluded(c.rel, patterns); got != c.skip {
			t.Errorf("%s: skip=%v, want %v", c.rel, got, c.skip)
		}
	}
	for _, invalid := range []string{"../x", "/private", "a//b", "a/../b", "", `a\\b`} {
		if validExclusion(invalid) {
			t.Fatalf("invalid exclusion %q accepted", invalid)
		}
	}
}

func TestSafeIndexedPath(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "okay.pdf"), []byte("valid"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "backups", "jupyter"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "backups", "jupyter", "secret.txt"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	m := &Manager{root: root, allowed: []string{root}}
	v := Volume{Path: root}
	if _, err := m.safeIndexedPath(v, "okay.pdf"); err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"../secret", ".homecloud/objects/abc.bin", "backups/jupyter/secret.txt", "nonexistent", "/absolute"} {
		if _, err := m.safeIndexedPath(v, rel); err == nil {
			t.Fatalf("unsafe path %q accepted", rel)
		}
	}
	external := t.TempDir()
	if err := os.WriteFile(filepath.Join(external, "secret"), []byte("secret"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(external, filepath.Join(root, "link")); err == nil {
		if _, err := m.safeIndexedPath(v, "link/secret"); err == nil {
			t.Fatal("symlink escape accepted")
		}
	}
}
