package storage

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestScanPathRecursiveAndManagedExclusion(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "backups", "daily"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, ".homecloud", "objects"), 0700); err != nil {
		t.Fatal(err)
	}
	for name, contents := range map[string]string{"one.pdf": "1234", "backups/daily/other.pdf": "123456", ".homecloud/objects/managed.bin": "abcd"} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	result, err := scanPath(context.Background(), root, "vol1", true, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Complete || result.FilesCount != 2 || result.DirectoriesCount != 2 || result.TotalBytes != 10 {
		t.Fatalf("wrong scan: %+v", result)
	}
	limited, err := scanPath(context.Background(), root, "vol1", false, 1)
	if err != nil {
		t.Fatal(err)
	}
	if limited.FilesCount != 1 || limited.DirectoriesCount != 1 || !limited.EntriesTruncated {
		t.Fatalf("wrong shallow scan: %+v", limited)
	}
}
func TestScanPathContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result, _ := scanPath(ctx, t.TempDir(), "vol1", true, 10)
	if result.Complete {
		t.Fatalf("cancelled scan reported complete: %+v", result)
	}
}
