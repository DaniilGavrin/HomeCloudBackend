package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"HomeCloudBackend/wsconfig"
	_ "modernc.org/sqlite"
)

func newIndexTestManager(t *testing.T) (*Manager, string) {
	t.Helper()
	root := t.TempDir()
	db, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { db.Close() })
	for _, q := range []string{
		`CREATE TABLE hc_storage_volumes(id TEXT PRIMARY KEY,name TEXT,path TEXT,priority INTEGER,enabled INTEGER)`,
		`CREATE TABLE hc_storage_files(id TEXT PRIMARY KEY,storage_id TEXT,name TEXT,mime_type TEXT,size_bytes INTEGER,sha256 TEXT,created_at TEXT,uploaded_by TEXT)`,
		`CREATE TABLE hc_storage_index(id TEXT PRIMARY KEY,storage_id TEXT,relative_path TEXT,name TEXT,mime_type TEXT,size_bytes INTEGER,modified_at TEXT,indexed_at TEXT,seen_run TEXT,available INTEGER, UNIQUE(storage_id,relative_path))`,
	} {
		if _, err := db.Exec(q); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`INSERT INTO hc_storage_volumes(id,name,path,priority,enabled) VALUES('vol1','Main',?,10,1)`, root); err != nil {
		t.Fatal(err)
	}
	m, err := New(wsconfig.StorageConfig{Enabled: true, AllowedRoots: []string{root}, MaxUploadSizeMB: 10}, db, root)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.CloseAll)
	return m, root
}

func waitIndex(t *testing.T, m *Manager, id string) IndexJob {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := m.IndexStatus(id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status != "running" {
			return j
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatal("index job timeout")
	return IndexJob{}
}

func TestIndexAndReindexPersistentAndNoFileModification(t *testing.T) {
	m, root := newIndexTestManager(t)
	os.MkdirAll(filepath.Join(root, "backups", "jupyter"), 0700)
	os.MkdirAll(filepath.Join(root, "Certificate"), 0700)
	os.MkdirAll(filepath.Join(root, ".homecloud", "objects"), 0700)
	for name, text := range map[string]string{
		"ready.pdf": "hello", "Certificate/cert.pdf": "certificate", "backups/jupyter/secret.txt": "protected", ".homecloud/objects/private.bin": "managed",
	} {
		if err := os.WriteFile(filepath.Join(root, filepath.FromSlash(name)), []byte(text), 0600); err != nil {
			t.Fatal(err)
		}
	}
	job, err := m.BeginIndex(context.Background(), IndexRequest{StorageID: "vol1", Recursive: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	j := waitIndex(t, m, job.JobID)
	if j.Status != "completed" || j.FilesIndexed != 2 || !j.Complete {
		t.Fatalf("job %+v", j)
	}
	files, err := m.ListFiles(context.Background(), "vol1", 100)
	if err != nil || len(files) != 2 {
		t.Fatalf("files %v, err %v", files, err)
	}
	first := make(map[string]string)
	for _, f := range files {
		first[f.RelativePath] = f.ID
	}
	if first["ready.pdf"] == "" || first["Certificate/cert.pdf"] == "" {
		t.Fatalf("missing files %+v", files)
	}
	if stringMustRead(t, filepath.Join(root, "ready.pdf")) != "hello" {
		t.Fatal("index changed file")
	}
	transfer, err := m.PrepareDownload(context.Background(), "client", first["ready.pdf"])
	if err != nil {
		t.Fatalf("prepare indexed download: %v", err)
	}
	claimed, err := m.Claim(transfer.TransferID, "client", "download")
	if err != nil {
		t.Fatalf("claim indexed download: %v", err)
	}
	if got := stringMustRead(t, claimed.SourcePath); got != "hello" {
		t.Fatalf("wrong download source: %q", got)
	}
	m.Finish(claimed)
	if err := os.Remove(filepath.Join(root, "ready.pdf")); err != nil {
		t.Fatal(err)
	}
	job, err = m.BeginIndex(context.Background(), IndexRequest{StorageID: "vol1", Recursive: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	j = waitIndex(t, m, job.JobID)
	if j.Status != "completed" || j.MissingMarked != 1 {
		t.Fatalf("reindex %+v", j)
	}
	files, err = m.ListFiles(context.Background(), "vol1", 100)
	if err != nil || len(files) != 2 {
		t.Fatalf("files %v %v", files, err)
	}
	for _, f := range files {
		if f.ID != first[f.RelativePath] {
			t.Fatalf("ID changed for %s", f.RelativePath)
		}
		if f.RelativePath == "ready.pdf" && f.Available {
			t.Fatal("missing file still available")
		}
	}
	if err := os.WriteFile(filepath.Join(root, "ready.pdf"), []byte("hello again"), 0600); err != nil {
		t.Fatal(err)
	}
	job, err = m.BeginIndex(context.Background(), IndexRequest{StorageID: "vol1", Recursive: true}, true)
	if err != nil {
		t.Fatal(err)
	}
	j = waitIndex(t, m, job.JobID)
	if j.Status != "completed" {
		t.Fatalf("restore %+v", j)
	}
	f, err := m.GetFile(context.Background(), first["ready.pdf"])
	if err != nil || !f.Available || f.SizeBytes != int64(len("hello again")) {
		t.Fatalf("restored %+v %v", f, err)
	}
}

func stringMustRead(t *testing.T, p string) string {
	t.Helper()
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestPartialIndexDoesNotMarkExistingMissing(t *testing.T) {
	m, root := newIndexTestManager(t)
	os.WriteFile(filepath.Join(root, "keep.pdf"), []byte("1"), 0600)
	start, err := m.BeginIndex(context.Background(), IndexRequest{StorageID: "vol1", Recursive: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	if j := waitIndex(t, m, start.JobID); j.Status != "completed" {
		t.Fatalf("init %+v", j)
	}
	files, _ := m.ListFiles(context.Background(), "vol1", 100)
	id := files[0].ID
	os.Remove(filepath.Join(root, "keep.pdf"))
	// Excluded old entry must not be marked missing on a complete reindex.
	start, err = m.BeginIndex(context.Background(), IndexRequest{StorageID: "vol1", Recursive: true, Exclude: []string{"keep.pdf"}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if j := waitIndex(t, m, start.JobID); j.Status != "completed" || j.MissingMarked != 0 {
		t.Fatalf("exclude %+v", j)
	}
	f, err := m.GetFile(context.Background(), id)
	if err != nil || !f.Available {
		t.Fatalf("excluded entry should be unchanged %+v %v", f, err)
	}
}
