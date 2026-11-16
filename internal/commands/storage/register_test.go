package storage

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"HomeCloudBackend/internal/commands"
	store "HomeCloudBackend/internal/storage"
)

func TestStorageCommandRegistry(t *testing.T) {
	r := commands.NewRegistry()
	var gotOwner string
	Register(r, Dependencies{
		Scan: func(ctx context.Context, id string, recursive bool, limit int) (store.ScanResult, error) {
			if id != "vol1" || !recursive {
				t.Fatal("wrong scan parameters")
			}
			return store.ScanResult{StorageID: id, FilesCount: 8, Complete: true}, nil
		},
		List: func(context.Context) ([]store.Volume, error) {
			return []store.Volume{{ID: "vol1", Name: "local", Enabled: true}}, nil
		},
		PrepareUpload: func(ctx context.Context, owner, name, mime, id string, size int64) (store.TransferInfo, error) {
			gotOwner = owner
			if name != "test.pdf" || size != 3 || id != "auto" {
				t.Fatal("wrong arguments")
			}
			return store.TransferInfo{TransferID: "transfer", Role: "upload"}, nil
		},
	})
	client := commands.ClientContext{DeviceID: "pc"}
	call := func(id, action, data string) commands.Outbound {
		t.Helper()
		return r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: id, Type: "command", Action: action, Data: json.RawMessage(data)}, client)
	}
	if res := call("l", "storage.list", `{}`); res.Type != "result" {
		t.Fatalf("list %+v", res)
	}
	if res := call("s", "storage.scan", `{"storage_id":"vol1"}`); res.Type != "result" {
		t.Fatalf("scan %+v", res)
	}
	if res := call("u", "storage.upload.prepare", `{"name":"test.pdf","size_bytes":3,"storage_id":"auto"}`); res.Type != "result" || gotOwner != "pc" {
		t.Fatalf("upload %+v owner %q", res, gotOwner)
	}
	if res := call("bad", "storage.upload.prepare", `{"name":"test.pdf","size_bytes":3,"storage_id":"auto","extra":true}`); res.Type != "error" {
		t.Fatalf("unexpected extra field %+v", res)
	}
	if res := call("bad2", "storage.upload.prepare", `{"name":"test.pdf"} {"bad":1}`); res.Type != "error" {
		t.Fatalf("trailing JSON %+v", res)
	}
	if res := call("off", "storage.download.prepare", `{"file_id":"123"}`); res.Type != "error" || res.Error != store.ErrUnavailable.Error() {
		t.Fatalf("unavailable %+v", res)
	}
}
func TestRegisterErrorPassThrough(t *testing.T) {
	r := commands.NewRegistry()
	Register(r, Dependencies{Register: func(context.Context, string, string, int) (store.Volume, error) {
		return store.Volume{}, errors.New("volume offline")
	}})
	out := r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "x", Type: "command", Action: "storage.register", Data: json.RawMessage(`{"name":"USB","path":"/mnt/usb"}`)}, commands.ClientContext{DeviceID: "pc"})
	if out.Type != "error" || out.Error != "volume offline" {
		t.Fatalf("expected error: %+v", out)
	}
}

func TestIndexCommands(t *testing.T) {
	r := commands.NewRegistry()
	var started, synced bool
	Register(r, Dependencies{
		StartIndex: func(ctx context.Context, req store.IndexRequest, reindex bool) (store.IndexJob, error) {
			if req.StorageID != "vol1" || !req.Recursive || len(req.Exclude) != 1 || req.Exclude[0] != "backups/jupyter/**" {
				t.Fatalf("invalid index request: %+v", req)
			}
			started = true
			synced = reindex
			return store.IndexJob{JobID: "job1", Status: "running"}, nil
		},
		IndexStatus: func(id string) (store.IndexJob, error) {
			if id != "job1" {
				t.Fatal(id)
			}
			return store.IndexJob{JobID: id, Status: "completed"}, nil
		},
		CancelIndex: func(id string) error {
			if id != "job1" {
				t.Fatal(id)
			}
			return nil
		},
		GetFile: func(ctx context.Context, id string) (store.File, error) {
			return store.File{ID: id, Source: "indexed", Available: true}, nil
		},
	})
	call := func(action, data string) commands.Outbound {
		return r.Dispatch(context.Background(), commands.Inbound{V: 1, ID: "id", Type: "command", Action: action, Data: json.RawMessage(data)}, commands.ClientContext{DeviceID: "pc"})
	}
	request := `{"storage_id":"vol1","exclude":["backups/jupyter/**"]}`
	if a := call("storage.index", request); a.Type != "result" || !started || synced {
		t.Fatalf("index: %+v", a)
	}
	if a := call("storage.reindex", request); a.Type != "result" || !synced {
		t.Fatalf("reindex: %+v", a)
	}
	if a := call("storage.index.status", `{"job_id":"job1"}`); a.Type != "result" {
		t.Fatalf("status: %+v", a)
	}
	if a := call("storage.index.cancel", `{"job_id":"job1"}`); a.Type != "result" {
		t.Fatalf("cancel: %+v", a)
	}
	if a := call("storage.files.get", `{"file_id":"abc"}`); a.Type != "result" {
		t.Fatalf("get: %+v", a)
	}
	if a := call("storage.index", `{"storage_id":"vol1","extra":true}`); a.Type != "error" {
		t.Fatalf("unexpected field accepted: %+v", a)
	}
}
