package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"HomeCloudBackend/internal/commands"
	store "HomeCloudBackend/internal/storage"
)

type Dependencies struct {
	Register        func(context.Context, string, string, int) (store.Volume, error)
	List            func(context.Context) ([]store.Volume, error)
	ListFiles       func(context.Context, string, int) ([]store.File, error)
	Scan            func(context.Context, string, bool, int) (store.ScanResult, error)
	StartIndex      func(context.Context, store.IndexRequest, bool) (store.IndexJob, error)
	IndexStatus     func(string) (store.IndexJob, error)
	CancelIndex     func(string) error
	GetFile         func(context.Context, string) (store.File, error)
	PrepareUpload   func(context.Context, string, string, string, string, int64) (store.TransferInfo, error)
	PrepareDownload func(context.Context, string, string) (store.TransferInfo, error)
	Cancel          func(string, string) error
}

func strict(raw json.RawMessage, dst any) error {
	if len(raw) == 0 {
		return errors.New("data object required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid storage data: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON data")
	}
	return nil
}

func Register(r *commands.Registry, d Dependencies) {
	r.MustRegister("storage.register", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.Register == nil {
			return nil, store.ErrUnavailable
		}
		var v struct {
			Name     string `json:"name"`
			Path     string `json:"path"`
			Priority int    `json:"priority"`
		}
		if err := strict(msg.Data, &v); err != nil {
			return nil, err
		}
		return d.Register(ctx, v.Name, v.Path, v.Priority)
	})
	r.MustRegister("storage.list", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.List == nil {
			return nil, store.ErrUnavailable
		}
		v, err := d.List(ctx)
		return map[string]any{"storages": v}, err
	})
	r.MustRegister("storage.files.list", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.ListFiles == nil {
			return nil, store.ErrUnavailable
		}
		var v struct {
			StorageID string `json:"storage_id"`
			Limit     int    `json:"limit"`
		}
		if len(msg.Data) > 0 {
			if err := strict(msg.Data, &v); err != nil {
				return nil, err
			}
		}
		files, err := d.ListFiles(ctx, v.StorageID, v.Limit)
		return map[string]any{"files": files}, err
	})
	r.MustRegister("storage.scan", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.Scan == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			StorageID string `json:"storage_id"`
			Recursive *bool  `json:"recursive"`
			Limit     int    `json:"limit"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		if req.StorageID == "" {
			return nil, errors.New("storage_id required")
		}
		recursive := true
		if req.Recursive != nil {
			recursive = *req.Recursive
		}
		return d.Scan(ctx, req.StorageID, recursive, req.Limit)
	})
	r.MustRegister("storage.files.get", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.GetFile == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			FileID string `json:"file_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		return d.GetFile(ctx, req.FileID)
	})
	for _, action := range []string{"storage.index", "storage.reindex"} {
		reindex := action == "storage.reindex"
		r.MustRegister(action, func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
			if d.StartIndex == nil {
				return nil, store.ErrUnavailable
			}
			var req struct {
				StorageID string   `json:"storage_id"`
				Recursive *bool    `json:"recursive"`
				Exclude   []string `json:"exclude"`
			}
			if err := strict(msg.Data, &req); err != nil {
				return nil, err
			}
			recursive := true
			if req.Recursive != nil {
				recursive = *req.Recursive
			}
			return d.StartIndex(ctx, store.IndexRequest{StorageID: req.StorageID, Recursive: recursive, Exclude: req.Exclude}, reindex)
		})
	}
	r.MustRegister("storage.index.status", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.IndexStatus == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			JobID string `json:"job_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		return d.IndexStatus(req.JobID)
	})
	r.MustRegister("storage.index.cancel", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.CancelIndex == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			JobID string `json:"job_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		if err := d.CancelIndex(req.JobID); err != nil {
			return nil, err
		}
		return map[string]string{"job_id": req.JobID, "status": "cancelling"}, nil
	})
	r.MustRegister("storage.upload.prepare", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.PrepareUpload == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			Name      string `json:"name"`
			MIMEType  string `json:"mime_type"`
			SizeBytes int64  `json:"size_bytes"`
			StorageID string `json:"storage_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		return d.PrepareUpload(ctx, p.DeviceID, req.Name, req.MIMEType, req.StorageID, req.SizeBytes)
	})
	r.MustRegister("storage.download.prepare", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.PrepareDownload == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			FileID string `json:"file_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		return d.PrepareDownload(ctx, p.DeviceID, req.FileID)
	})
	r.MustRegister("storage.transfer.cancel", func(ctx context.Context, msg commands.Inbound, p commands.ClientContext) (any, error) {
		if d.Cancel == nil {
			return nil, store.ErrUnavailable
		}
		var req struct {
			TransferID string `json:"transfer_id"`
		}
		if err := strict(msg.Data, &req); err != nil {
			return nil, err
		}
		if req.TransferID == "" {
			return nil, errors.New("transfer_id required")
		}
		if err := d.Cancel(p.DeviceID, req.TransferID); err != nil {
			return nil, err
		}
		return map[string]string{"transfer_id": req.TransferID, "status": "cancelled"}, nil
	})
}
