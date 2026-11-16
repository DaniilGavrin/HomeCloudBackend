package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"
)

// Scan is deliberately read-only. It does not add arbitrary filesystem files
// to hc_storage_files: that table indexes managed objects stored by file ID.
const (
	scanMaxEntries  = 100000
	scanMaxDuration = 15 * time.Second
	scanMaxErrors   = 10
)

type ScanEntry struct {
	Path      string `json:"path"`
	Type      string `json:"type"`
	SizeBytes int64  `json:"size_bytes,omitempty"`
}

type ScanError struct {
	Path  string `json:"path"`
	Error string `json:"error"`
}

type ScanResult struct {
	StorageID        string      `json:"storage_id"`
	ScannedAt        string      `json:"scanned_at"`
	Recursive        bool        `json:"recursive"`
	Complete         bool        `json:"complete"`
	FilesCount       int64       `json:"files_count"`
	DirectoriesCount int64       `json:"directories_count"`
	SymlinksCount    int64       `json:"symlinks_count"`
	TotalBytes       int64       `json:"total_bytes"`
	Entries          []ScanEntry `json:"entries"`
	EntriesTruncated bool        `json:"entries_truncated"`
	ErrorCount       int64       `json:"error_count"`
	Errors           []ScanError `json:"errors"`
}

// Scan enumerates an already-registered storage volume without persisting
// results. The operating system must be able to enumerate the mount; in
// particular, Windows' NFS client can reject Linux filenames containing ':'.
func (m *Manager) Scan(ctx context.Context, storageID string, recursive bool, limit int) (ScanResult, error) {
	if !validID.MatchString(storageID) {
		return ScanResult{}, ErrInvalid
	}
	if limit <= 0 {
		limit = 30
	}
	if limit > 100 {
		return ScanResult{}, ErrInvalid
	}
	v, err := m.volume(ctx, storageID)
	if err != nil {
		return ScanResult{}, err
	}
	if !v.Enabled {
		return ScanResult{}, ErrUnavailable
	}
	root, err := m.safeRoot(v.Path)
	if err != nil {
		return ScanResult{}, err
	}
	return scanPath(ctx, root, storageID, recursive, limit)
}

func scanPath(ctx context.Context, root, storageID string, recursive bool, limit int) (ScanResult, error) {
	ctx, cancel := context.WithTimeout(ctx, scanMaxDuration)
	defer cancel()
	result := ScanResult{StorageID: storageID, ScannedAt: time.Now().UTC().Format(time.RFC3339Nano), Recursive: recursive, Complete: true, Entries: make([]ScanEntry, 0), Errors: make([]ScanError, 0)}
	var walked int
	errLimit := errors.New("scan entry limit reached")
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, readErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			if readErr != nil {
				return readErr
			} // cannot even open the volume
			return nil
		}
		if readErr != nil {
			result.Complete = false
			result.ErrorCount++
			if len(result.Errors) < scanMaxErrors {
				result.Errors = append(result.Errors, ScanError{Path: filepath.ToSlash(rel), Error: readErr.Error()})
			}
			return nil // keep traversing other readable directories
		}
		// .homecloud contains private managed objects and temporary uploads.
		// Counting them would duplicate files already in the SQLite index.
		if rel == ".homecloud" && entry.IsDir() {
			return filepath.SkipDir
		}
		walked++
		if walked > scanMaxEntries {
			return errLimit
		}
		if entry.IsDir() {
			result.DirectoriesCount++
			if len(result.Entries) < limit {
				result.Entries = append(result.Entries, ScanEntry{Path: filepath.ToSlash(rel), Type: "directory"})
			} else {
				result.EntriesTruncated = true
			}
			if !recursive {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.Type()&fs.ModeSymlink != 0 {
			result.SymlinksCount++ // never follow symlinks and junctions
			if len(result.Entries) < limit {
				result.Entries = append(result.Entries, ScanEntry{Path: filepath.ToSlash(rel), Type: "symlink"})
			} else {
				result.EntriesTruncated = true
			}
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			result.Complete = false
			result.ErrorCount++
			if len(result.Errors) < scanMaxErrors {
				result.Errors = append(result.Errors, ScanError{Path: filepath.ToSlash(rel), Error: err.Error()})
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		result.FilesCount++
		result.TotalBytes += info.Size()
		if len(result.Entries) < limit {
			result.Entries = append(result.Entries, ScanEntry{Path: filepath.ToSlash(rel), Type: "file", SizeBytes: info.Size()})
		} else {
			result.EntriesTruncated = true
		}
		return nil
	})
	if err != nil {
		// A slow / inaccessible NFS volume must never report a complete count.
		if errors.Is(err, errLimit) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			result.Complete = false
			result.ErrorCount++
			if len(result.Errors) < scanMaxErrors {
				result.Errors = append(result.Errors, ScanError{Path: ".", Error: err.Error()})
			}
		} else {
			return result, fmt.Errorf("storage scan: %w", err)
		}
	}
	return result, nil
}
