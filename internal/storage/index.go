package storage

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	indexMaxFiles         = 250000
	indexMaxDuration      = 15 * time.Minute
	indexMaxConcurrent    = 2
	indexProgressInterval = 2 * time.Second
)

type IndexJob struct {
	JobID         string      `json:"job_id"`
	StorageID     string      `json:"storage_id"`
	Mode          string      `json:"mode"`
	Status        string      `json:"status"`
	FilesSeen     int64       `json:"files_seen"`
	FilesIndexed  int64       `json:"files_indexed"`
	BytesSeen     int64       `json:"bytes_seen"`
	ErrorsCount   int64       `json:"errors_count"`
	Errors        []ScanError `json:"errors"`
	Complete      bool        `json:"complete"`
	MissingMarked int64       `json:"missing_marked"`
	StartedAt     string      `json:"started_at"`
	FinishedAt    string      `json:"finished_at,omitempty"`
	Error         string      `json:"error,omitempty"`
}

type IndexRequest struct {
	StorageID string   `json:"storage_id"`
	Recursive bool     `json:"recursive"`
	Exclude   []string `json:"exclude"`
}

func (m *Manager) SetIndexPublisher(publish func(string, any)) {
	m.indexMu.Lock()
	m.publishIndex = publish
	m.indexMu.Unlock()
}

func validExclusion(s string) bool {
	if s == "" || strings.Contains(s, "\\") || path.IsAbs(s) || strings.HasPrefix(s, "/") {
		return false
	}
	for _, part := range strings.Split(s, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return !strings.Contains(s, "\x00")
}

// excludePattern accepts a literal relative path, a subtree ending in /**,
// or ordinary path.Match patterns (e.g. *.tmp at root). No path escapes.
func excludePattern(rel, pattern string) bool {
	pattern = strings.TrimPrefix(pattern, "./")
	if strings.HasSuffix(pattern, "/**") {
		prefix := strings.TrimSuffix(pattern, "/**")
		return rel == prefix || strings.HasPrefix(rel, prefix+"/")
	}
	if rel == pattern {
		return true
	}
	matched, err := path.Match(pattern, rel)
	return err == nil && matched
}
func excluded(rel string, patterns []string) bool {
	for _, p := range patterns {
		if excludePattern(rel, p) {
			return true
		}
	}
	return false
}

func (m *Manager) BeginIndex(ctx context.Context, req IndexRequest, reindex bool) (IndexJob, error) {
	if !validID.MatchString(req.StorageID) || len(req.Exclude) > 32 {
		return IndexJob{}, ErrInvalid
	}
	patterns := append([]string{".homecloud/**", "backups/jupyter/**"}, req.Exclude...)
	for _, p := range patterns {
		if !validExclusion(p) {
			return IndexJob{}, ErrInvalid
		}
	}
	v, err := m.volume(ctx, req.StorageID)
	if err != nil {
		return IndexJob{}, err
	}
	if !v.Enabled {
		return IndexJob{}, ErrUnavailable
	}
	root, err := m.safeRoot(v.Path)
	if err != nil {
		return IndexJob{}, err
	}
	jobID, err := newID()
	if err != nil {
		return IndexJob{}, err
	}
	mode := "index"
	if reindex {
		mode = "reindex"
	}
	job := IndexJob{JobID: jobID, StorageID: req.StorageID, Mode: mode, Status: "running", StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Errors: []ScanError{}}
	m.indexMu.Lock()
	m.mu.Lock()
	closed := m.closed
	m.mu.Unlock()
	if closed {
		m.indexMu.Unlock()
		return IndexJob{}, ErrUnavailable
	}
	if m.indexJobs == nil {
		m.indexJobs = make(map[string]*IndexJob)
	}
	if m.indexCancels == nil {
		m.indexCancels = make(map[string]context.CancelFunc)
	}
	running := 0
	for _, j := range m.indexJobs {
		if j.Status == "running" {
			running++
			if j.StorageID == req.StorageID {
				m.indexMu.Unlock()
				return IndexJob{}, ErrBusy
			}
		}
	}
	if running >= indexMaxConcurrent {
		m.indexMu.Unlock()
		return IndexJob{}, ErrBusy
	}
	// Evict finished jobs, not the persistent index, after their status expires.
	for id, j := range m.indexJobs {
		if j.Status != "running" {
			when, _ := time.Parse(time.RFC3339Nano, j.FinishedAt)
			if time.Since(when) > time.Hour {
				delete(m.indexJobs, id)
			}
		}
	}
	workerCtx, cancel := context.WithTimeout(context.Background(), indexMaxDuration)
	m.indexJobs[jobID] = &job
	m.indexCancels[jobID] = cancel
	m.indexMu.Unlock()
	go m.runIndex(workerCtx, cancel, root, jobID, req.Recursive, patterns, reindex)
	return job, nil
}

func (m *Manager) IndexStatus(id string) (IndexJob, error) {
	if !validID.MatchString(id) {
		return IndexJob{}, ErrInvalid
	}
	m.indexMu.Lock()
	defer m.indexMu.Unlock()
	job := m.indexJobs[id]
	if job == nil {
		return IndexJob{}, ErrNotFound
	}
	result := *job
	result.Errors = append([]ScanError(nil), job.Errors...)
	return result, nil
}

func (m *Manager) CancelIndex(id string) error {
	if !validID.MatchString(id) {
		return ErrInvalid
	}
	m.indexMu.Lock()
	defer m.indexMu.Unlock()
	job := m.indexJobs[id]
	if job == nil {
		return ErrNotFound
	}
	if job.Status != "running" {
		return ErrInvalid
	}
	if cancel := m.indexCancels[id]; cancel != nil {
		cancel()
	}
	return nil
}

func (m *Manager) stopIndexJobs() {
	m.indexMu.Lock()
	for _, cancel := range m.indexCancels {
		cancel()
	}
	m.indexMu.Unlock()
}

func (m *Manager) notifyIndex(topic string, job IndexJob) {
	m.indexMu.Lock()
	callback := m.publishIndex
	m.indexMu.Unlock()
	if callback != nil {
		callback(topic, job)
	}
}

func (m *Manager) updateIndexJob(id string, modify func(*IndexJob)) IndexJob {
	m.indexMu.Lock()
	defer m.indexMu.Unlock()
	job := m.indexJobs[id]
	if job == nil {
		return IndexJob{}
	}
	modify(job)
	result := *job
	result.Errors = append([]ScanError(nil), job.Errors...)
	return result
}

// safeIndexedPath verifies that a discovered file remains inside the
// registered volume even if a directory was replaced by a symlink.
func (m *Manager) safeIndexedPath(v Volume, rel string) (string, error) {
	if rel == "" || filepath.IsAbs(rel) || path.IsAbs(rel) || strings.Contains(rel, "\\") || rel == ".." || strings.HasPrefix(rel, "../") || excluded(rel, []string{".homecloud/**", "backups/jupyter/**"}) {
		return "", ErrInvalid
	}
	root, err := m.safeRoot(v.Path)
	if err != nil {
		return "", err
	}
	candidate := filepath.Join(root, filepath.FromSlash(rel))
	if !isWithin(root, candidate) {
		return "", ErrInvalid
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(candidate)
	if err != nil {
		return "", err
	}
	if !isWithin(resolvedRoot, resolved) {
		return "", ErrInvalid
	}
	info, err := os.Stat(candidate)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() {
		return "", ErrInvalid
	}
	return candidate, nil
}

func (m *Manager) runIndex(ctx context.Context, cancel context.CancelFunc, root, id string, recursive bool, patterns []string, reindex bool) {
	defer cancel()
	defer func() { m.indexMu.Lock(); delete(m.indexCancels, id); m.indexMu.Unlock() }()
	job, _ := m.IndexStatus(id)
	lastProgress := time.Now()
	var seen, stored, bytesSeen, errCount int64
	errorsList := make([]ScanError, 0)
	addErr := func(rel string, err error) {
		errCount++
		if len(errorsList) < 10 {
			errorsList = append(errorsList, ScanError{Path: rel, Error: err.Error()})
		}
	}
	// All rows for this run are tagged with a unique seen_run. A failed or
	// incomplete walk cannot incorrectly mark previous rows as missing.
	walkErr := filepath.WalkDir(root, func(fp string, d fs.DirEntry, entryErr error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		rel, err := filepath.Rel(root, fp)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return entryErr
		}
		if excluded(rel, patterns) {
			if entryErr != nil {
				return nil
			}
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entryErr != nil {
			addErr(rel, entryErr)
			return nil
		}
		if d.IsDir() {
			if !recursive {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() {
			return nil
		} // no symlinks, junctions or special files
		info, err := d.Info()
		if err != nil {
			addErr(rel, err)
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		seen++
		bytesSeen += info.Size()
		if seen > indexMaxFiles {
			return errors.New("index file limit reached")
		}
		name := d.Name()
		mt := mime.TypeByExtension(filepath.Ext(name))
		if mt == "" {
			mt = "application/octet-stream"
		}
		indexedAt := time.Now().UTC().Format(time.RFC3339Nano)
		// Assign ID once; ON CONFLICT keeps the existing file_id stable.
		nextID, err := newID()
		if err != nil {
			return err
		}
		_, err = m.db.ExecContext(ctx, `INSERT INTO hc_storage_index(id,storage_id,relative_path,name,mime_type,size_bytes,modified_at,indexed_at,seen_run,available)
           VALUES(?,?,?,?,?,?,?,?,?,1)
           ON CONFLICT(storage_id,relative_path) DO UPDATE SET
           name=excluded.name,mime_type=excluded.mime_type,size_bytes=excluded.size_bytes,
           modified_at=excluded.modified_at,indexed_at=excluded.indexed_at,
           seen_run=excluded.seen_run,available=1`, nextID, job.StorageID, rel, name, mt, info.Size(), info.ModTime().UTC().Format(time.RFC3339Nano), indexedAt, id)
		if err != nil {
			return fmt.Errorf("index %s: %w", rel, err)
		}
		stored++
		if time.Since(lastProgress) >= indexProgressInterval {
			j := m.updateIndexJob(id, func(j *IndexJob) {
				j.FilesSeen = seen
				j.FilesIndexed = stored
				j.BytesSeen = bytesSeen
				j.ErrorsCount = errCount
				j.Errors = append([]ScanError(nil), errorsList...)
			})
			m.notifyIndex("storage.index.progress", j)
			lastProgress = time.Now()
		}
		return nil
	})
	complete := walkErr == nil && errCount == 0
	var missing int64
	if reindex && complete && recursive {
		// Only mark records missing in directories that were actually walked.
		// Entries deliberately excluded from this run remain untouched.
		rows, err := m.db.QueryContext(ctx, `SELECT id,relative_path FROM hc_storage_index WHERE storage_id=? AND seen_run<>? AND available=1`, job.StorageID, id)
		var ids []string
		if err == nil {
			for rows.Next() {
				var rowID, rel string
				if e := rows.Scan(&rowID, &rel); e != nil {
					err = e
					break
				}
				if !excluded(rel, patterns) {
					ids = append(ids, rowID)
				}
			}
			if e := rows.Err(); err == nil {
				err = e
			}
			rows.Close()
		}
		if err == nil {
			for _, missingID := range ids {
				res, e := m.db.ExecContext(ctx, `UPDATE hc_storage_index SET available=0 WHERE id=? AND seen_run<>?`, missingID, id)
				if e != nil {
					err = e
					break
				}
				n, _ := res.RowsAffected()
				missing += n
			}
		}
		if err != nil {
			walkErr = err
			complete = false
		}
	}
	state := "completed"
	reason := ""
	if walkErr != nil {
		state = "failed"
		reason = walkErr.Error()
	}
	if state == "completed" && !complete {
		state = "partial"
	}
	if errors.Is(walkErr, context.Canceled) {
		state = "cancelled"
	}
	finished := m.updateIndexJob(id, func(j *IndexJob) {
		j.Status = state
		j.FilesSeen = seen
		j.FilesIndexed = stored
		j.BytesSeen = bytesSeen
		j.ErrorsCount = errCount
		j.Errors = append([]ScanError(nil), errorsList...)
		j.Complete = complete
		j.MissingMarked = missing
		j.Error = reason
		j.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	})
	m.notifyIndex("storage.index.finished", finished)
}
