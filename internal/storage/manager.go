// Package storage manages registered local/NFS/SMB directories and indexed files.
// Dynamic volumes and file metadata live in SQLite. Paths never come from the
// binary WebSocket frames, and no volumes are automatically registered.
package storage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"HomeCloudBackend/wsconfig"
	"github.com/shirou/gopsutil/v4/disk"
)

var (
	ErrUnavailable = errors.New("storage unavailable or disabled")
	ErrNotFound    = errors.New("storage or file not found")
	ErrInvalid     = errors.New("invalid storage parameters")
	ErrBusy        = errors.New("too many active storage transfers")
)

const (
	MaxActiveTransfers       = 16
	TransferTTL              = 2 * time.Minute
	MaxChunkBytes      int64 = 1 << 20
)

var validID = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

type Volume struct {
	ID             string `json:"storage_id"`
	Name           string `json:"name"`
	Path           string `json:"path"`
	Priority       int    `json:"priority"`
	Enabled        bool   `json:"enabled"`
	AvailableBytes uint64 `json:"available_bytes"`
	TotalBytes     uint64 `json:"total_bytes"`
	Online         bool   `json:"online"`
}

type File struct {
	ID           string `json:"file_id"`
	StorageID    string `json:"storage_id"`
	Name         string `json:"name"`
	MIMEType     string `json:"mime_type"`
	SizeBytes    int64  `json:"size_bytes"`
	SHA256       string `json:"sha256"`
	CreatedAt    string `json:"created_at"`
	UploadedBy   string `json:"uploaded_by"`
	Source       string `json:"source,omitempty"`
	RelativePath string `json:"relative_path,omitempty"`
	Available    bool   `json:"available"`
}

type TransferInfo struct {
	TransferID string `json:"transfer_id"`
	Role       string `json:"role"`
	Path       string `json:"path"`
	Name       string `json:"name"`
	MIMEType   string `json:"mime_type"`
	SizeBytes  int64  `json:"size_bytes"`
	StorageID  string `json:"storage_id"`
	FileID     string `json:"file_id,omitempty"`
	ExpiresAt  string `json:"expires_at"`
}

type Transfer struct {
	TransferInfo
	Owner      string
	Volume     Volume
	File       File
	SourcePath string // computed server-side after safe path validation
	used       bool
	expires    time.Time
}

type Manager struct {
	db           *sql.DB
	cfg          wsconfig.StorageConfig
	root         string
	allowed      []string
	mu           sync.Mutex
	pending      map[string]*Transfer
	active       map[string]*Transfer
	closed       bool
	indexMu      sync.Mutex
	indexJobs    map[string]*IndexJob
	indexCancels map[string]context.CancelFunc
	publishIndex func(string, any)
}

func newID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(raw[:]), nil
}

func resolve(root, p string) (string, error) {
	if p == "" {
		return "", ErrInvalid
	}
	if !filepath.IsAbs(p) {
		p = filepath.Join(root, p)
	}
	return filepath.Abs(filepath.Clean(p))
}

// isWithin treats relative segments as components, not string prefixes.
func isWithin(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	return err == nil && rel != ".." && !filepath.IsAbs(rel) && !strings.HasPrefix(rel, ".."+string(os.PathSeparator))
}

func New(cfg wsconfig.StorageConfig, db *sql.DB, projectRoot string) (*Manager, error) {
	if !cfg.Enabled || db == nil || len(cfg.AllowedRoots) == 0 {
		return nil, ErrUnavailable
	}
	root, err := filepath.Abs(projectRoot)
	if err != nil {
		return nil, err
	}
	m := &Manager{db: db, cfg: cfg, root: root, pending: make(map[string]*Transfer), active: make(map[string]*Transfer), indexJobs: make(map[string]*IndexJob), indexCancels: make(map[string]context.CancelFunc)}
	for _, entry := range cfg.AllowedRoots {
		p, err := resolve(root, entry)
		if err != nil {
			return nil, err
		}
		m.allowed = append(m.allowed, p)
	}
	if cfg.MaxUploadSizeMB <= 0 {
		return nil, errors.New("storage.max_upload_size_mb must be positive when enabled")
	}
	return m, nil
}

// safeRoot checks configuration allowlist and avoids following a symlink or
// junction to an unapproved location. An unavailable mounted volume fails.
func (m *Manager) safeRoot(path string) (string, error) {
	p, err := resolve(m.root, path)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(p)
	if err != nil || !info.IsDir() {
		return "", errors.New("storage directory missing or not a directory")
	}
	actual, err := filepath.EvalSymlinks(p)
	if err != nil {
		return "", err
	}
	actual, err = filepath.Abs(actual)
	if err != nil {
		return "", err
	}
	allowed := false
	for _, entry := range m.allowed {
		// The configured allowed root also must exist; fail closed otherwise.
		base, err := filepath.EvalSymlinks(entry)
		if err != nil {
			continue
		}
		base, err = filepath.Abs(base)
		if err == nil && isWithin(base, actual) && isWithin(entry, p) {
			allowed = true
			break
		}
	}
	if !allowed {
		return "", errors.New("storage root not under allowed_roots")
	}
	return p, nil
}

func volumeOnline(v Volume) Volume {
	stats, err := disk.Usage(v.Path)
	if err == nil && stats != nil {
		v.Online = true
		v.AvailableBytes = stats.Free
		v.TotalBytes = stats.Total
	}
	return v
}

func (m *Manager) Register(ctx context.Context, name, path string, priority int) (Volume, error) {
	if len(name) == 0 || len(name) > 100 || priority < 0 || priority > 1000 {
		return Volume{}, ErrInvalid
	}
	p, err := m.safeRoot(path)
	if err != nil {
		return Volume{}, err
	}
	v := volumeOnline(Volume{Name: name, Path: p, Priority: priority, Enabled: true})
	if !v.Online {
		return Volume{}, ErrUnavailable
	}
	// An existing read-only mount is not a valid upload destination.
	if err := m.ensureDirs(v); err != nil {
		return Volume{}, err
	}
	id, err := newID()
	if err != nil {
		return Volume{}, err
	}
	v.ID = id
	_, err = m.db.ExecContext(ctx, `INSERT INTO hc_storage_volumes(id,name,path,priority,enabled) VALUES(?,?,?,?,1)`, id, name, p, priority)
	if err != nil {
		return Volume{}, err
	}
	return v, nil
}

func (m *Manager) List(ctx context.Context) ([]Volume, error) {
	rows, err := m.db.QueryContext(ctx, `SELECT id,name,path,priority,enabled FROM hc_storage_volumes ORDER BY priority DESC,name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Volume, 0)
	for rows.Next() {
		var v Volume
		var enabled int
		if err := rows.Scan(&v.ID, &v.Name, &v.Path, &v.Priority, &enabled); err != nil {
			return nil, err
		}
		v.Enabled = enabled != 0
		// A removed mount never silently points at a different path.
		if _, err := m.safeRoot(v.Path); err == nil {
			v = volumeOnline(v)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ListFiles includes managed uploads and indexed (unmanaged) files.
// Indexed files have no SHA-256 unless downloaded and hashed by the client.
func (m *Manager) ListFiles(ctx context.Context, storageID string, limit int) ([]File, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	query := `SELECT id,storage_id,name,mime_type,size_bytes,sha256,created_at,uploaded_by,'managed' AS source,'' AS relative_path,1 AS available
       FROM hc_storage_files
       UNION ALL
       SELECT id,storage_id,name,mime_type,size_bytes,'' AS sha256,indexed_at AS created_at,'' AS uploaded_by,'indexed' AS source,relative_path,available
       FROM hc_storage_index`
	args := []any{}
	if storageID != "" {
		if !validID.MatchString(storageID) {
			return nil, ErrInvalid
		}
		query = `SELECT * FROM (` + query + `) WHERE storage_id=? ORDER BY created_at DESC LIMIT ?`
		args = append(args, storageID)
	} else {
		query = `SELECT * FROM (` + query + `) ORDER BY created_at DESC LIMIT ?`
	}
	args = append(args, limit)
	rows, err := m.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]File, 0)
	for rows.Next() {
		var f File
		var available int
		if err := rows.Scan(&f.ID, &f.StorageID, &f.Name, &f.MIMEType, &f.SizeBytes, &f.SHA256, &f.CreatedAt, &f.UploadedBy, &f.Source, &f.RelativePath, &available); err != nil {
			return nil, err
		}
		f.Available = available != 0
		out = append(out, f)
	}
	return out, rows.Err()
}

func (m *Manager) file(ctx context.Context, id string) (File, error) {
	var f File
	err := m.db.QueryRowContext(ctx, `SELECT id,storage_id,name,mime_type,size_bytes,sha256,created_at,uploaded_by FROM hc_storage_files WHERE id=?`, id).Scan(&f.ID, &f.StorageID, &f.Name, &f.MIMEType, &f.SizeBytes, &f.SHA256, &f.CreatedAt, &f.UploadedBy)
	if err == nil {
		f.Source = "managed"
		f.Available = true
		return f, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return f, err
	}
	var available int
	err = m.db.QueryRowContext(ctx, `SELECT id,storage_id,name,mime_type,size_bytes,indexed_at,relative_path,available FROM hc_storage_index WHERE id=?`, id).Scan(&f.ID, &f.StorageID, &f.Name, &f.MIMEType, &f.SizeBytes, &f.CreatedAt, &f.RelativePath, &available)
	f.Available = available != 0
	if errors.Is(err, sql.ErrNoRows) {
		return f, ErrNotFound
	}
	if err != nil {
		return f, err
	}
	f.Source = "indexed"
	return f, nil
}

func (m *Manager) GetFile(ctx context.Context, id string) (File, error) {
	if !validID.MatchString(id) {
		return File{}, ErrInvalid
	}
	return m.file(ctx, id)
}
func (m *Manager) volume(ctx context.Context, id string) (Volume, error) {
	var v Volume
	var enabled int
	err := m.db.QueryRowContext(ctx, `SELECT id,name,path,priority,enabled FROM hc_storage_volumes WHERE id=?`, id).Scan(&v.ID, &v.Name, &v.Path, &v.Priority, &enabled)
	v.Enabled = enabled != 0
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	return v, err
}

func (m *Manager) selectVolume(ctx context.Context, storageID string, size int64) (Volume, error) {
	var choices []Volume
	if storageID != "" && storageID != "auto" {
		v, err := m.volume(ctx, storageID)
		if err != nil {
			return v, err
		}
		choices = []Volume{v}
	} else {
		var err error
		choices, err = m.List(ctx)
		if err != nil {
			return Volume{}, err
		}
	}
	sort.SliceStable(choices, func(i, j int) bool { return choices[i].Priority > choices[j].Priority })
	for _, v := range choices {
		if !v.Enabled {
			continue
		}
		p, err := m.safeRoot(v.Path)
		if err != nil {
			continue
		}
		v.Path = p
		v = volumeOnline(v)
		if !v.Online || uint64(size) > v.AvailableBytes {
			continue
		}
		if err := m.ensureDirs(v); err == nil {
			return v, nil
		}
	}
	return Volume{}, ErrUnavailable
}

// ensureDirs disallows symlinks for our private file/index subdirectories.
func (m *Manager) ensureDirs(v Volume) error {
	for _, sub := range []string{".homecloud", filepath.Join(".homecloud", "objects"), filepath.Join(".homecloud", "tmp")} {
		p := filepath.Join(v.Path, sub)
		if err := os.MkdirAll(p, 0700); err != nil {
			return err
		}
		fi, err := os.Lstat(p)
		if err != nil {
			return err
		}
		if fi.Mode()&os.ModeSymlink != 0 || !fi.IsDir() {
			return errors.New("unsafe storage subdirectory")
		}
	}
	return nil
}

func (m *Manager) filePath(v Volume, id string) string {
	return filepath.Join(v.Path, ".homecloud", "objects", id+".bin")
}
func (m *Manager) tempPath(v Volume, id string) string {
	return filepath.Join(v.Path, ".homecloud", "tmp", id+".part")
}

func (m *Manager) PrepareUpload(ctx context.Context, owner, name, mime, storageID string, size int64) (TransferInfo, error) {
	if owner == "" || name == "" || len(name) > 255 || strings.ContainsAny(name, "\x00\r\n") || len(mime) > 128 || size < 0 || size > int64(m.cfg.MaxUploadSizeMB)*1024*1024 {
		return TransferInfo{}, ErrInvalid
	}
	v, err := m.selectVolume(ctx, storageID, size)
	if err != nil {
		return TransferInfo{}, err
	}
	id, err := newID()
	if err != nil {
		return TransferInfo{}, err
	}
	fileID, err := newID()
	if err != nil {
		return TransferInfo{}, err
	}
	deadline := time.Now().Add(TransferTTL)
	t := &Transfer{TransferInfo: TransferInfo{TransferID: id, Role: "upload", Path: "/ws/storage", Name: name, MIMEType: mime, SizeBytes: size, StorageID: v.ID, FileID: fileID, ExpiresAt: deadline.UTC().Format(time.RFC3339)}, Owner: owner, Volume: v, expires: deadline}
	if err := m.put(t); err != nil {
		return TransferInfo{}, err
	}
	return t.TransferInfo, nil
}

func (m *Manager) PrepareDownload(ctx context.Context, owner, fileID string) (TransferInfo, error) {
	if owner == "" || !validID.MatchString(fileID) {
		return TransferInfo{}, ErrInvalid
	}
	f, err := m.file(ctx, fileID)
	if err != nil {
		return TransferInfo{}, err
	}
	v, err := m.volume(ctx, f.StorageID)
	if err != nil {
		return TransferInfo{}, err
	}
	if !v.Enabled {
		return TransferInfo{}, ErrUnavailable
	}
	if _, err := m.safeRoot(v.Path); err != nil {
		return TransferInfo{}, err
	}
	var sourcePath string
	if f.Source == "indexed" {
		if !f.Available {
			return TransferInfo{}, ErrUnavailable
		}
		sourcePath, err = m.safeIndexedPath(v, f.RelativePath)
		if err != nil {
			return TransferInfo{}, err
		}
	} else {
		sourcePath = m.filePath(v, fileID)
		if _, err := os.Stat(sourcePath); err != nil {
			return TransferInfo{}, err
		}
	}
	id, err := newID()
	if err != nil {
		return TransferInfo{}, err
	}
	deadline := time.Now().Add(TransferTTL)
	t := &Transfer{TransferInfo: TransferInfo{TransferID: id, Role: "download", Path: "/ws/storage", Name: f.Name, MIMEType: f.MIMEType, SizeBytes: f.SizeBytes, StorageID: v.ID, FileID: f.ID, ExpiresAt: deadline.UTC().Format(time.RFC3339)}, Owner: owner, Volume: v, File: f, SourcePath: sourcePath, expires: deadline}
	if err := m.put(t); err != nil {
		return TransferInfo{}, err
	}
	return t.TransferInfo, nil
}

func (m *Manager) put(t *Transfer) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrUnavailable
	}
	now := time.Now()
	for id, old := range m.pending {
		if now.After(old.expires) {
			delete(m.pending, id)
		}
	}
	if len(m.pending) >= MaxActiveTransfers {
		return ErrBusy
	}
	m.pending[t.TransferID] = t
	return nil
}

// Claim is one-shot. Client IDs are routing labels, not security credentials.
func (m *Manager) Claim(id, owner, role string) (*Transfer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.pending[id]
	if t == nil || time.Now().After(t.expires) {
		delete(m.pending, id)
		return nil, ErrNotFound
	}
	if t.Owner != owner || t.Role != role || t.used {
		return nil, ErrInvalid
	}
	if len(m.active) >= MaxActiveTransfers {
		return nil, ErrBusy
	}
	t.used = true
	delete(m.pending, id)
	m.active[id] = t
	return t, nil
}

func (m *Manager) Cancel(owner, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := m.pending[id]
	if t == nil {
		return ErrNotFound
	}
	if t.Owner != owner {
		return ErrInvalid
	}
	delete(m.pending, id)
	return nil
}

func (m *Manager) Finish(t *Transfer) {
	m.mu.Lock()
	if m.active[t.TransferID] == t {
		delete(m.active, t.TransferID)
	}
	m.mu.Unlock()
}

func (m *Manager) CloseAll() {
	m.mu.Lock()
	m.closed = true
	m.pending = make(map[string]*Transfer)
	m.mu.Unlock()
	m.stopIndexJobs()
}

func (m *Manager) CommitUpload(ctx context.Context, t *Transfer, sha string) (File, error) {
	// Only the stream endpoint can call this after a complete, size-checked transfer.
	v := t.Volume
	if _, err := m.safeRoot(v.Path); err != nil {
		return File{}, err
	}
	source := m.tempPath(v, t.TransferID)
	target := m.filePath(v, t.FileID)
	if err := os.Rename(source, target); err != nil {
		return File{}, err
	}
	f := File{ID: t.FileID, StorageID: v.ID, Name: t.Name, MIMEType: t.MIMEType, SizeBytes: t.SizeBytes, SHA256: sha, CreatedAt: time.Now().UTC().Format(time.RFC3339Nano), UploadedBy: t.Owner, Source: "managed", Available: true}
	_, err := m.db.ExecContext(ctx, `INSERT INTO hc_storage_files(id,storage_id,name,mime_type,size_bytes,sha256,created_at,uploaded_by) VALUES(?,?,?,?,?,?,?,?)`, f.ID, f.StorageID, f.Name, f.MIMEType, f.SizeBytes, f.SHA256, f.CreatedAt, f.UploadedBy)
	if err != nil {
		_ = os.Remove(target)
		return File{}, err
	}
	return f, nil
}

func (m *Manager) TempPath(t *Transfer) string                            { return m.tempPath(t.Volume, t.TransferID) }
func (m *Manager) ObjectPath(t *Transfer) string                          { return m.filePath(t.Volume, t.FileID) }
func (m *Manager) MaxSize() int64                                         { return int64(m.cfg.MaxUploadSizeMB) * 1024 * 1024 }
func (m *Manager) CheckFile(ctx context.Context, id string) (File, error) { return m.file(ctx, id) }
func (m *Manager) String() string {
	return fmt.Sprintf("StorageManager(%d allowed_roots)", len(m.allowed))
}
