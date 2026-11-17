package telemetry

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"strings"
	"time"
)

const (
	rawResolution = int64(1000)
	tenResolution = int64(10000)
	minResolution = int64(60000)
	writeBatchSize = 256
)

type SQLiteHistory struct {
	db       *sql.DB
	queue    chan []Sample
	stop     chan struct{}
	done     chan struct{}
	mu       sync.RWMutex
	closed   bool
	closeErr error
	rawRetention time.Duration
	tenRetention time.Duration
	minuteRetention time.Duration
}

func InitSQLiteSchema(ctx context.Context, db *sql.DB) error {
	const schema = `
CREATE TABLE IF NOT EXISTS hc_metric_samples (
 node_id TEXT NOT NULL,
 metric TEXT NOT NULL,
 labels_json TEXT NOT NULL,
 resolution_ms INTEGER NOT NULL,
 timestamp_ms INTEGER NOT NULL,
 value REAL NOT NULL,
 min_value REAL NOT NULL,
 max_value REAL NOT NULL,
 sample_count INTEGER NOT NULL DEFAULT 1,
 unit TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(node_id, metric, labels_json, resolution_ms, timestamp_ms)
);
CREATE INDEX IF NOT EXISTS idx_hc_metric_samples_range
 ON hc_metric_samples(node_id, metric, resolution_ms, timestamp_ms);
CREATE INDEX IF NOT EXISTS idx_hc_metric_samples_retention
 ON hc_metric_samples(resolution_ms, timestamp_ms);
`
	for _, statement := range strings.Split(schema, ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("initialize telemetry history schema: %w", err)
		}
	}
	return nil
}

func NewSQLiteHistory(db *sql.DB, queueSize int) (*SQLiteHistory, error) {
	return NewSQLiteHistoryWithRetention(db, queueSize, time.Hour, 24*time.Hour, 14*24*time.Hour)
}

func NewSQLiteHistoryWithRetention(db *sql.DB, queueSize int, rawRetention, tenRetention, minuteRetention time.Duration) (*SQLiteHistory, error) {
	if db == nil {
		return nil, errors.New("telemetry database is nil")
	}
	if queueSize <= 0 {
		queueSize = 256
	}
	if rawRetention <= 0 || tenRetention <= 0 || minuteRetention <= 0 {
		return nil, errors.New("telemetry retention durations must be positive")
	}
	h := &SQLiteHistory{
		db: db, queue: make(chan []Sample, queueSize),
		stop: make(chan struct{}), done: make(chan struct{}),
		rawRetention: rawRetention, tenRetention: tenRetention, minuteRetention: minuteRetention,
	}
	go h.run()
	return h, nil
}

// Append is deliberately non-blocking. A full queue is observable by the caller
// and cannot stall WebSocket command handling or the SQLite users.
func (h *SQLiteHistory) Append(ctx context.Context, samples []Sample) error {
	if len(samples) == 0 {
		return nil
	}
	batch := make([]Sample, len(samples))
	copy(batch, samples)
	for i := range batch {
		if err := batch[i].Validate(); err != nil {
			return fmt.Errorf("sample %d: %w", i, err)
		}
		batch[i].Labels = cloneLabels(batch[i].Labels)
		batch[i].Timestamp = batch[i].Timestamp.UTC()
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	if h.closed {
		return ErrClosed
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case h.queue <- batch:
		return nil
	default:
		return ErrQueueFull
	}
}

func (h *SQLiteHistory) Query(ctx context.Context, q Query) ([]Point, error) {
	if q.From.IsZero() || q.To.IsZero() || !q.To.After(q.From) {
		return nil, ErrInvalidQuery
	}
	if q.NodeID == "" || len(q.NodeID) > 128 || q.Metric == "" || len(q.Metric) > 160 {
		return nil, ErrInvalidQuery
	}
	if q.Limit <= 0 {
		q.Limit = 5000
	}
	if q.Limit > 20000 {
		q.Limit = 20000
	}
	resolution := q.Resolution
	if resolution <= 0 {
		span := q.To.Sub(q.From)
		switch {
		case span <= time.Hour:
			resolution = time.Second
		case span <= 24*time.Hour:
			resolution = 10 * time.Second
		default:
			resolution = time.Minute
		}
	}
	resolutionMS := resolution.Milliseconds()
	if resolutionMS != rawResolution && resolutionMS != tenResolution && resolutionMS != minResolution {
		return nil, ErrInvalidQuery
	}

	rows, err := h.db.QueryContext(ctx, `
SELECT node_id, metric, labels_json, timestamp_ms, value, min_value, max_value, sample_count, unit
FROM hc_metric_samples
WHERE node_id = ? AND metric = ? AND resolution_ms = ?
  AND timestamp_ms >= ? AND timestamp_ms < ?
ORDER BY timestamp_ms ASC
LIMIT ?`, q.NodeID, q.Metric, resolutionMS, q.From.UTC().UnixMilli(), q.To.UTC().UnixMilli(), q.Limit)
	if err != nil {
		return nil, fmt.Errorf("query telemetry history: %w", err)
	}
	defer rows.Close()
	points := make([]Point, 0)
	for rows.Next() {
		var p Point
		var labels string
		var ts int64
		if err := rows.Scan(&p.NodeID, &p.Name, &labels, &ts, &p.Value, &p.Min, &p.Max, &p.Count, &p.Unit); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(labels), &p.Labels); err != nil {
			return nil, fmt.Errorf("decode metric labels: %w", err)
		}
		p.Timestamp = time.UnixMilli(ts).UTC()
		points = append(points, p)
	}
	return points, rows.Err()
}

func (h *SQLiteHistory) run() {
	defer close(h.done)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	pending := make([]Sample, 0, writeBatchSize)
	lastCompactedBucket := int64(-1)
	hasSamples := false
	flush := func() {
		if len(pending) == 0 {
			return
		}
		writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := h.writeBatch(writeCtx, pending)
		cancel()
		if err != nil {
			h.mu.Lock()
			h.closeErr = errors.Join(h.closeErr, err)
			h.mu.Unlock()
		}
		pending = pending[:0]
	}
	for {
		select {
		case batch := <-h.queue:
			hasSamples = true
			for len(batch) > 0 {
				space := writeBatchSize - len(pending)
				if space > len(batch) {
					space = len(batch)
				}
				pending = append(pending, batch[:space]...)
				batch = batch[space:]
				if len(pending) >= writeBatchSize {
					flush()
				}
			}
		case <-ticker.C:
			flush()
			now := time.Now().UTC()
			bucket := now.UnixMilli() / tenResolution
			if hasSamples && bucket != lastCompactedBucket {
				compactCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := h.compact(compactCtx, now)
				cancel()
				if err == nil {
					lastCompactedBucket = bucket
					hasSamples = false
				}
			}
		case <-h.stop:
			for {
				select {
				case batch := <-h.queue:
					pending = append(pending, batch...)
				default:
					flush()
					return
				}
			}
		}
	}
}

func (h *SQLiteHistory) writeBatch(ctx context.Context, samples []Sample) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `
INSERT INTO hc_metric_samples
(node_id,metric,labels_json,resolution_ms,timestamp_ms,value,min_value,max_value,sample_count,unit)
VALUES(?,?,?,?,?,?,?,?,1,?)
ON CONFLICT(node_id,metric,labels_json,resolution_ms,timestamp_ms)
DO UPDATE SET value=excluded.value,min_value=excluded.min_value,
 max_value=excluded.max_value,sample_count=excluded.sample_count,unit=excluded.unit`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer stmt.Close()
	for _, sample := range samples {
		labels, err := encodeLabels(sample.Labels)
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		ts := sample.Timestamp.UnixMilli()
		ts = ts / rawResolution * rawResolution
		if _, err := stmt.ExecContext(ctx, sample.NodeID, sample.Name, labels, rawResolution, ts,
			sample.Value, sample.Value, sample.Value, sample.Unit); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	return tx.Commit()
}

func (h *SQLiteHistory) compact(ctx context.Context, now time.Time) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	nowMS := now.UnixMilli()
	tenEnd := nowMS / tenResolution * tenResolution
	tenStart := tenEnd - tenResolution
	_, err = tx.ExecContext(ctx, `
INSERT OR REPLACE INTO hc_metric_samples
(node_id,metric,labels_json,resolution_ms,timestamp_ms,value,min_value,max_value,sample_count,unit)
SELECT node_id,metric,labels_json,?, ?, AVG(value),MIN(min_value),MAX(max_value),SUM(sample_count),unit
FROM hc_metric_samples
WHERE resolution_ms=? AND timestamp_ms>=? AND timestamp_ms<?
GROUP BY node_id,metric,labels_json,unit`, tenResolution, tenStart, rawResolution, tenStart, tenEnd)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	// Rebuild the most recently completed minute on every compaction pass.
	// This also retries a minute rollup if SQLite was temporarily busy at its boundary.
	minEnd := nowMS / minResolution * minResolution
	minStart := minEnd - minResolution
	_, err = tx.ExecContext(ctx, `
INSERT OR REPLACE INTO hc_metric_samples
(node_id,metric,labels_json,resolution_ms,timestamp_ms,value,min_value,max_value,sample_count,unit)
SELECT node_id,metric,labels_json,?, ?, SUM(value*sample_count)/MAX(SUM(sample_count),1),
 MIN(min_value),MAX(max_value),SUM(sample_count),unit
FROM hc_metric_samples
WHERE resolution_ms=? AND timestamp_ms>=? AND timestamp_ms<?
GROUP BY node_id,metric,labels_json,unit`, minResolution, minStart, tenResolution, minStart, minEnd)
	if err != nil {
		_ = tx.Rollback()
		return err
	}

	return tx.Commit()
}

func (h *SQLiteHistory) prune(ctx context.Context, now time.Time) error {
	tx, err := h.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	nowMS := now.UnixMilli()
	_, err = tx.ExecContext(ctx, `
DELETE FROM hc_metric_samples
WHERE (resolution_ms=? AND timestamp_ms<?)
   OR (resolution_ms=? AND timestamp_ms<?)
   OR (resolution_ms=? AND timestamp_ms<?)`,
		rawResolution, nowMS-h.rawRetention.Milliseconds(),
		tenResolution, nowMS-h.tenRetention.Milliseconds(),
		minResolution, nowMS-h.minuteRetention.Milliseconds())
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (h *SQLiteHistory) Close() error {
	h.mu.Lock()
	if !h.closed {
		h.closed = true
		close(h.stop)
	}
	h.mu.Unlock()
	<-h.done
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.closeErr
}

func encodeLabels(labels map[string]string) (string, error) {
	if len(labels) == 0 {
		return "{}", nil
	}
	// encoding/json sorts string map keys; sorting here also makes the
	// canonicalization guarantee explicit and stable across Go versions.
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	canonical := make(map[string]string, len(labels))
	for _, key := range keys {
		canonical[key] = labels[key]
	}
	raw, err := json.Marshal(canonical)
	return string(raw), err
}

func cloneLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	copy := make(map[string]string, len(labels))
	for key, value := range labels {
		copy[key] = value
	}
	return copy
}

