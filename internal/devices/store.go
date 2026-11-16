// Package devices stores persistent metadata for clients known to a HomeCloud node.
// Live connection state remains owned by internal/connections.
package devices

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var ErrNotFound = errors.New("device not found")

type Record struct {
	DeviceID    string `json:"device_id"`
	ClientType  string `json:"client_type"`
	Name        string `json:"name"`
	FirstSeenAt int64  `json:"first_seen_at"`
	LastSeenAt  int64  `json:"last_seen_at"`
	Online      bool   `json:"online"`
}

type Store struct { db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

// Register records a successful WebSocket identity without replacing a
// user-assigned name. Timestamps are Unix seconds to match the UI contract.
func (s *Store) Register(ctx context.Context, deviceID, clientType string) error {
	now := time.Now().UTC().Unix()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO hc_devices(device_id, client_type, name, first_seen_at, last_seen_at, updated_at)
		VALUES(?, ?, '', ?, ?, ?)
		ON CONFLICT(device_id) DO UPDATE SET
			client_type = excluded.client_type,
			last_seen_at = excluded.last_seen_at,
			updated_at = excluded.updated_at
	`, deviceID, clientType, now, now, now)
	if err != nil { return fmt.Errorf("register device: %w", err) }
	return nil
}

func (s *Store) List(ctx context.Context, online func(string) bool) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT device_id, client_type, name, first_seen_at, last_seen_at
		FROM hc_devices ORDER BY device_id
	`)
	if err != nil { return nil, fmt.Errorf("list devices: %w", err) }
	defer rows.Close()
	out := make([]Record, 0)
	for rows.Next() {
		var item Record
		if err := rows.Scan(&item.DeviceID, &item.ClientType, &item.Name, &item.FirstSeenAt, &item.LastSeenAt); err != nil {
			return nil, fmt.Errorf("scan device: %w", err)
		}
		if online != nil { item.Online = online(item.DeviceID) }
		out = append(out, item)
	}
	if err := rows.Err(); err != nil { return nil, fmt.Errorf("iterate devices: %w", err) }
	return out, nil
}

func (s *Store) Rename(ctx context.Context, deviceID, name string) (Record, error) {
	name = strings.TrimSpace(name)
	if !utf8.ValidString(name) || utf8.RuneCountInString(name) < 1 || utf8.RuneCountInString(name) > 64 {
		return Record{}, errors.New("device name must contain 1 to 64 characters")
	}
	for _, r := range name {
		if unicode.IsControl(r) { return Record{}, errors.New("device name contains control characters") }
	}
	now := time.Now().UTC().Unix()
	result, err := s.db.ExecContext(ctx, `
		UPDATE hc_devices SET name = ?, updated_at = ? WHERE device_id = ?
	`, name, now, deviceID)
	if err != nil { return Record{}, fmt.Errorf("rename device: %w", err) }
	affected, err := result.RowsAffected()
	if err != nil { return Record{}, fmt.Errorf("check renamed device: %w", err) }
	if affected == 0 { return Record{}, ErrNotFound }
	var item Record
	err = s.db.QueryRowContext(ctx, `
		SELECT device_id, client_type, name, first_seen_at, last_seen_at
		FROM hc_devices WHERE device_id = ?
	`, deviceID).Scan(&item.DeviceID, &item.ClientType, &item.Name, &item.FirstSeenAt, &item.LastSeenAt)
	if err != nil { return Record{}, fmt.Errorf("read renamed device: %w", err) }
	return item, nil
}
