package physicaldevices

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

var ErrNotFound = errors.New("physical device not found")

type Record struct {
	DeviceID           string          `json:"device_id"`
	DeviceTypeID       string          `json:"device_type_id"`
	DeviceTypeVersion  int             `json:"device_type_version"`
	Metadata           json.RawMessage `json:"metadata"`
	CapabilityRevision int64           `json:"capability_revision"`
	Status             string          `json:"status"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
	LastSeenAt         *time.Time      `json:"last_seen_at,omitempty"`
}

type Store struct { db *sql.DB }

func New(db *sql.DB) *Store { return &Store{db: db} }

func (s *Store) Get(ctx context.Context, deviceID string) (Record, error) {
	var item Record
	var created, updated string
	var lastSeen sql.NullString
	err := s.db.QueryRowContext(ctx, `SELECT device_id,device_type_id,device_type_version,metadata_json,
		capability_revision,status,created_at,updated_at,last_seen_at
		FROM hc_physical_devices WHERE device_id=?`, deviceID).Scan(
		&item.DeviceID, &item.DeviceTypeID, &item.DeviceTypeVersion, &item.Metadata,
		&item.CapabilityRevision, &item.Status, &created, &updated, &lastSeen)
	if errors.Is(err, sql.ErrNoRows) {
		return Record{}, ErrNotFound
	}
	if err != nil {
		return Record{}, fmt.Errorf("get physical device: %w", err)
	}
	if item.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
		return Record{}, fmt.Errorf("parse physical device created_at: %w", err)
	}
	if item.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil {
		return Record{}, fmt.Errorf("parse physical device updated_at: %w", err)
	}
	if lastSeen.Valid {
		t, parseErr := time.Parse(time.RFC3339Nano, lastSeen.String)
		if parseErr != nil { return Record{}, fmt.Errorf("parse physical device last_seen_at: %w", parseErr) }
		item.LastSeenAt = &t
	}
	return item, nil
}

func (s *Store) List(ctx context.Context) ([]Record, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT device_id,device_type_id,device_type_version,metadata_json,
		capability_revision,status,created_at,updated_at,last_seen_at
		FROM hc_physical_devices WHERE status!='removed' ORDER BY device_id`)
	if err != nil { return nil, fmt.Errorf("list physical devices: %w", err) }
	defer rows.Close()
	out := make([]Record, 0)
	for rows.Next() {
		var item Record
		var created, updated string
		var lastSeen sql.NullString
		if err := rows.Scan(&item.DeviceID,&item.DeviceTypeID,&item.DeviceTypeVersion,&item.Metadata,
			&item.CapabilityRevision,&item.Status,&created,&updated,&lastSeen); err != nil {
			return nil, fmt.Errorf("scan physical device: %w", err)
		}
		if item.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil { return nil, err }
		if item.UpdatedAt, err = time.Parse(time.RFC3339Nano, updated); err != nil { return nil, err }
		if lastSeen.Valid {
			t, parseErr := time.Parse(time.RFC3339Nano,lastSeen.String)
			if parseErr != nil { return nil, parseErr }
			item.LastSeenAt=&t
		}
		out=append(out,item)
	}
	if err:=rows.Err(); err!=nil { return nil, fmt.Errorf("iterate physical devices: %w",err) }
	return out,nil
}
