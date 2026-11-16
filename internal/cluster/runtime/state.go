// Package runtime coordinates the existing HomeCloud identity, secure Raft,
// durable registry, first-use TLS and membership protocol. It provides no
// anonymous HTTP or WebSocket administrative route.
package runtime

import (
	"HomeCloudBackend/internal/cluster/registry"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const stateVersion = 1

type persisted struct {
	Version            int                 `json:"version"`
	ClusterID          string              `json:"cluster_id"`
	Mode               string              `json:"mode"` // bootstrapping | joining | active
	LeaderID           string              `json:"leader_id,omitempty"`
	RaftAddress        string              `json:"raft_address"`
	BootstrapName      string              `json:"bootstrap_name,omitempty"`
	BootstrapHomeID    string              `json:"bootstrap_home_id,omitempty"`
	BootstrapEndpoints []registry.Endpoint `json:"bootstrap_endpoints,omitempty"`
}

func readState(path, localRaft string) (persisted, bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return persisted{}, false, nil
	}
	if err != nil {
		return persisted{}, false, err
	}
	var s persisted
	if err := json.Unmarshal(data, &s); err != nil {
		return persisted{}, false, err
	}
	if s.Version != stateVersion || len(s.ClusterID) < 3 || len(s.ClusterID) > 128 || s.RaftAddress != localRaft || (s.Mode != "joining" && s.Mode != "bootstrapping" && s.Mode != "active") || strings.ContainsAny(s.ClusterID, " \n\r\t") {
		return persisted{}, false, errors.New("corrupt/incompatible HomeCloud cluster runtime state; recovery required")
	}
	return s, true, nil
}
func saveState(path string, s persisted) error {
	if s.Version != stateVersion || s.ClusterID == "" || s.RaftAddress == "" {
		return errors.New("invalid runtime state")
	}
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".runtime-*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = f.Chmod(0600); err != nil {
		f.Close()
		return err
	}
	if _, err = f.Write(append(b, '\n')); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("persist cluster runtime state: %w", err)
	}
	return nil
}
