package consensus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"

	"HomeCloudBackend/internal/cluster/registry"
	"github.com/hashicorp/raft"
)

const maxSnapshotBytes int64 = 128 << 20

type applyResponse struct {
	Result registry.ApplyResult
	Err    error
}

// FSM is the sole production writer of registry.Store. Its mutex prevents
// snapshot capture from interleaving with a committed log application or
// snapshot restore. UI and HTTP MUST NOT call registry.ApplyCommitted directly.
type FSM struct {
	store *registry.Store
	mu    sync.Mutex
}

func NewFSM(store *registry.Store) *FSM { return &FSM{store: store} }

func (f *FSM) Apply(log *raft.Log) interface{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	if log.Type != raft.LogCommand {
		return applyResponse{Err: errors.New("unexpected raft log type")}
	}
	var cmd registry.Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return applyResponse{Err: fmt.Errorf("decode raft command: %w", err)}
	}
	res, err := f.store.ApplyCommitted(context.Background(), cmd)
	return applyResponse{Result: res, Err: err}
}

func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, err := f.store.ReadDurableState(context.Background())
	if err != nil {
		return nil, err
	}
	data, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	if len(data) > int(maxSnapshotBytes) {
		return nil, errors.New("registry snapshot exceeds 128 MiB; implement snapshot pagination before growing cluster")
	}
	return &fsmSnapshot{data: data}, nil
}

func (f *FSM) Restore(reader io.ReadCloser) error {
	defer reader.Close()
	f.mu.Lock()
	defer f.mu.Unlock()
	data, err := io.ReadAll(io.LimitReader(reader, maxSnapshotBytes+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > maxSnapshotBytes {
		return errors.New("raft snapshot too large")
	}
	var state registry.DurableState
	if err := json.Unmarshal(data, &state); err != nil {
		return fmt.Errorf("decode raft snapshot: %w", err)
	}
	return f.store.RestoreDurableState(context.Background(), state)
}

type fsmSnapshot struct{ data []byte }

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return err
	}
	if err := sink.Close(); err != nil {
		_ = sink.Cancel()
		return err
	}
	return nil
}
func (*fsmSnapshot) Release() {}
