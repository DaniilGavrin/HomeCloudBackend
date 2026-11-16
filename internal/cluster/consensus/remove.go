package consensus

import (
	"context"
	"errors"
	"fmt"
	"github.com/hashicorp/raft"
	"time"
)

// RemoveVoter is an INTERNAL, owner-authorized membership operation.
// KAN-9 Controller first commits the registry tombstone. Never expose this
// method directly via unauthenticated WS or call it for the current leader.
func (s *Service) RemoveVoter(ctx context.Context, id string) error {
	if s.raft.State() != raft.Leader {
		return ErrNotLeader
	}
	if id == s.nodeID {
		return errors.New("cannot remove current leader; transfer leadership first")
	}
	conf, err := s.Configuration()
	if err != nil {
		return err
	}
	found := false
	voters := 0
	for _, m := range conf.Servers {
		if m.Suffrage == raft.Voter {
			voters++
		}
		if string(m.ID) == id {
			found = true
		}
	}
	if !found {
		return nil
	} // idempotent after a committed config change
	if voters <= 1 {
		return errors.New("refuse removing the last raft voter")
	}
	timeout := 8 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return ctx.Err()
		}
	}
	future := s.raft.RemoveServer(raft.ServerID(id), 0, timeout)
	done := make(chan error, 1)
	go func() { done <- future.Error() }()
	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("raft remove voter %s: %w", id, err)
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
