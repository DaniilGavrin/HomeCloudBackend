// Package raftadapter connects the privileged KAN-9 controller to KAN-7/8 Raft.
package raftadapter

import (
	"HomeCloudBackend/internal/cluster/consensus"
	"HomeCloudBackend/internal/cluster/membership"
	"HomeCloudBackend/internal/cluster/registry"
	"context"
	"github.com/hashicorp/raft"
)

type Adapter struct{ Service *consensus.Service }

func (a Adapter) NodeID() string { return a.Service.NodeID() }
func (a Adapter) IsLeader() bool { return a.Service.RaftState() == raft.Leader }
func (a Adapter) Members(ctx context.Context) ([]membership.Voter, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	conf, err := a.Service.Configuration()
	if err != nil {
		return nil, err
	}
	out := make([]membership.Voter, 0, len(conf.Servers))
	for _, s := range conf.Servers {
		out = append(out, membership.Voter{NodeID: string(s.ID), RaftAddress: string(s.Address), Voting: s.Suffrage == raft.Voter})
	}
	return out, nil
}
func (a Adapter) Apply(ctx context.Context, c registry.Command) (registry.ApplyResult, error) {
	return a.Service.Apply(ctx, c)
}
func (a Adapter) AddVoter(ctx context.Context, id, address string) error {
	return a.Service.AddVoter(ctx, id, address)
}
func (a Adapter) RemoveVoter(ctx context.Context, id string) error {
	return a.Service.RemoveVoter(ctx, id)
}
