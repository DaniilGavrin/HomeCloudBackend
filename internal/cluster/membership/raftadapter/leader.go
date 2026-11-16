package raftadapter

// LeaderAddress lets the authenticated membership-control gate correlate the
// Raft leader's transport endpoint with the pinned, TLS-authenticated Node ID.
// This is a read-only view; it grants no ability to mutate Raft membership.
func (a Adapter) LeaderAddress() string { return a.Service.LeaderAddress() }
