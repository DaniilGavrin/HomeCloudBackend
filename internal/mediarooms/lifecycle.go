package mediarooms

// stoppedHistoryLimit bounds the closed-room replay window. Identities never
// repeat, so forgetting an ancient STOP cannot resurrect a room.
const stoppedHistoryLimit = 256

// ClosedRoom lets the WebSocket layer authorize repeated TV STOP requests
// against the original target, without putting closed rooms back in snapshots.
func (m *Manager) ClosedRoom(id string) (Snapshot, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	room, ok := m.stopped[id]
	return room, ok
}
