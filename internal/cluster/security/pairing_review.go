package security

import "errors"

// VerifiedPeer returns the exact candidate whose proof was verified for a
// single-use challenge. It never approves trust. Owner permission checks MUST
// be performed against this returned NodeID, not a caller-provided node ID.
func (p *PairingManager) VerifiedPeer(challengeID string) (Peer, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.challenges[challengeID]
	if !ok || !p.now().Before(item.expires) || !item.verified {
		return Peer{}, errors.New("pairing challenge not verified, expired or consumed")
	}
	peer := item.peer
	peer.Addresses = append([]string(nil), peer.Addresses...)
	return peer, nil
}
