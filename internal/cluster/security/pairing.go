package security

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"sync"
	"time"
)

const proofDomain = "HomeCloud/cluster/node-proof/v1\x00" // must match KAN-5 SignChallenge.

type Challenge struct {
	ID        string    `json:"id"`
	Nonce     string    `json:"nonce"`
	ExpiresAt time.Time `json:"expires_at"`
}
type PairingManager struct {
	mu         sync.Mutex
	store      *TrustStore
	challenges map[string]*pending
	now        func() time.Time
}
type pending struct {
	nonce    []byte
	expires  time.Time
	peer     Peer
	verified bool
}

func NewPairingManager(store *TrustStore) *PairingManager {
	return &PairingManager{store: store, challenges: map[string]*pending{}, now: time.Now}
}

// Begin is called only by a privileged LOCAL owner workflow; challenge expires
// after 2 minutes. This does not grant any TLS access.
func (p *PairingManager) Begin() (Challenge, error) {
	nonce := make([]byte, 32)
	id := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return Challenge{}, err
	}
	if _, err := rand.Read(id); err != nil {
		return Challenge{}, err
	}
	t := p.now().Add(2 * time.Minute)
	c := Challenge{ID: base64.RawURLEncoding.EncodeToString(id), Nonce: base64.StdEncoding.EncodeToString(nonce), ExpiresAt: t}
	p.mu.Lock()
	defer p.mu.Unlock()
	for k, v := range p.challenges {
		if !p.now().Before(v.expires) {
			delete(p.challenges, k)
		}
	}
	if len(p.challenges) >= 16 {
		return Challenge{}, errors.New("too many pairing requests")
	}
	p.challenges[c.ID] = &pending{nonce: nonce, expires: t}
	return c, nil
}

// VerifyProof verifies the signed, fresh KAN-5 challenge and holds the candidate
// for explicit owner acceptance; it DOES NOT create trust.
func (p *PairingManager) VerifyProof(id string, peer Peer, signature []byte) error {
	peer, err := normalizePeer(peer)
	if err != nil {
		return err
	}
	pub, err := parsePublicKey(peer.PublicKey)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.challenges[id]
	if !ok {
		return errors.New("unknown or consumed pairing challenge")
	}
	if !p.now().Before(item.expires) {
		delete(p.challenges, id)
		return errors.New("expired pairing challenge")
	}
	if item.verified {
		return errors.New("pairing proof already verified")
	}
	message := append([]byte(proofDomain), item.nonce...)
	if !ed25519.Verify(pub, message, signature) {
		return errors.New("invalid pairing proof signature")
	}
	item.peer = peer
	item.verified = true
	return nil
}

// Approve MUST be called ONLY after the local owner confirms the displayed
// fingerprint and Node details in a privileged UI. There is NO public WS route.
func (p *PairingManager) Approve(id string, ownerConfirmed bool) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	item, ok := p.challenges[id]
	if !ok {
		return errors.New("pairing challenge not found")
	}
	delete(p.challenges, id) // single-use even on rejection
	if !p.now().Before(item.expires) {
		return errors.New("pairing challenge expired")
	}
	if !item.verified {
		return errors.New("no verified proof of key possession")
	}
	if !ownerConfirmed {
		return fmt.Errorf("pairing rejected by owner")
	}
	return p.store.Approve(item.peer)
}
