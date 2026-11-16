package identity

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// RaftTLSCertificate creates a fresh, short-lived self-signed certificate
// using the EXISTING KAN-5 Ed25519 identity. The private key stays local.
// Peer verification MUST PIN the public key; the self-signed cert alone
// cannot authenticate membership.
func (s *Store) RaftTLSCertificate() (tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id, err := load(filepath.Join(s.dir, privateKeyFile), filepath.Join(s.dir, identityFile))
	if err != nil {
		return tls.Certificate{}, err
	}
	seed, err := os.ReadFile(filepath.Join(s.dir, privateKeyFile))
	if err != nil {
		return tls.Certificate{}, err
	}
	if len(seed) != ed25519.SeedSize {
		return tls.Certificate{}, errors.New("invalid Ed25519 seed")
	}
	key := ed25519.NewKeyFromSeed(seed)
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, err
	}
	now := time.Now().UTC()
	t := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: id.NodeID},
		NotBefore: now.Add(-5 * time.Minute), NotAfter: now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true, IsCA: false,
	}
	der, err := x509.CreateCertificate(rand.Reader, t, t, key.Public(), key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: t}, nil
}
