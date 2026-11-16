package identity

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"path/filepath"
	"testing"
)

func TestRaftCertificateUsesExistingPermanentIdentity(t *testing.T) {
	store := NewStore(filepath.Join(t.TempDir(), "cluster"))
	id, err := store.LoadOrCreate()
	if err != nil {
		t.Fatal(err)
	}
	cert, err := store.RaftTLSCertificate()
	if err != nil {
		t.Fatal(err)
	}
	leaf, err := x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	pub := leaf.PublicKey.(ed25519.PublicKey)
	expected, _ := base64.StdEncoding.DecodeString(id.PublicKey)
	if !ed25519.PublicKey(pub).Equal(ed25519.PublicKey(expected)) || leaf.Subject.CommonName != id.NodeID {
		t.Fatal("certificate identity mismatch")
	}
	if _, err := store.RaftTLSCertificate(); err != nil {
		t.Fatal(err)
	}
	after, err := store.LoadOrCreate()
	if err != nil || after.NodeID != id.NodeID {
		t.Fatal("certificate issuance changed node identity")
	}
}
