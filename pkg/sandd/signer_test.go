/*
Copyright 2026 The InftyAI Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package sandd

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// writeTestKey generates an Ed25519 key, writes the private half as PKCS#8 PEM to a
// temp file (as the manager's Secret mount would look), and returns the path plus
// the public half a verifier (controller/gateway) would hold.
func writeTestKey(t *testing.T) (keyPath string, pub ed25519.PublicKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generating key: %v", err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshaling key: %v", err)
	}
	keyPath = filepath.Join(t.TempDir(), "signing.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	if err := os.WriteFile(keyPath, pemBytes, 0o600); err != nil {
		t.Fatalf("writing key: %v", err)
	}
	return keyPath, pub
}

func TestSigner_SignAndVerify(t *testing.T) {
	keyPath, pub := writeTestKey(t)
	signer, err := NewSigner(keyPath, "kid-1", "nebula-manager", 24*time.Hour)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tokenStr, err := signer.MintDaemonToken("daemon-abc", "controller-xyz", "tenant-a", now)
	if err != nil {
		t.Fatalf("MintDaemonToken: %v", err)
	}

	// Verify exactly as a controller/gateway would: public key only, EdDSA enforced.
	var claims DaemonClaims
	parsed, err := jwt.ParseWithClaims(tokenStr, &claims, func(tok *jwt.Token) (interface{}, error) {
		if tok.Method != jwt.SigningMethodEdDSA {
			t.Fatalf("unexpected signing method %v", tok.Header["alg"])
		}
		if kid := tok.Header["kid"]; kid != "kid-1" {
			t.Fatalf("kid = %v, want kid-1", kid)
		}
		return pub, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithTimeFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !parsed.Valid {
		t.Fatalf("token not valid")
	}
	if claims.Subject != "daemon-abc" {
		t.Errorf("sub = %q, want daemon-abc", claims.Subject)
	}
	if len(claims.Audience) != 1 || claims.Audience[0] != "controller-xyz" {
		t.Errorf("aud = %v, want [controller-xyz]", claims.Audience)
	}
	if claims.Issuer != "nebula-manager" {
		t.Errorf("iss = %q, want nebula-manager", claims.Issuer)
	}
	if claims.Tenant != "tenant-a" {
		t.Errorf("tenant = %q, want tenant-a", claims.Tenant)
	}
	if got := claims.ExpiresAt.Time; !got.Equal(now.Add(24 * time.Hour)) {
		t.Errorf("exp = %v, want %v", got, now.Add(24*time.Hour))
	}
}

func TestSigner_WrongKeyRejected(t *testing.T) {
	keyPath, _ := writeTestKey(t)
	signer, err := NewSigner(keyPath, "kid-1", "nebula-manager", time.Hour)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tokenStr, err := signer.MintDaemonToken("daemon-abc", "controller-xyz", "", now)
	if err != nil {
		t.Fatalf("MintDaemonToken: %v", err)
	}

	// A DIFFERENT public key must fail verification — proves the signature binds to
	// the manager's private key, not just any Ed25519 key.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	_, err = jwt.ParseWithClaims(tokenStr, &DaemonClaims{}, func(*jwt.Token) (interface{}, error) {
		return otherPub, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithTimeFunc(func() time.Time { return now }))
	if err == nil {
		t.Fatalf("verify with wrong key succeeded, want failure")
	}
}

func TestSigner_ExpiredRejected(t *testing.T) {
	keyPath, pub := writeTestKey(t)
	signer, _ := NewSigner(keyPath, "kid-1", "nebula-manager", time.Hour)
	issued := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	tokenStr, err := signer.MintDaemonToken("daemon-abc", "controller-xyz", "", issued)
	if err != nil {
		t.Fatalf("MintDaemonToken: %v", err)
	}

	// Verify two hours later: past the 1h TTL, so exp must fail.
	later := issued.Add(2 * time.Hour)
	_, err = jwt.ParseWithClaims(tokenStr, &DaemonClaims{}, func(*jwt.Token) (interface{}, error) {
		return pub, nil
	}, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithTimeFunc(func() time.Time { return later }))
	if err == nil {
		t.Fatalf("verify of expired token succeeded, want failure")
	}
}

func TestSigner_EmptyIDsRejected(t *testing.T) {
	keyPath, _ := writeTestKey(t)
	signer, _ := NewSigner(keyPath, "kid-1", "nebula-manager", time.Hour)
	now := time.Date(2026, 8, 5, 12, 0, 0, 0, time.UTC)
	if _, err := signer.MintDaemonToken("", "controller-xyz", "", now); err == nil {
		t.Errorf("empty daemon id (sub) accepted, want error")
	}
	if _, err := signer.MintDaemonToken("daemon-abc", "", "", now); err == nil {
		t.Errorf("empty controller id (aud) accepted, want error")
	}
}

func TestNewSigner_BadConfig(t *testing.T) {
	dir := t.TempDir()
	notPEM := filepath.Join(dir, "bad.pem")
	if err := os.WriteFile(notPEM, []byte("not a pem file"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewSigner(notPEM, "kid-1", "iss", time.Hour); err == nil {
		t.Errorf("NewSigner accepted non-PEM file, want error")
	}
	if _, err := NewSigner(filepath.Join(dir, "missing.pem"), "kid-1", "iss", time.Hour); err == nil {
		t.Errorf("NewSigner accepted missing file, want error")
	}

	// A valid key but bad kid / ttl is still a config error.
	goodKey, _ := writeTestKey(t)
	if _, err := NewSigner(goodKey, "", "iss", time.Hour); err == nil {
		t.Errorf("NewSigner accepted empty kid, want error")
	}
	if _, err := NewSigner(goodKey, "kid-1", "iss", 0); err == nil {
		t.Errorf("NewSigner accepted zero ttl, want error")
	}
}

// TestSigner_PublicKeyPEMVerifiesOwnTokens is the property the whole handout rests
// on: the PEM the signer exports must verify the tokens that same signer mints. It
// is derived from the private key rather than configured, so the two cannot drift —
// a verifier holding a stale public key rejects every daemon in the fleet at once.
func TestSigner_PublicKeyPEMVerifiesOwnTokens(t *testing.T) {
	keyPath, _ := writeTestKey(t)
	signer, err := NewSigner(keyPath, "kid-1", "nebula", time.Hour)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}

	pubPEM, err := signer.PublicKeyPEM()
	if err != nil {
		t.Fatalf("PublicKeyPEM: %v", err)
	}
	// The private half must NEVER ride along: this value is handed to a Deployment in
	// the user's own namespace, so a PRIVATE block here would leak the signing key to
	// the tenant.
	if strings.Contains(pubPEM, "PRIVATE") {
		t.Fatal("PublicKeyPEM emitted private key material")
	}
	if !strings.HasPrefix(pubPEM, "-----BEGIN PUBLIC KEY-----") {
		t.Errorf("expected a PKIX PUBLIC KEY block, got %q", pubPEM)
	}

	// Parse it back the way a verifier would, and check a freshly minted token.
	block, _ := pem.Decode([]byte(pubPEM))
	if block == nil {
		t.Fatal("exported PEM does not decode")
	}
	anyPub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		t.Fatalf("exported PEM is not a PKIX public key: %v", err)
	}
	pub, ok := anyPub.(ed25519.PublicKey)
	if !ok {
		t.Fatalf("exported key is %T, want ed25519.PublicKey", anyPub)
	}

	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	tokenStr, err := signer.MintDaemonToken("daemon-abc", "sandd-xyz", "", now)
	if err != nil {
		t.Fatalf("MintDaemonToken: %v", err)
	}
	if _, err := jwt.ParseWithClaims(tokenStr, &DaemonClaims{},
		func(*jwt.Token) (interface{}, error) { return pub, nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithAudience("sandd-xyz"),
		jwt.WithIssuer("nebula"),
		jwt.WithTimeFunc(func() time.Time { return now })); err != nil {
		t.Fatalf("token minted by this signer must verify with its exported public key: %v", err)
	}
}

// TestSigner_ExportedMetadataMatchesTokens: the kid and issuer handed to verifiers
// must be the ones actually stamped into tokens. A verifier selects its key by kid
// and rejects on iss, so a mismatch here is a fleet-wide auth outage.
func TestSigner_ExportedMetadataMatchesTokens(t *testing.T) {
	keyPath, pub := writeTestKey(t)
	signer, err := NewSigner(keyPath, "kid-rotated-2", "nebula-prod", time.Hour)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	if signer.KID() != "kid-rotated-2" {
		t.Errorf("KID() = %q", signer.KID())
	}
	if signer.Issuer() != "nebula-prod" {
		t.Errorf("Issuer() = %q", signer.Issuer())
	}

	now := time.Date(2026, 8, 8, 12, 0, 0, 0, time.UTC)
	tokenStr, err := signer.MintDaemonToken("d", "c", "", now)
	if err != nil {
		t.Fatalf("mint: %v", err)
	}
	var claims DaemonClaims
	parsed, err := jwt.ParseWithClaims(tokenStr, &claims,
		func(*jwt.Token) (interface{}, error) { return pub, nil },
		jwt.WithValidMethods([]string{"EdDSA"}),
		jwt.WithTimeFunc(func() time.Time { return now }))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	// The advertised kid must equal the header kid a verifier keys on.
	if got := parsed.Header["kid"]; got != signer.KID() {
		t.Errorf("token kid %v != advertised KID() %q", got, signer.KID())
	}
	if claims.Issuer != signer.Issuer() {
		t.Errorf("token iss %q != advertised Issuer() %q", claims.Issuer, signer.Issuer())
	}
}
