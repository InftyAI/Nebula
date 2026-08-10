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

// Package sandd mints the Ed25519 JWTs a SandD daemon presents when it dials the
// controller/gateway directly (the dial-out control plane that replaced the
// headscale mesh).
//
// WHY IN-PROCESS, NOT A SEPARATE SERVICE: minting needs the private signing key,
// and the ONLY minter is the manager, on the Provision path. Verifiers (the SandD
// controller and gateway) never mint — they check signatures locally with the
// PUBLIC key alone, no call back here. With a single in-cluster minter doing a
// purely local signature, a standalone keybroker Deployment would add a network
// hop and a point of failure for one caller and no isolation the manager doesn't
// already need; so the manager holds the key and signs directly via this package.
//
// SECURITY POSTURE:
//   - The signing key is the crown jewel; it is mounted into the manager from a
//     Secret and never leaves the process. It is never logged.
//   - The minted token is a secret: returned to the caller but NEVER logged. Only
//     its metadata (sub, aud, tenant, ttl) is safe to log.
//   - A daemon only ever receives its OWN short-lived token, never the key, so a
//     compromised workload cannot forge others; a leak self-heals at exp.
package sandd

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Ed25519 is the signing algorithm: small keys/signatures, fast verification, and
// no RSA-family footguns (no key-size or padding choices to get wrong). Verifiers
// enforce EdDSA so a token cannot be downgraded to an unsigned "alg":"none".
var tokenSigningMethod = jwt.SigningMethodEdDSA

// DaemonClaims is the JWT payload for a daemon token. RegisteredClaims carries the
// standard sub/aud/iss/iat/nbf/exp; Tenant is the one custom claim, used by the
// gateway to reject a token whose tenant may not reach the addressed controller.
type DaemonClaims struct {
	jwt.RegisteredClaims
	// Tenant scopes cross-tenant authorization. It is OPTIONAL: empty when the
	// deployment is single-tenant. When set, a verifier may reject a token whose
	// tenant does not match the controller's tenant (defense beyond aud alone).
	Tenant string `json:"tenant,omitempty"`
}

// Signer mints daemon JWTs. It holds the Ed25519 private key and the key id (kid)
// stamped into every token header so verifiers can select the right public key
// during a rotation (hold both old and new public keys, keyed by kid, until the
// old key's tokens have all expired). issuer is the fixed `iss` claim; ttl bounds
// every token it mints.
type Signer struct {
	key    ed25519.PrivateKey
	kid    string
	issuer string
	ttl    time.Duration
}

// NewSigner loads an Ed25519 private key from a PEM file (PKCS#8) and returns a
// signer. The key is the crown jewel: it is mounted into the manager from a Secret
// readable only by it. A missing/invalid key, empty kid, or non-positive ttl is a
// configuration error, so the caller can fail fast rather than mint bad tokens.
func NewSigner(keyPath, kid, issuer string, ttl time.Duration) (*Signer, error) {
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("reading signing key %q: %w", keyPath, err)
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("signing key %q is not valid PEM", keyPath)
	}
	parsed, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parsing signing key %q (want PKCS#8 Ed25519): %w", keyPath, err)
	}
	key, ok := parsed.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("signing key %q is %T, want ed25519.PrivateKey", keyPath, parsed)
	}
	if kid == "" {
		return nil, fmt.Errorf("signing key id (kid) must not be empty")
	}
	if ttl <= 0 {
		return nil, fmt.Errorf("token ttl must be positive, got %s", ttl)
	}
	return &Signer{key: key, kid: kid, issuer: issuer, ttl: ttl}, nil
}

// KID is the key id stamped in every token header this signer mints. A verifier
// needs it to select the matching public key, so it is handed to the controllers
// alongside PublicKeyPEM.
func (s *Signer) KID() string { return s.kid }

// Issuer is the `iss` claim this signer stamps. A verifier must check it, so that
// a token signed by an unrelated system whose key it happens to trust cannot pass
// as a Nebula daemon token.
func (s *Signer) Issuer() string { return s.issuer }

// PublicKeyPEM returns the PKIX PEM encoding of the signer's PUBLIC half — the
// material every verifier needs and the private half must never accompany.
//
// It is DERIVED from the private key rather than configured separately, which is
// the whole point: a public key handed out through a second channel can silently
// disagree with the key actually signing (a half-finished rotation, a copy-paste
// from the wrong cluster), and the failure mode is every daemon failing to
// authenticate at once. Deriving makes disagreement impossible.
//
// Its one caller hands it straight to the embedded controller in this same process
// (setupSandD), so today it does not travel at all. The result is safe to publish
// regardless — no confidentiality claim rests on it.
func (s *Signer) PublicKeyPEM() (string, error) {
	der, err := x509.MarshalPKIXPublicKey(s.key.Public())
	if err != nil {
		return "", fmt.Errorf("marshalling signing public key: %w", err)
	}
	return string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})), nil
}

// MintDaemonToken mints a daemon token scoped to (daemonID, controllerID). daemonID
// becomes `sub` (the controller enforces sub==registered daemon id, so a leaked
// token cannot impersonate a DIFFERENT daemon); controllerID becomes `aud` (the
// gateway routes by it and the controller admits only its own aud); tenant is the
// optional cross-tenant scope. The token is bounded by the signer's ttl (a leak
// self-heals at exp, and controller teardown makes it inert sooner by removing the
// route). now is passed in so tests are deterministic and there is no hidden clock.
//
// The returned token is a SECRET — callers must never log it.
func (s *Signer) MintDaemonToken(daemonID, controllerID, tenant string, now time.Time) (string, error) {
	if daemonID == "" {
		return "", fmt.Errorf("daemon id (sub) must not be empty")
	}
	if controllerID == "" {
		return "", fmt.Errorf("controller id (aud) must not be empty")
	}
	claims := DaemonClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Subject:   daemonID,
			Audience:  jwt.ClaimStrings{controllerID},
			Issuer:    s.issuer,
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(s.ttl)),
		},
		Tenant: tenant,
	}
	token := jwt.NewWithClaims(tokenSigningMethod, claims)
	// kid rides in the header so a verifier holding multiple public keys can pick
	// the matching one — the hook that makes key rotation possible without a flag day.
	token.Header["kid"] = s.kid
	signed, err := token.SignedString(s.key)
	if err != nil {
		return "", fmt.Errorf("signing daemon token: %w", err)
	}
	return signed, nil
}
