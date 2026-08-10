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

// mint-token mints one SandD daemon JWT from the command line, for verifying by hand
// that a deployed controller ACCEPTS a token its own manager would issue.
//
// WHY IT EXISTS: the verifier's rejection path is easy to test (dial with no token,
// get a 401), but the acceptance path spans two languages and four pieces of
// configuration — the Go minter's key/kid/issuer and the Rust verifier's expectations
// of them. A mismatch in any one (key path pointing at the public half, a kid the
// verifier doesn't hold, iss/aud spelled differently on each side) rejects every
// daemon in the fleet identically, and no unit test on either side can see it because
// each mints with its own fixture. This mints with the REAL key and the REAL settings
// so a successful registration proves the seam.
//
// It deliberately calls pkg/sandd.Signer — the same minter the manager uses on the
// Provision path — rather than assembling a JWT itself. A hand-rolled token would test
// this tool's idea of the format instead of the manager's, which is the one thing the
// exercise must not do.
//
// The token it prints is a BEARER CREDENTIAL. It goes to stdout so the caller can pipe
// it into a daemon's environment; keep it off command lines (/proc/<pid>/cmdline is
// world-readable) and out of shell history. The signing key is only ever read, never
// echoed.
//
// Usage:
//
//	go run ./hack/mint-token -key /tmp/signing.pem -sub daemon-1 [-kid local-dev]
//
// The defaults for -kid/-issuer/-aud/-ttl match cmd/main.go's, so a mint with no flags
// matches a manager running with no SANDD_SIGNING_KID/ISSUER/TTL set. When the target
// deployment DOES set them, pass them — a wrong kid here looks exactly like the
// misconfiguration this tool is meant to detect.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/sandd"
)

func main() {
	keyPath := flag.String("key", "", "path to the PKCS#8 Ed25519 signing key (required)")
	sub := flag.String("sub", "", "daemon id, becomes the token's `sub` and IS the identity the controller registers (required)")
	kid := flag.String("kid", "default", "key id stamped in the JWT header; must match the manager's SANDD_SIGNING_KID")
	issuer := flag.String("issuer", "nebula", "the `iss` claim; must match the manager's SANDD_TOKEN_ISSUER")
	aud := flag.String("aud", nebulav1alpha1.SanddControllerAudience, "the `aud` claim; the embedded controller admits only its own")
	ttl := flag.Duration("ttl", time.Hour, "token lifetime")
	flag.Parse()

	if *keyPath == "" || *sub == "" {
		fmt.Fprintln(os.Stderr, "both -key and -sub are required")
		flag.Usage()
		os.Exit(2)
	}

	signer, err := sandd.NewSigner(*keyPath, *kid, *issuer, *ttl)
	if err != nil {
		// NewSigner's errors name the path but never the key material.
		fmt.Fprintf(os.Stderr, "loading signer: %v\n", err)
		os.Exit(1)
	}

	token, err := signer.MintDaemonToken(*sub, *aud, "", time.Now())
	if err != nil {
		fmt.Fprintf(os.Stderr, "minting: %v\n", err)
		os.Exit(1)
	}

	// Metadata to stderr, token to stdout — so `... 2>/dev/null` or a pipe carries only
	// the secret, and a human running it interactively still sees what they minted.
	fmt.Fprintf(os.Stderr, "minted: sub=%s aud=%s iss=%s kid=%s ttl=%s\n", *sub, *aud, *issuer, *kid, *ttl)
	fmt.Println(token)
}
