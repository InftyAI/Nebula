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

package util

import (
	"strings"
	"testing"
)

func TestClaimName_ShortStaysReadableAndCompatible(t *testing.T) {
	// The common case must remain the historical "namespace-name" join, verbatim:
	// instances already tagged this way keep matching the teardown backstop, and it
	// stays readable. Any change here silently orphans live instances.
	if got := ClaimName("default", "p1"); got != "default-p1" {
		t.Fatalf("ClaimName(default, p1) = %q, want default-p1", got)
	}
}

func TestClaimName_Deterministic(t *testing.T) {
	// The whole scheme rests on every component deriving the SAME token from the
	// same (namespace, name) — the vnode handler tags with it, the backstop matches
	// on it. It must be a pure function.
	for _, tc := range []struct{ ns, name string }{
		{"default", "p1"},
		{strings.Repeat("n", 120), strings.Repeat("p", 200)}, // over-length branch
	} {
		want := ClaimName(tc.ns, tc.name)
		for i := 0; i < 3; i++ {
			if got := ClaimName(tc.ns, tc.name); got != want {
				t.Fatalf("ClaimName(%q,%q) is not deterministic: %q != %q", tc.ns, tc.name, got, want)
			}
		}
	}
}

func TestClaimName_OverLengthIsBoundedAndUnique(t *testing.T) {
	// A long namespace + long Pod name would exceed the k8s object-name cap; the
	// token must be truncated to fit (else the NodeClaim create fails and the Pod is
	// never placed) while staying distinct across different inputs that share a
	// truncated prefix.
	ns := strings.Repeat("n", 200)
	a := ClaimName(ns, strings.Repeat("a", 200))
	b := ClaimName(ns, strings.Repeat("a", 199)+"b")

	if len(a) > maxClaimNameLen {
		t.Fatalf("over-length claim name not bounded: len=%d > %d", len(a), maxClaimNameLen)
	}
	if a == b {
		t.Fatal("distinct over-length inputs collided; hash suffix must disambiguate")
	}
	if !strings.HasPrefix(a, ns+"-") {
		t.Fatalf("expected readable prefix retained, got %q", a)
	}
}

func TestClaimName_BoundaryExactlyAtCapIsNotHashed(t *testing.T) {
	// A join landing exactly on the cap must stay the plain readable form (the hash
	// fallback triggers only strictly above the cap).
	name := strings.Repeat("p", maxClaimNameLen-len("ns-"))
	got := ClaimName("ns", name)
	if len(got) != maxClaimNameLen {
		t.Fatalf("expected exact-cap join of len %d, got %d", maxClaimNameLen, len(got))
	}
	if got != "ns-"+name {
		t.Fatal("a join exactly at the cap must not be hashed")
	}
}
