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

// Package util holds small, dependency-free helpers shared across Nebula's
// control plane and provider adapters.
package util

import (
	"crypto/sha256"
	"encoding/hex"
)

const (
	// maxClaimNameLen is the DNS-subdomain cap Kubernetes enforces on a NodeClaim's
	// metadata.name (the claim is a cluster-scoped object). The token must never
	// exceed it, or ensureClaim's create fails and the Pod is never placed.
	maxClaimNameLen = 253
	// claimHashLen is the hex width of the disambiguating suffix appended only when
	// a name is truncated. 10 hex chars = 40 bits, ample against collision among the
	// (few) over-length names in one cluster.
	claimHashLen = 10
)

// ClaimName is the instance-identity token Nebula encodes into a provider instance's
// name/tag so List/Terminate can find it later without a durable id. It is a PURE
// function of the workload's namespace and name, so whoever knows the Pod (the virtual
// kubelet) or the claim's PodRef (the teardown backstop) derives the same token. Keep
// this the single source of truth — both depend on producing identical values.
//
// Normally a plain "namespace-name" join, kept verbatim: it is the historical format
// (already-tagged instances keep matching) and it is readable in kubectl output.
//
// A NodeClaim name is a DNS subdomain, capped at maxClaimNameLen, and a long namespace
// plus long Pod name can exceed it — then the join is un-creatable and the Pod is never
// placed. Only in that case do we truncate and append a hash of the canonical
// "namespace/name" key. "/" is illegal in both, so the hashed input is unambiguous and
// two pairs that truncate to the same prefix still differ.
func ClaimName(namespace, name string) string {
	joined := namespace + "-" + name
	if len(joined) <= maxClaimNameLen {
		return joined
	}

	// Over the cap: truncate the readable join and append a hash of the canonical
	// key so the token stays unique and within the limit.
	sum := sha256.Sum256([]byte(namespace + "/" + name))
	suffix := hex.EncodeToString(sum[:])[:claimHashLen]
	prefix := joined[:maxClaimNameLen-claimHashLen-1] // room for "-<hash>"
	return prefix + "-" + suffix
}
