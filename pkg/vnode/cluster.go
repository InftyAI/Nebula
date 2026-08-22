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

package vnode

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// ClusterReader reads the two cluster-scoped objects every provision is derived from:
//
//	NodePool  - the pool's POLICY (the egress policy, the failover TTL). Admin-owned.
//	NodeClaim - the placement DECISION for this Pod (capacity tier, region), recorded by
//	            the placement controller before it ungated. Controller-owned.
//
// Cluster-scoped is the point, not an incidental: it puts both objects out of reach of the
// namespaced RBAC a workload's owner holds. The Pod says what to run; these say under what
// policy and where. Anything the handler took off the Pod instead would be patchable by
// whoever the policy is meant to constrain — the Pod webhook runs on CREATE only, and
// placement stops watching a Pod once its gate is gone.
//
// Narrower than client.Reader — two methods, two types, no options — because that is the
// whole dependency: a fake in a test is a few lines, and no test needs a scheme.
type ClusterReader interface {
	Pool(ctx context.Context, name string) (*nebulav1alpha1.NodePool, error)
	Claim(ctx context.Context, name string) (*nebulav1alpha1.NodeClaim, error)
}

// cachedClusterReader adapts the manager's client.
type cachedClusterReader struct{ reader client.Reader }

// NewCachedClusterReader wraps a controller-runtime reader as a ClusterReader. Pass the
// manager's client: its cache is already synced before runnables start, and both informers
// are shared with the controllers that watch NodePools and NodeClaims, so the reads on the
// provisioning path cost no API call.
func NewCachedClusterReader(reader client.Reader) ClusterReader {
	return &cachedClusterReader{reader: reader}
}

func (c *cachedClusterReader) Pool(ctx context.Context, name string) (*nebulav1alpha1.NodePool, error) {
	var pool nebulav1alpha1.NodePool
	if err := c.reader.Get(ctx, client.ObjectKey{Name: name}, &pool); err != nil {
		return nil, err
	}
	return &pool, nil
}

func (c *cachedClusterReader) Claim(ctx context.Context, name string) (*nebulav1alpha1.NodeClaim, error) {
	var claim nebulav1alpha1.NodeClaim
	if err := c.reader.Get(ctx, client.ObjectKey{Name: name}, &claim); err != nil {
		return nil, err
	}
	return &claim, nil
}

// poolFor resolves the NodePool a Pod is placed against — the trusted source for the pool's
// own policy: the egress policy applied to the instance, and the TTL of any block a failed
// provision records.
//
// FAIL-CLOSED: no pool means no policy, and provisioning under an unknown policy is the
// failure this path exists to prevent. Callers treat the error as non-terminal, so a pool
// not yet in cache costs a retry rather than an unrestricted instance.
//
// Read ONCE per provision and passed down, so the egress policy applied and the TTL of any
// resulting block come from the same observation. The returned pool is shared informer
// state — read it, never mutate it.
func (h *Handler) poolFor(ctx context.Context, pod *corev1.Pod) (*nebulav1alpha1.NodePool, error) {
	name := pod.Labels[nebulav1alpha1.PoolLabel]
	if name == "" {
		return nil, fmt.Errorf("no %s label on the Pod, so its pool policy cannot be established",
			nebulav1alpha1.PoolLabel)
	}
	if h.cluster == nil {
		// Wiring bug, not a user error: every production handler gets a reader (see
		// NewRunner). Refusing keeps it a loud, immediate failure instead of a fleet that
		// silently provisions unrestricted.
		return nil, fmt.Errorf("no cluster reader configured, cannot establish the policy for pool %q", name)
	}
	pool, err := h.cluster.Pool(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read NodePool %q for its policy: %w", name, err)
	}
	return pool, nil
}

// claimFor resolves the NodeClaim placement recorded for this Pod — the trusted source for
// the DECISION: which capacity tier and which region to provision in.
func (h *Handler) claimFor(ctx context.Context, pod *corev1.Pod, name string) (*nebulav1alpha1.NodeClaim, error) {
	if h.cluster == nil {
		return nil, fmt.Errorf("no cluster reader configured, cannot establish the placement for claim %q", name)
	}
	claim, err := h.cluster.Claim(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read NodeClaim %q for its placement decision: %w", name, err)
	}
	// The claim name is derived from namespace/name, which a recreated Pod reuses, so the
	// UID is what proves this ledger is OURS. Placement already refuses to ungate against a
	// stale claim, so this should never fire — and if it does, the alternative is
	// provisioning in a region chosen for a different Pod.
	if claim.Spec.PodRef.UID != string(pod.UID) {
		return nil, fmt.Errorf("NodeClaim %q records Pod UID %q, not %q; refusing to provision against a stale ledger",
			name, claim.Spec.PodRef.UID, pod.UID)
	}
	return claim, nil
}

// blocklistTTLOf is the base exclusion a failed placement gets, from the pool's own
// FailoverPolicy. A non-positive TTL reads as unset, because zero would install a permanent
// block. Pure, and driven by the pool poolFor already returned, so the failure path never
// re-reads (and never has to decide what an unreadable pool means for a block).
func blocklistTTLOf(pool *nebulav1alpha1.NodePool) time.Duration {
	if pool == nil || pool.Spec.Failover == nil || pool.Spec.Failover.BlocklistTTL.Duration <= 0 {
		return defaultBlocklistTTL
	}
	return pool.Spec.Failover.BlocklistTTL.Duration
}
