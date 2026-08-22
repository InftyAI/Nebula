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

// PoolReader reads the NodePool a Pod is placed against. It exists so the handler can
// resolve pool POLICY from the pool itself, instead of trusting a copy of it carried on
// the Pod (see Handler.poolFor).
//
// Narrower than client.Reader — one method, one type, no options — because that is the
// whole dependency: a fake in a test is three lines, and no test needs a scheme.
type PoolReader interface {
	Get(ctx context.Context, name string) (*nebulav1alpha1.NodePool, error)
}

// cachedPoolReader adapts the manager's client. NodePool is cluster-scoped, so the name
// is the whole key.
type cachedPoolReader struct{ reader client.Reader }

// NewCachedPoolReader wraps a controller-runtime reader as a PoolReader. Pass the
// manager's client: its cache is already synced before runnables start, and the pool
// informer is shared with the controllers that watch NodePools, so the read on the
// provisioning path costs no API call.
func NewCachedPoolReader(reader client.Reader) PoolReader {
	return &cachedPoolReader{reader: reader}
}

func (c *cachedPoolReader) Get(ctx context.Context, name string) (*nebulav1alpha1.NodePool, error) {
	var pool nebulav1alpha1.NodePool
	if err := c.reader.Get(ctx, client.ObjectKey{Name: name}, &pool); err != nil {
		return nil, err
	}
	return &pool, nil
}

// poolFor resolves the NodePool a Pod is placed against — the trusted source for every
// policy the handler needs to provision (the egress policy, the failover TTL), none of which
// may come off the Pod, where the workload's own owner can patch it.
//
// FAIL-CLOSED: no pool means no policy, and provisioning under an unknown policy is the
// failure this path exists to prevent. Callers treat the error as non-terminal, so a pool
// not yet in cache costs a retry rather than an unrestricted instance.
//
// Read ONCE per provision and passed down, so the egress policy applied and the TTL of any
// resulting block come from the same observation of the pool. The returned pool is shared
// informer state — read it, never mutate it.
func (h *Handler) poolFor(ctx context.Context, pod *corev1.Pod) (*nebulav1alpha1.NodePool, error) {
	name := pod.Labels[nebulav1alpha1.PoolLabel]
	if name == "" {
		return nil, fmt.Errorf("no %s label on the Pod, so its pool policy cannot be established",
			nebulav1alpha1.PoolLabel)
	}
	if h.pools == nil {
		// Wiring bug, not a user error: every production handler gets a reader (see
		// NewRunner). Refusing keeps it a loud, immediate failure instead of a fleet that
		// silently provisions unrestricted.
		return nil, fmt.Errorf("no NodePool reader configured, cannot establish the policy for pool %q", name)
	}
	pool, err := h.pools.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read NodePool %q for its policy: %w", name, err)
	}
	return pool, nil
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
