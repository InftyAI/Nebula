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

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// PoolReader reads the NodePool a Pod is placed against. It exists so the handler can
// resolve pool POLICY from the pool itself, instead of trusting a copy of it carried on
// the Pod (see Handler.egressFor).
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

// egressFor resolves the egress policy to provision under, from the POOL rather than from
// the Pod.
func (h *Handler) egressFor(ctx context.Context, pod *corev1.Pod) (*nebulav1alpha1.EgressPolicy, error) {
	name := pod.Labels[nebulav1alpha1.PoolLabel]
	if name == "" {
		return nil, fmt.Errorf("no %s label on the Pod, so no egress policy can be established",
			nebulav1alpha1.PoolLabel)
	}
	if h.pools == nil {
		// Wiring bug, not a user error: every production handler gets a reader (see
		// NewRunner). Refusing keeps it a loud, immediate failure instead of a fleet that
		// silently provisions unrestricted.
		return nil, fmt.Errorf("no NodePool reader configured, cannot establish the egress policy for pool %q", name)
	}
	pool, err := h.pools.Get(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("read NodePool %q for its egress policy: %w", name, err)
	}
	return pool.Spec.Egress, nil
}
