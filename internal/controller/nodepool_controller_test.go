/*
Copyright 2026.

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

package controller

import (
	"context"
	"testing"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// newPoolReconciler wires a NodePoolReconciler over a fake client seeded with
// objs. known is the set of provider names treated as registered.
func newPoolReconciler(t *testing.T, known []string, objs ...client.Object) (*NodePoolReconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&nebulav1alpha1.NodePool{}).
		Build()
	knownSet := map[string]bool{}
	for _, n := range known {
		knownSet[n] = true
	}
	r := &NodePoolReconciler{
		Client: c,
		Scheme: s,
		Providers: func(name string) (provider.Provider, bool) {
			if knownSet[name] {
				return &fakeProvider{name: name}, true
			}
			return nil, false
		},
	}
	return r, c
}

func newPool(name string, strategy nebulav1alpha1.PlacementStrategy, providers ...nebulav1alpha1.ProviderSpec) *nebulav1alpha1.NodePool {
	return &nebulav1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodePoolSpec{
			Providers: providers,
			Strategy:  strategy,
		},
	}
}

// runningClaim builds a NodeClaim in a given phase for pool/provider accounting.
func poolClaim(name, pool, prov string, phase nebulav1alpha1.NodeClaimPhase) *nebulav1alpha1.NodeClaim {
	nc := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: "default", Name: name + "-pod", UID: "u-" + name},
			Provider: prov,
			PoolRef:  pool,
		},
	}
	nc.Status.Phase = phase
	return nc
}

func reconcilePool(t *testing.T, r *NodePoolReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: name},
	}); err != nil {
		t.Fatalf("reconcile pool: %v", err)
	}
}

func getPool(t *testing.T, c client.Client, name string) *nebulav1alpha1.NodePool {
	t.Helper()
	var p nebulav1alpha1.NodePool
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &p); err != nil {
		t.Fatalf("get pool %s: %v", name, err)
	}
	return &p
}

func poolReadyCond(p *nebulav1alpha1.NodePool) *metav1.Condition {
	return apimeta.FindStatusCondition(p.Status.Conditions, nebulav1alpha1.NodePoolConditionReady)
}

func TestPool_ValidPoolBecomesReady(t *testing.T) {
	pool := newPool("gpu", nebulav1alpha1.StrategyOrdered,
		nebulav1alpha1.ProviderSpec{Name: "modal"})
	r, c := newPoolReconciler(t, []string{"modal"}, pool)

	reconcilePool(t, r, "gpu")

	cond := poolReadyCond(getPool(t, c, "gpu"))
	if cond == nil || cond.Status != metav1.ConditionTrue {
		t.Fatalf("expected Ready=True, got %+v", cond)
	}
	if cond.Reason != nebulav1alpha1.ReasonPoolValid {
		t.Fatalf("reason = %q, want %q", cond.Reason, nebulav1alpha1.ReasonPoolValid)
	}
}

func TestPool_UnknownProviderNotReady(t *testing.T) {
	pool := newPool("gpu", nebulav1alpha1.StrategyOrdered,
		nebulav1alpha1.ProviderSpec{Name: "modal"},
		nebulav1alpha1.ProviderSpec{Name: "ghost"})
	r, c := newPoolReconciler(t, []string{"modal"}, pool) // ghost not registered

	reconcilePool(t, r, "gpu")

	cond := poolReadyCond(getPool(t, c, "gpu"))
	if cond == nil || cond.Status != metav1.ConditionFalse {
		t.Fatalf("expected Ready=False, got %+v", cond)
	}
	if cond.Reason != nebulav1alpha1.ReasonUnknownProvider {
		t.Fatalf("reason = %q, want %q", cond.Reason, nebulav1alpha1.ReasonUnknownProvider)
	}
}

func TestPool_PlacedCountsBoundClaims(t *testing.T) {
	pool := newPool("gpu", nebulav1alpha1.StrategyOrdered,
		nebulav1alpha1.ProviderSpec{Name: "modal"},
		nebulav1alpha1.ProviderSpec{Name: "runpod"})
	// 2 bound on modal, 1 on runpod for this pool; 1 bound for another pool
	// (ignored); 1 pending on modal for this pool (ignored — not Bound).
	r, c := newPoolReconciler(t, []string{"modal", "runpod"},
		pool,
		poolClaim("a", "gpu", "modal", nebulav1alpha1.NodeClaimBound),
		poolClaim("b", "gpu", "modal", nebulav1alpha1.NodeClaimBound),
		poolClaim("c", "gpu", "runpod", nebulav1alpha1.NodeClaimBound),
		poolClaim("x", "otherpool", "modal", nebulav1alpha1.NodeClaimBound),
		poolClaim("pend", "gpu", "modal", nebulav1alpha1.NodeClaimProvisioning),
	)

	reconcilePool(t, r, "gpu")

	got := getPool(t, c, "gpu").Status.Placed
	if got["modal"] != 2 {
		t.Fatalf("modal placed = %d, want 2", got["modal"])
	}
	if got["runpod"] != 1 {
		t.Fatalf("runpod placed = %d, want 1", got["runpod"])
	}
	if _, ok := got["otherpool"]; ok {
		t.Fatal("must not count claims from another pool")
	}
}

func TestPool_PlacedNilWhenNothingRunning(t *testing.T) {
	pool := newPool("gpu", nebulav1alpha1.StrategyOrdered,
		nebulav1alpha1.ProviderSpec{Name: "modal"})
	r, c := newPoolReconciler(t, []string{"modal"},
		pool,
		poolClaim("pend", "gpu", "modal", nebulav1alpha1.NodeClaimProvisioning),
	)

	reconcilePool(t, r, "gpu")

	if got := getPool(t, c, "gpu").Status.Placed; got != nil {
		t.Fatalf("expected nil Placed when nothing running, got %+v", got)
	}
}

func TestClaimToPool(t *testing.T) {
	nc := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "c1"},
		Spec:       nebulav1alpha1.NodeClaimSpec{PoolRef: "gpu"},
	}
	reqs := claimToPool(context.Background(), nc)
	if len(reqs) != 1 || reqs[0].Name != "gpu" {
		t.Fatalf("claimToPool = %+v, want one request for 'gpu'", reqs)
	}

	// A claim with no pool ref enqueues nothing.
	orphan := &nebulav1alpha1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: "c2"}}
	if reqs := claimToPool(context.Background(), orphan); reqs != nil {
		t.Fatalf("expected nil for pool-less claim, got %+v", reqs)
	}
}
