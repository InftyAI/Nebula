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

package controller

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// newPlacementReconciler wires a PodPlacementReconciler over a fake client.
func newPlacementReconciler(t *testing.T, objs []client.Object, provs ...*fakeProvider) (*PodPlacementReconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	_ = clientgoscheme.AddToScheme(s)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	r := &PodPlacementReconciler{Client: c, Scheme: s}
	if len(provs) > 0 {
		r.Providers = resolver(provs...)
	}
	return r, c
}

// gatedPod builds an opted-in, gated, unscheduled Pod bound to a pool.
func gatedPod(name, ns, uid, pool, gpu string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(uid),
			Labels: map[string]string{
				nebulav1alpha1.EnabledLabel: "true",
				nebulav1alpha1.PoolLabel:    pool,
			},
		},
		Spec: corev1.PodSpec{
			SchedulingGates: []corev1.PodSchedulingGate{
				{Name: nebulav1alpha1.ProviderSelectionGate},
			},
			Containers: []corev1.Container{{Name: "main", Image: "img"}},
		},
	}
	if gpu != "" {
		// The placement controller matches on the accelerator TYPE only, which
		// rides on the accelerator-type label; the count (nvidia.com/gpu) is a
		// provisioning detail the adapter reads, not needed here.
		pod.Labels[nebulav1alpha1.AcceleratorTypeLabel] = gpu
	}
	return pod
}

func poolWith(name string, capTypes []nebulav1alpha1.CapacityType, providers ...string) *nebulav1alpha1.NodePool {
	var refs []nebulav1alpha1.ProviderRef
	for _, p := range providers {
		refs = append(refs, nebulav1alpha1.ProviderRef{Name: p})
	}
	return &nebulav1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodePoolSpec{
			Providers:     refs,
			CapacityTypes: capTypes,
		},
	}
}

func reconcilePod(t *testing.T, r *PodPlacementReconciler, ns, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: ns, Name: name},
	}); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func getPod(t *testing.T, c client.Client, ns, name string) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	return &pod
}

func TestPlacement_UngatesAndRoutesAndCreatesClaim(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	prov := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	// Gate removed.
	if hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the provider-selection gate to be removed")
	}
	// Routed to the chosen provider.
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "modal" {
		t.Fatalf("expected nodeSelector provider=modal, got %v", got.Spec.NodeSelector)
	}
	// Capacity tier stamped for the VK handler.
	if got.Annotations[nebulav1alpha1.CapacityTypeAnnotation] != "OnDemand" {
		t.Fatalf("expected capacity-type OnDemand, got %q", got.Annotations[nebulav1alpha1.CapacityTypeAnnotation])
	}
	// NodeClaim created, pinned to the Pod, on the chosen provider.
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err != nil {
		t.Fatalf("expected NodeClaim default-p1: %v", err)
	}
	if nc.Spec.Provider != "modal" || nc.Spec.PodRef.UID != "uid-1" || nc.Spec.PoolRef != "pool-a" {
		t.Fatalf("unexpected claim spec: %+v", nc.Spec)
	}
}

func TestPlacement_FirstMatchingProviderWins(t *testing.T) {
	// runpod is listed first but does not offer H100; modal does. First MATCHING
	// provider wins, so modal is chosen.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "runpod", "modal")
	runpod := &fakeProvider{name: "runpod", gpus: []string{"A100"}}
	modal := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, runpod, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "modal" {
		t.Fatalf("expected modal (first matching), got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_OrderedPrefersEarlierProvider(t *testing.T) {
	// Both offer H100; the first in the list wins.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "runpod", "modal")
	runpod := &fakeProvider{name: "runpod", gpus: []string{"H100"}}
	modal := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, runpod, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "runpod" {
		t.Fatalf("expected runpod (first in list), got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_NoMatchingProviderLeavesPodGated(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "runpod")
	runpod := &fakeProvider{name: "runpod", gpus: []string{"A100"}} // no H100
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, runpod)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the Pod to stay gated when no provider matches")
	}
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "" {
		t.Fatal("expected no provider nodeSelector when unplaced")
	}
}

func TestPlacement_CPUOnlyPodMatchesAnyProvider(t *testing.T) {
	// No GPU annotation => any provider matches; even one offering nothing.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	modal := &fakeProvider{name: "modal", gpus: []string{}} // offers no GPUs
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "modal" {
		t.Fatalf("expected a CPU-only Pod to place on modal, got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_SkipsPodWithoutOptInLabel(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pod.Labels[nebulav1alpha1.EnabledLabel] = "false"
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	modal := &fakeProvider{name: "modal"}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected a non-opted-in Pod to be left untouched")
	}
}

func TestPlacement_SkipsAlreadyScheduledPod(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pod.Spec.NodeName = "some-node"
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	modal := &fakeProvider{name: "modal"}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	// No claim should be created for an already-bound Pod.
	var nc nebulav1alpha1.NodeClaim
	err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc)
	if err == nil {
		t.Fatal("expected no claim for an already-scheduled Pod")
	}
}

func TestPlacement_MissingPoolLeavesPodGated(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "ghost-pool", "H100")
	modal := &fakeProvider{name: "modal"}
	r, c := newPlacementReconciler(t, []client.Object{pod}, modal) // no pool seeded

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the Pod to stay gated when its pool is missing")
	}
}

func TestPlacement_StaleClaimForPriorPodBlocksUngate(t *testing.T) {
	// A claim of the Pod's name already exists but pins a PRIOR incarnation
	// (different UID). Placement must NOT ungate against the wrong ledger: it
	// leaves the Pod gated and requeues until the backstop reaps the stale claim.
	pod := gatedPod("p1", "default", "uid-new", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	stale := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "default-p1"},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: "default", Name: "p1", UID: "uid-old"},
			Provider: "modal",
			PoolRef:  "pool-a",
		},
	}
	prov := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool, stale}, prov)

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "p1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("expected a requeue while the stale claim is reaped, got %+v", res)
	}

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the Pod to stay gated against a stale claim")
	}
	// The stale claim must be left untouched (the backstop, not placement, owns it).
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err != nil {
		t.Fatalf("stale claim should still exist: %v", err)
	}
	if nc.Spec.PodRef.UID != "uid-old" {
		t.Fatalf("placement must not overwrite the stale claim, got UID %q", nc.Spec.PodRef.UID)
	}
}

func TestPlacement_AdoptsOwnClaimOnRetry(t *testing.T) {
	// A claim of this name already exists AND pins this Pod's UID: a genuine retry
	// after a crash between create and ungate. Placement adopts it and ungates.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	mine := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "default-p1"},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: "default", Name: "p1", UID: "uid-1"},
			Provider: "modal",
			PoolRef:  "pool-a",
		},
	}
	prov := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool, mine}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if hasGateNamed(got, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the Pod placed when the existing claim is its own")
	}
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "modal" {
		t.Fatalf("expected routing to modal, got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_IdempotentOnRetry(t *testing.T) {
	// A second reconcile after a successful placement must not error (claim
	// AlreadyExists is success) and must leave the Pod placed.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "modal")
	prov := &fakeProvider{name: "modal", gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")
	// Re-gate the in-cluster Pod to simulate a duplicate event racing the claim.
	got := getPod(t, c, "default", "p1")
	got.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: nebulav1alpha1.ProviderSelectionGate}}
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatalf("re-gate: %v", err)
	}
	reconcilePod(t, r, "default", "p1") // must not error on AlreadyExists

	final := getPod(t, c, "default", "p1")
	if hasGateNamed(final, nebulav1alpha1.ProviderSelectionGate) {
		t.Fatal("expected the Pod placed again on retry")
	}
}

// terminalOwnedPod builds an opted-in Pod in a terminal phase. When ownedByRS is
// true it carries a controlling ReplicaSet ownerReference (so a controller would
// recreate it); otherwise it is a bare Pod.
func terminalOwnedPod(name, ns, uid string, phase corev1.PodPhase, ownedByRS bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			UID:       types.UID(uid),
			Labels:    map[string]string{nebulav1alpha1.EnabledLabel: "true"},
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
		Status: corev1.PodStatus{Phase: phase},
	}
	if ownedByRS {
		ctrl := true
		pod.OwnerReferences = []metav1.OwnerReference{{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       "rs-1",
			UID:        types.UID("rs-uid"),
			Controller: &ctrl,
		}}
	}
	return pod
}

func podPresent(c client.Client, ns, name string) bool {
	var pod corev1.Pod
	return c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, &pod) == nil
}

func TestReap_TerminalControllerOwnedPodIsDeleted(t *testing.T) {
	// A Failed, controller-owned Nebula Pod is a tombstone: its owner recreates a
	// replacement beside it and nothing removes the dead one. Placement reaps it.
	pod := terminalOwnedPod("p1", "default", "uid-1", corev1.PodFailed, true)
	r, c := newPlacementReconciler(t, []client.Object{pod})

	reconcilePod(t, r, "default", "p1")

	if podPresent(c, "default", "p1") {
		t.Fatal("expected a terminal controller-owned Pod to be deleted")
	}
}

func TestReap_TerminalBarePodIsKept(t *testing.T) {
	// A bare (un-owned) terminal Pod is left intact as an inspectable record —
	// nothing would recreate it, so deleting it would only lose information.
	pod := terminalOwnedPod("p1", "default", "uid-1", corev1.PodFailed, false)
	r, c := newPlacementReconciler(t, []client.Object{pod})

	reconcilePod(t, r, "default", "p1")

	if !podPresent(c, "default", "p1") {
		t.Fatal("expected a bare terminal Pod to be kept")
	}
}

func TestReap_RunningPodIsNotReaped(t *testing.T) {
	// A live Pod must never be reaped, even when controller-owned.
	pod := terminalOwnedPod("p1", "default", "uid-1", corev1.PodRunning, true)
	r, c := newPlacementReconciler(t, []client.Object{pod})

	reconcilePod(t, r, "default", "p1")

	if !podPresent(c, "default", "p1") {
		t.Fatal("must not reap a Running Pod")
	}
}

func TestReap_NonNebulaTerminalPodIsIgnored(t *testing.T) {
	// A terminal Pod that never opted into Nebula is not ours to reap.
	pod := terminalOwnedPod("p1", "default", "uid-1", corev1.PodFailed, true)
	pod.Labels[nebulav1alpha1.EnabledLabel] = "false"
	r, c := newPlacementReconciler(t, []client.Object{pod})

	reconcilePod(t, r, "default", "p1")

	if !podPresent(c, "default", "p1") {
		t.Fatal("must not reap a non-Nebula Pod")
	}
}
