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
	"slices"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	awsprovider "github.com/InftyAI/Nebula/pkg/provider/aws"
	"github.com/InftyAI/Nebula/pkg/util"
)

// fakeBlocklist is a test Blocklister that reports a fixed set of candidates as
// blocked. A candidate is blocked when it matches an entry on every NON-empty
// field (empty fields are wildcards), mirroring the real blocklist's coverage.
// A blocked candidate reports retryAfter=until (defaulting to a nonzero duration
// so the placement controller's requeue-on-block path is exercised).
type fakeBlocklist struct {
	blocked []failover.Candidate
	until   time.Duration
}

func (b *fakeBlocklist) BlockedUntil(c failover.Candidate) (time.Duration, bool) {
	for _, e := range b.blocked {
		if e.Provider != "" && e.Provider != c.Provider {
			continue
		}
		if e.Accelerator != "" && e.Accelerator != c.Accelerator {
			continue
		}
		if e.CapacityType != "" && e.CapacityType != c.CapacityType {
			continue
		}
		if e.Region != "" && e.Region != c.Region {
			continue
		}
		until := b.until
		if until == 0 {
			until = time.Minute
		}
		return until, true
	}
	return 0, false
}

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
	refs := make([]nebulav1alpha1.ProviderSpec, 0, len(providers))
	for _, p := range providers {
		refs = append(refs, nebulav1alpha1.ProviderSpec{Name: p})
	}
	return &nebulav1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodePoolSpec{
			Providers:     refs,
			CapacityTypes: capTypes,
		},
	}
}

// poolWithRegions builds a single-provider pool whose provider ref lists the given
// regions, with the given capacity-type fallback order.
func poolWithRegions(name string, capTypes []nebulav1alpha1.CapacityType, providerName string, regions ...string) *nebulav1alpha1.NodePool {
	return &nebulav1alpha1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodePoolSpec{
			Providers:     []nebulav1alpha1.ProviderSpec{{Name: providerName, Regions: regions}},
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
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	// Gate removed.
	if hasGateNamed(got) {
		t.Fatal("expected the provider-selection gate to be removed")
	}
	// Routed to the chosen provider.
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != provider.ProviderModal {
		t.Fatalf("expected nodeSelector provider=modal, got %v", got.Spec.NodeSelector)
	}
	// NodeClaim created, pinned to the Pod, on the chosen provider.
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err != nil {
		t.Fatalf("expected NodeClaim default-p1: %v", err)
	}
	if nc.Spec.Provider != provider.ProviderModal || nc.Spec.PodRef.UID != "uid-1" || nc.Spec.PoolRef != "pool-a" {
		t.Fatalf("unexpected claim spec: %+v", nc.Spec)
	}
	// Capacity tier recorded on the CLAIM, which is what the VK handler reads on
	// CreatePod. Never on the Pod, where it would be patchable after the gate is gone.
	if nc.Spec.CapacityType != nebulav1alpha1.CapacityOnDemand {
		t.Fatalf("expected claim capacityType OnDemand, got %q", nc.Spec.CapacityType)
	}
	// The request's POOL identity (type:count) is recorded for reporting: H100 with
	// no explicit count defaults to 1, so the pool is "H100:1".
	if nc.Spec.Accelerator != "H100:1" {
		t.Fatalf("expected claim to record accelerator H100:1, got %q", nc.Spec.Accelerator)
	}
}

func TestPlacement_FirstMatchingProviderWins(t *testing.T) {
	// runpod is listed first but does not offer H100; modal does. First MATCHING
	// provider wins, so modal is chosen.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "runpod", provider.ProviderModal)
	runpod := &fakeProvider{name: "runpod", gpus: []string{"A100"}}
	modal := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, runpod, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != provider.ProviderModal {
		t.Fatalf("expected modal (first matching), got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_OrderedPrefersEarlierProvider(t *testing.T) {
	// Both offer H100; the first in the list wins.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "runpod", provider.ProviderModal)
	runpod := &fakeProvider{name: "runpod", gpus: []string{"H100"}}
	modal := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
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
	if !hasGateNamed(got) {
		t.Fatal("expected the Pod to stay gated when no provider matches")
	}
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "" {
		t.Fatal("expected no provider nodeSelector when unplaced")
	}
}

func TestPlacement_CPUOnlyPodMatchesAnyProvider(t *testing.T) {
	// No GPU annotation => any provider matches; even one offering nothing.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	modal := &fakeProvider{name: provider.ProviderModal, gpus: []string{}} // offers no GPUs
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != provider.ProviderModal {
		t.Fatalf("expected a CPU-only Pod to place on modal, got %v", got.Spec.NodeSelector)
	}
	// A CPU-only claim requests no accelerator, so the pool identity is left empty.
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err != nil {
		t.Fatalf("expected NodeClaim default-p1: %v", err)
	}
	if nc.Spec.Accelerator != "" {
		t.Fatalf("expected empty accelerator for a CPU-only claim, got %q", nc.Spec.Accelerator)
	}
}

func TestPlacement_GPUCountWithoutAcceleratorTypeLeavesPodGated(t *testing.T) {
	// A Pod that requests nvidia.com/gpu but omits accelerator-type is malformed,
	// not CPU-only. Placement must not silently route it as a CPU-only workload.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "")
	pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
		util.NvidiaGPUResource: resource.MustParse("1"),
	}
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	modal := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected malformed GPU Pod to stay gated")
	}
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "" {
		t.Fatalf("expected no provider nodeSelector for malformed GPU Pod, got %v", got.Spec.NodeSelector)
	}
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err == nil {
		t.Fatal("expected no claim for malformed GPU Pod")
	}
}

func TestPlacement_SkipsPodWithoutOptInLabel(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pod.Labels[nebulav1alpha1.EnabledLabel] = "false"
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	modal := &fakeProvider{name: provider.ProviderModal}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, modal)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected a non-opted-in Pod to be left untouched")
	}
}

func TestPlacement_SkipsAlreadyScheduledPod(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pod.Spec.NodeName = "some-node"
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	modal := &fakeProvider{name: provider.ProviderModal}
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
	modal := &fakeProvider{name: provider.ProviderModal}
	r, c := newPlacementReconciler(t, []client.Object{pod}, modal) // no pool seeded

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected the Pod to stay gated when its pool is missing")
	}
}

func TestPlacement_StaleClaimForPriorPodBlocksUngate(t *testing.T) {
	// A claim of the Pod's name already exists but pins a PRIOR incarnation
	// (different UID). Placement must NOT ungate against the wrong ledger: it
	// leaves the Pod gated and requeues until the backstop reaps the stale claim.
	pod := gatedPod("p1", "default", "uid-new", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	stale := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "default-p1"},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: "default", Name: "p1", UID: "uid-old"},
			Provider: provider.ProviderModal,
			PoolRef:  "pool-a",
		},
	}
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
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
	if !hasGateNamed(got) {
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
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	mine := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "default-p1"},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: "default", Name: "p1", UID: "uid-1"},
			Provider: provider.ProviderModal,
			PoolRef:  "pool-a",
		},
	}
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool, mine}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if hasGateNamed(got) {
		t.Fatal("expected the Pod placed when the existing claim is its own")
	}
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != provider.ProviderModal {
		t.Fatalf("expected routing to modal, got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_IdempotentOnRetry(t *testing.T) {
	// A second reconcile after a successful placement must not error (claim
	// AlreadyExists is success) and must leave the Pod placed.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
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
	if hasGateNamed(final) {
		t.Fatal("expected the Pod placed again on retry")
	}
}

func TestPlacement_FailsOverToNextRegionWhenBlocked(t *testing.T) {
	// The pool lists two regions on one provider. us-east-1 is blocklisted, so the
	// inner region walk advances to us-west-2 within the SAME capacity tier.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWithRegions("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand},
		provider.ProviderModal, "us-east-1", "us-west-2")
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)
	r.Blocklist = &fakeBlocklist{blocked: []failover.Candidate{
		{Provider: provider.ProviderModal, Accelerator: "H100:1", CapacityType: nebulav1alpha1.CapacityOnDemand, Region: "us-east-1"},
	}}

	reconcilePod(t, r, "default", "p1")

	if got := getClaim(t, c, "default-p1").Spec.Region; got != "us-west-2" {
		t.Fatalf("expected failover to us-west-2, got region %q", got)
	}
}

func TestPlacement_CapacityIsOuterAxis(t *testing.T) {
	// Two providers, tiers [Spot, OnDemand]. Spot is blocked on BOTH providers but
	// OnDemand is free. Capacity is the OUTER axis, so the walk must exhaust every
	// provider's Spot before dropping any provider to OnDemand — landing on the
	// first provider's OnDemand, not the second provider's (still-Spot) offer.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand},
		"runpod", provider.ProviderModal)
	// Both advertise Spot, so the tier is genuinely reachable and the blocklist —
	// not a capability gap — is what pushes the walk onward (see
	// TestPlacement_SkipsSpotWhenProviderHasNoSpotTier for that path).
	runpod := &fakeProvider{name: "runpod", gpus: []string{"H100"}, spot: true}
	modal := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}, spot: true}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, runpod, modal)
	r.Blocklist = &fakeBlocklist{blocked: []failover.Candidate{
		// Both providers' Spot is exhausted (wildcard provider, Spot only).
		{CapacityType: nebulav1alpha1.CapacitySpot},
	}}

	reconcilePod(t, r, "default", "p1")

	if got := getClaim(t, c, "default-p1").Spec.CapacityType; got != nebulav1alpha1.CapacityOnDemand {
		t.Fatalf("expected the walk to drop to OnDemand, got %q", got)
	}
	// ...and to the FIRST provider (runpod), since OnDemand is walked provider-first.
	got := getPod(t, c, "default", "p1")
	if got.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] != "runpod" {
		t.Fatalf("expected first provider runpod at the OnDemand tier, got %v", got.Spec.NodeSelector)
	}
}

func TestPlacement_ExpandsRegionGroupIntoConcreteCandidates(t *testing.T) {
	// The pool declares a GROUP token, not a region. Placement must walk the concrete
	// regions the provider expands it into — and must record a CONCRETE one on the claim,
	// never the token: the claim's region feeds ProvisionRequest.Region, which the adapter
	// turns into a regional API endpoint, and "us" is not one.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWithRegions("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand},
		provider.ProviderModal, "us")
	prov := &fakeProvider{
		name: provider.ProviderModal, gpus: []string{"H100"},
		expandRegions: func(declared []string) []string {
			if slices.Equal(declared, []string{"us"}) {
				return []string{"us-east-1", "us-west-2"}
			}
			return declared
		},
	}
	// The first expanded region is blocked, so the walk must reach the second — proving
	// the group really became multiple candidates rather than one opaque value.
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)
	r.Blocklist = &fakeBlocklist{blocked: []failover.Candidate{
		{Provider: provider.ProviderModal, Accelerator: "H100:1",
			CapacityType: nebulav1alpha1.CapacityOnDemand, Region: "us-east-1"},
	}}

	reconcilePod(t, r, "default", "p1")

	if region := getClaim(t, c, "default-p1").Spec.Region; region != "us-west-2" {
		t.Fatalf("expected the group to expand and fail over to us-west-2, got %q", region)
	}
}

func TestRegionsFor_UnconstrainedOnRegionSimpleProviderYieldsOneCandidate(t *testing.T) {
	// A region-simple provider passes nil through (catalog.Base's default), so
	// expansion yields nothing. regionsFor must still emit ONE candidate — the empty
	// region, meaning "send no region" — or `range` would run zero times and the
	// provider would be silently unplaceable with no error anywhere.
	prov := &fakeProvider{name: provider.ProviderModal}
	got := regionsFor(prov, nebulav1alpha1.ProviderSpec{Name: provider.ProviderModal})
	if !slices.Equal(got, []string{""}) {
		t.Fatalf("regionsFor(nil) = %v, want one empty candidate", got)
	}
}

func TestRegionsFor_AgreesWithAWSSweepExpansion(t *testing.T) {
	// The two readers of ProviderSpec.Regions — placement's regionsFor and the AWS
	// RegionSource in cmd/main.go — MUST expand a declaration identically. If the
	// sweep covers less than placement provisions into, the missing region's instances
	// are absent from List, and applyState maps absence to Terminated: a live, billing
	// fleet reported as gone. Both go through ExpandRegions; this pins that they do.
	for _, declared := range [][]string{nil, {"us"}, {"eu"}, {"us-east-1"}, {"us", "me-central-1"}} {
		placementSide := regionsFor(awsprovider.New(nil, nil, nil),
			nebulav1alpha1.ProviderSpec{Name: provider.ProviderAWS, Regions: declared})
		sweepSide := awsprovider.ExpandRegions(declared)
		if !slices.Equal(placementSide, sweepSide) {
			t.Errorf("declared %v: placement walks %v but the sweep covers %v",
				declared, placementSide, sweepSide)
		}
	}
}

func TestPlacement_SkipsSpotWhenProviderHasNoSpotTier(t *testing.T) {
	// Modal has no user-facing preemptible capacity (SupportsSpot=false). The pool
	// asks for Spot first, but that candidate is unservable, so the walk falls
	// through to OnDemand — and the Pod is labelled OnDemand, which is what it will
	// actually be billed as. Placing it as Spot would be a silent downgrade: the
	// adapter ignores CapacityType, so the user would pay OnDemand rates for a Pod
	// whose annotation claims Spot.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand},
		provider.ProviderModal)
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}} // spot: false
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if hasGateNamed(got) {
		t.Fatal("expected the Pod placed at the OnDemand tier")
	}
	if tier := getClaim(t, c, "default-p1").Spec.CapacityType; tier != nebulav1alpha1.CapacityOnDemand {
		t.Fatalf("expected the Spot candidate skipped for OnDemand, got %q", tier)
	}
}

func TestPlacement_SpotOnlyPoolStaysGatedOnOnDemandOnlyProvider(t *testing.T) {
	// Spot is the pool's ONLY tier and the sole provider cannot serve it, so there is
	// no candidate at all. The Pod stays gated — visibly unplaceable — rather than
	// being quietly provisioned as OnDemand against an explicit Spot-only policy.
	// Nothing here can be fixed by a lapsing TTL, so there is no requeue hint: the
	// unblock is a pool edit or a provider gaining a Spot tier, both of which
	// generate their own event.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot}, provider.ProviderModal)
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}} // spot: false
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "p1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue hint for a capability gap, got %v", res.RequeueAfter)
	}

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected the Pod to stay gated when no provider serves the only tier")
	}
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err == nil {
		t.Fatal("expected no claim for an unplaceable Pod")
	}
}

func TestPlacement_CopiesNoEgressPolicyOntoThePod(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	pool.Spec.Egress = &nebulav1alpha1.EgressPolicy{
		Mode:    nebulav1alpha1.EgressAllowlist,
		Targets: []string{"10.0.0.0/8", "*.huggingface.co"},
	}
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}, egress: true}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if hasGateNamed(got) {
		t.Fatal("expected the Pod placed on a provider that enforces egress")
	}
	// Swept by substring rather than checked against the two retired keys, because the claim
	// is that NO key carries the policy — a copy under a fresh spelling is the same bug and
	// should fail here too.
	for k, v := range got.Annotations {
		if strings.Contains(k, "egress") {
			t.Errorf("annotation %s=%q copies the pool's egress policy onto the Pod; the "+
				"handler must read it from the pool", k, v)
		}
	}
	// The pool label is what the handler resolves the policy through, so it must be set.
	if got.Labels[nebulav1alpha1.PoolLabel] != "pool-a" {
		t.Errorf("pool label = %q, want pool-a", got.Labels[nebulav1alpha1.PoolLabel])
	}
}

func TestPlacement_CopiesNothingNebulaOwnedOntoThePod(t *testing.T) {
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWithRegions("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot},
		provider.ProviderModal, "us-east-1")
	// A pool with something to say on every axis that used to ride the Pod.
	pool.Spec.Egress = &nebulav1alpha1.EgressPolicy{Mode: nebulav1alpha1.EgressBlocked}
	pool.Spec.Failover = &nebulav1alpha1.FailoverPolicy{BlocklistTTL: metav1.Duration{Duration: time.Hour}}
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}, spot: true, egress: true}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	reconcilePod(t, r, "default", "p1")

	got := getPod(t, c, "default", "p1")
	if hasGateNamed(got) {
		t.Fatal("expected the Pod placed")
	}
	// Swept by prefix rather than key by key: the claim is that NO Nebula-owned annotation is
	// written, so a copy under a fresh spelling is the same bug and fails here too.
	for k, v := range got.Annotations {
		if strings.HasPrefix(k, nebulav1alpha1.GroupVersion.Group+"/") {
			t.Errorf("annotation %s=%q was stamped on the Pod; a provisioning input on the Pod "+
				"is patchable between ungate and CreatePod", k, v)
		}
	}
	// ...and the decision really is recorded, on the claim the handler reads.
	nc := getClaim(t, c, "default-p1")
	if nc.Spec.CapacityType != nebulav1alpha1.CapacitySpot || nc.Spec.Region != "us-east-1" {
		t.Errorf("claim records tier %q region %q, want Spot/us-east-1",
			nc.Spec.CapacityType, nc.Spec.Region)
	}
}

func TestPlacement_RestrictedPoolStaysGatedWhenNoProviderEnforcesEgress(t *testing.T) {
	// The provider cannot enforce the policy, so there is no candidate and the Pod stays
	// visibly unplaceable. This is the whole point of the capability gate: placing it
	// anyway would provision a workload with open internet access under a pool that says
	// Blocked, with nothing to reveal the substitution. No requeue hint — only a pool edit
	// or a provider gaining support fixes it, and both emit their own event.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	pool.Spec.Egress = &nebulav1alpha1.EgressPolicy{Mode: nebulav1alpha1.EgressBlocked}
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}} // egress: false
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "p1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue hint for a capability gap, got %v", res.RequeueAfter)
	}

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected the Pod to stay gated when no provider can enforce the egress policy")
	}
}

func TestPlacement_AllCandidatesBlockedRequeuesForBlockExpiry(t *testing.T) {
	// Every (tier, provider, region) candidate is blocked (DenyAll on the provider),
	// but the candidates are servable — the block is a transient failover exclusion.
	// Placement has nowhere to go now, so it leaves the Pod gated, but it must
	// requeue for when the soonest block frees (TTL expiry emits no event) rather
	// than idling until the periodic resync.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand},
		provider.ProviderModal)
	// spot:true so BOTH tiers are servable and every skip is the blocklist's doing —
	// the point of the test is the requeue hint, which only exists for candidates a
	// lapsing TTL can free.
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"H100"}, spot: true}
	r, c := newPlacementReconciler(t, []client.Object{pod, pool}, prov)
	r.Blocklist = &fakeBlocklist{
		blocked: []failover.Candidate{{Provider: provider.ProviderModal}}, // whole-provider block (auth/quota)
		until:   2 * time.Minute,
	}

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "p1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	// Requeue lands at the block's expiry plus a stable per-Pod jitter (added so
	// Pods sharing a scope-keyed block don't wake in lockstep), so it sits in
	// [2m, 2m+jitter). The jitter must be strictly bounded, never negative.
	if res.RequeueAfter < 2*time.Minute || res.RequeueAfter >= 2*time.Minute+blockRequeueJitter {
		t.Fatalf("expected requeue in [2m, 2m+jitter), got %v", res.RequeueAfter)
	}

	got := getPod(t, c, "default", "p1")
	if !hasGateNamed(got) {
		t.Fatal("expected the Pod to stay gated when every candidate is blocked")
	}
	// No claim should have been created for an unplaced Pod.
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: "default-p1"}, &nc); err == nil {
		t.Fatal("expected no claim when placement is blocked everywhere")
	}
}

func TestRequeueJitter_StableAndBounded(t *testing.T) {
	// The offset must be deterministic per UID (a Pod's own retries must not drift,
	// so it isn't a moving target) and stay within [0, blockRequeueJitter).
	uids := []types.UID{"uid-1", "uid-2", "uid-3", "", "a-much-longer-pod-uid-value"}
	for _, uid := range uids {
		first := requeueJitter(uid)
		if first < 0 || first >= blockRequeueJitter {
			t.Fatalf("jitter(%q) = %v, want [0, %v)", uid, first, blockRequeueJitter)
		}
		if again := requeueJitter(uid); again != first {
			t.Fatalf("jitter(%q) not stable: %v then %v", uid, first, again)
		}
	}

	// Distinct UIDs should generally land on distinct offsets (that's the whole
	// point). Not a hard guarantee for any two, but across a spread they must not
	// all collapse to one value.
	distinct := map[time.Duration]struct{}{}
	for _, uid := range uids {
		distinct[requeueJitter(uid)] = struct{}{}
	}
	if len(distinct) < 2 {
		t.Fatalf("jitter collapsed distinct UIDs to %d offset(s); herd would not spread", len(distinct))
	}
}

func TestPlacement_NoServableCandidateDoesNotRequeue(t *testing.T) {
	// The pool's only provider does not offer the requested accelerator, so no
	// candidate is servable and none can be freed by a lapsing TTL. Placement leaves
	// the Pod gated with NO requeue — a Pod edit, pool edit, or the resync is what
	// retries — rather than spinning on a candidate that will never become servable.
	pod := gatedPod("p1", "default", "uid-1", "pool-a", "H100")
	pool := poolWith("pool-a", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, provider.ProviderModal)
	prov := &fakeProvider{name: provider.ProviderModal, gpus: []string{"A100"}} // no H100
	r, _ := newPlacementReconciler(t, []client.Object{pod, pool}, prov)

	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: "default", Name: "p1"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter != 0 {
		t.Fatalf("expected no requeue when no candidate can ever be unblocked, got %v", res.RequeueAfter)
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
