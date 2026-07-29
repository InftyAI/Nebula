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
	"hash/fnv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/util"
)

// poolFor resolves the NodePool a Pod is placed against. The Pod names its pool
// via PoolLabel. Returns (nil, nil) when the label is absent or the named pool
// does not exist, so the caller leaves the Pod gated rather than guessing.
func (r *PodPlacementReconciler) poolFor(ctx context.Context, pod *corev1.Pod) (*nebulav1alpha1.NodePool, error) {
	name := pod.Labels[nebulav1alpha1.PoolLabel]
	if name == "" {
		return nil, nil
	}
	var pool nebulav1alpha1.NodePool
	if err := r.Get(ctx, client.ObjectKey{Name: name}, &pool); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return &pool, nil
}

// selectPlacement resolves the pool's policy along the two orthogonal axes, in the
// fixed order the NodePoolSpec doc mandates: capacity tier is the OUTER axis and
// region (nested per provider) is the INNER one. It walks
//
//	FOR each capacityType in CapacityTypes (listed order):   // outer: hard tier
//	    FOR each provider (listed order = Ordered strategy):
//	        FOR each of that provider's regions:              // inner
//	            skip if the candidate is blocklisted; else place here
//
// so every provider's Spot is tried (in every one of its regions) before ANY
// provider's OnDemand — "capacity is the outer axis". Skipping blocklisted
// candidates is what turns a single provision failure into failover: a region-
// scoped block (a Spot limit in us-east-1) advances to the next region, then the
// next tier, without hot-looping against the placement that just failed. The
// adapter already handled the finer zone axis (sweeping a region's AZs) before the
// failure ever reached the blocklist, so this loop only walks regions, not zones.
//
// Returns ok=false when every (tier, provider, region) candidate is either
// unservable (provider unregistered or does not offer the accelerator) or blocked;
// the caller then leaves the Pod gated for a later retry (a pool edit, a provider
// registering, or a block expiring). Provider quirks (e.g. Modal being
// OnDemand-only) are still handled at Provision time, not here.
//
// On ok=false it also returns retryAfter: the time until the SOONEST currently-
// servable candidate (one skipped ONLY because it is blocklisted) frees, or 0 when
// no candidate can ever be unblocked by a lapsing TTL (every candidate is
// unregistered or does not offer the accelerator). The caller requeues on a
// positive hint so a Pod stuck purely on failover retries the moment a block
// expires, rather than idling until the periodic resync — blocklist TTL expiry
// emits no event of its own.
//
// Strategy (LowestPrice/Weighted price-ranking) is a later swap-in for the inner
// ordering; today the inner walk is listed order (Ordered), which the caller's
// flow does not depend on — only that a (provider, capacityType, region) comes
// back.
func (r *PodPlacementReconciler) selectPlacement(ctx context.Context, pod *corev1.Pod, pool *nebulav1alpha1.NodePool) (placement, bool, time.Duration) {
	log := logf.FromContext(ctx).WithName("placement-select").WithValues(
		"pod", pod.Namespace+"/"+pod.Name, "pool", pool.Name)

	// The (type, count) together select the concrete offering: a provider resolves
	// them through MapAccelerator to its own id (an EC2 instance type on AWS), which
	// is what the blocklist keys on so L4x1 and L4x8 (distinct instance types) block
	// independently. A malformed request is treated as "no accelerator" so placement
	// stays a no-op rather than erroring the reconcile — provisioning would surface
	// the real error.
	accel, count, _ := util.AcceleratorRequest(pod)

	var soonest time.Duration                  // 0 = no blocked-but-servable candidate seen
	for _, tier := range capacityTiers(pool) { // outer: capacity
		for _, ref := range pool.Spec.Providers { // provider (Ordered = listed order)
			prov, ok := r.provider(ref.Name)
			if !ok {
				log.V(1).Info("skipping candidate: provider not registered",
					"provider", ref.Name, "capacityType", tier)
				continue // unregistered; NodePool status surfaces this separately
			}
			// A CPU-only Pod (no accelerator) matches any provider; an accelerator
			// Pod only matches a provider whose catalog serves that (type, count). The
			// resolved id is also what the block is keyed on, so it is captured here
			// from the same lookup that decides servability.
			acceleratorID := ""
			if accel != "" {
				id, offered := prov.MapAccelerator(accel, count)
				if !offered {
					log.V(1).Info("skipping candidate: provider does not offer the accelerator",
						"provider", ref.Name, "accelerator", accel, "count", count)
					continue
				}
				acceleratorID = id
			}
			for _, region := range regionsFor(ref) { // inner: region
				if until, blocked := r.blockedUntil(ref.Name, acceleratorID, tier, region); blocked {
					// Servable but failed recently; try the next region, then the next
					// tier, and remember when this one frees so we can requeue for it.
					log.Info("skipping candidate: blocked by failover blocklist",
						"provider", ref.Name, "acceleratorID", acceleratorID,
						"capacityType", tier, "region", region, "freesIn", until.String())
					if until > 0 && (soonest == 0 || until < soonest) {
						soonest = until
					}
					continue
				}
				log.Info("selected placement candidate",
					"provider", ref.Name, "capacityType", tier, "region", region)
				return placement{
					provider:     ref.Name,
					capacityType: tier,
					region:       region,
				}, true, 0
			}
		}
	}
	return placement{}, false, soonest
}

// capacityTiers is the outer axis to walk: the pool's CapacityTypes in fallback
// order. An empty list means "the provider default tier" — a single unnamed
// candidate ("") so the walk still runs once. (Admission defaults the field, so
// this only guards a hand-built pool.)
func capacityTiers(pool *nebulav1alpha1.NodePool) []nebulav1alpha1.CapacityType {
	if len(pool.Spec.CapacityTypes) == 0 {
		return []nebulav1alpha1.CapacityType{""}
	}
	return pool.Spec.CapacityTypes
}

// regionsFor is the inner axis for one provider ref: the regions to try, in listed
// order. An empty/omitted list means "the provider's configured default region",
// represented as a single empty-string candidate so the walk runs once for
// region-simple providers (Modal, RunPod). AWS is required by admission to list at
// least one region (the CEL rule on NodePoolSpec). An "all regions" wildcard is not
// supported yet (see ProviderSpec.Regions), so there is nothing to expand here.
func regionsFor(ref nebulav1alpha1.ProviderSpec) []string {
	if len(ref.Regions) == 0 {
		return []string{""} // omitted => provider default region (region-simple providers)
	}
	return ref.Regions
}

// blockedUntil reports whether the (provider, acceleratorID, tier, region)
// candidate is currently excluded by the failover blocklist and, if so, how long
// until it frees (for the requeue hint). acceleratorID is the provider's resolved
// id for the request (see selectPlacement), so a capacity block matches only
// candidates on the same instance type / pool. It is nil-safe: with no blocklist
// wired (tests, or a blocklist-less build) nothing is ever blocked.
func (r *PodPlacementReconciler) blockedUntil(provName, acceleratorID string, tier nebulav1alpha1.CapacityType, region string) (time.Duration, bool) {
	if r.Blocklist == nil {
		return 0, false
	}
	return r.Blocklist.BlockedUntil(failover.Candidate{
		Provider:      provName,
		AcceleratorID: acceleratorID,
		CapacityType:  tier,
		Region:        region,
	})
}

// requeueJitter maps a Pod UID to a stable offset in [0, blockRequeueJitter) to
// desynchronize the requeues of Pods that share a scope-keyed failover block.
// It is deterministic (a hash of the UID, not a random draw) so a given Pod's
// successive retries land at the same offset — the goal is to spread DISTINCT
// Pods apart, not to move one Pod around between attempts. A missing UID hashes
// to a fixed value, which is harmless: real Pods always carry a UID.
func requeueJitter(uid types.UID) time.Duration {
	h := fnv.New64a()
	_, _ = h.Write([]byte(uid))
	return time.Duration(h.Sum64() % uint64(blockRequeueJitter))
}

// ensureClaim creates the NodeClaim for this placement if it does not already
// exist. The claim is the durable teardown ledger (see NodeClaimReconciler): it
// pins the Pod by UID, records the chosen provider and capacity tier, and labels
// itself with the pool for status roll-up.
//
// It returns ready=true only when a claim pinned to THIS Pod's UID exists, so the
// caller may ungate. Two cases return ready=false with no error, telling the
// caller to requeue rather than ungate:
//   - A pre-existing claim of the same name still pins a PRIOR Pod incarnation
//     (same namespace/name, different UID). This happens when a Pod is recreated
//     faster than the backstop reaps the old Pod's claim. Ungating now would bind
//     the new Pod against a ledger that names the wrong instance, so we wait: the
//     NodeClaim controller sees the old Pod is gone (UID mismatch), self-deletes
//     the stale claim and terminates its instance, after which our Create succeeds
//     with the correct UID.
func (r *PodPlacementReconciler) ensureClaim(ctx context.Context, pod *corev1.Pod, pool *nebulav1alpha1.NodePool, p placement) (ready bool, err error) {
	claim := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: util.ClaimName(pod.Namespace, pod.Name),
			Labels: map[string]string{
				nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
				nebulav1alpha1.PoolLabel:      pool.Name,
				nebulav1alpha1.ProviderLabel:  p.provider,
			},
		},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef: nebulav1alpha1.PodReference{
				Namespace: pod.Namespace,
				Name:      pod.Name,
				UID:       string(pod.UID),
			},
			Provider:     p.provider,
			CapacityType: p.capacityType,
			Region:       p.region,
			PoolRef:      pool.Name,
		},
	}
	err = r.Create(ctx, claim)
	if err == nil {
		return true, nil // freshly created for this Pod
	}
	if !apierrors.IsAlreadyExists(err) {
		return false, err
	}

	// A claim of this name already exists. Confirm it belongs to THIS Pod before
	// treating the create as a successful no-op; otherwise it is a stale claim for
	// a prior same-named Pod and we must wait for the backstop to reap it.
	var existing nebulav1alpha1.NodeClaim
	if err := r.Get(ctx, client.ObjectKeyFromObject(claim), &existing); err != nil {
		// A NotFound here means the stale claim was reaped between our Create and
		// Get; let the caller requeue so the next pass re-creates it cleanly.
		return false, client.IgnoreNotFound(err)
	}
	if existing.Spec.PodRef.UID == string(pod.UID) {
		return true, nil // our own claim (a retry after a crash before ungate)
	}
	return false, nil // stale claim for a prior Pod; wait for the backstop
}

// place stamps the routing decision onto the Pod and removes the gate, atomically
// from the Pod's perspective (one Update). After this, the scheduler is free to
// bind the Pod to the chosen provider's virtual node.
func (r *PodPlacementReconciler) place(ctx context.Context, pod *corev1.Pod, pool *nebulav1alpha1.NodePool, p placement) error {
	// Route to the provider's virtual node.
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] = p.provider

	// Carry the capacity tier and region the VK handler reads on CreatePod (inputs
	// that are not otherwise on the Pod). Skip each when empty (provider default).
	if p.capacityType != "" {
		setAnnotation(pod, nebulav1alpha1.CapacityTypeAnnotation, string(p.capacityType))
	}
	if p.region != "" {
		setAnnotation(pod, nebulav1alpha1.RegionAnnotation, p.region)
	}
	// Carry the pool's blocklist TTL so the VK handler (which never sees the pool)
	// knows how long to exclude a placement that fails. Only stamp an explicit
	// policy value; an unset policy leaves the handler on its own default.
	if pool.Spec.Failover != nil && pool.Spec.Failover.BlocklistTTL.Duration > 0 {
		setAnnotation(pod, nebulav1alpha1.BlocklistTTLAnnotation, pool.Spec.Failover.BlocklistTTL.Duration.String())
	}

	// Remove our gate, releasing the Pod to the scheduler. Preserve any other
	// gates a different controller may hold.
	pod.Spec.SchedulingGates = removeGate(pod.Spec.SchedulingGates, nebulav1alpha1.ProviderSelectionGate)

	return r.Update(ctx, pod)
}

// setAnnotation sets one annotation on the Pod, allocating the map on first use.
func setAnnotation(pod *corev1.Pod, key, value string) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[key] = value
}

// removeGate returns gates with the named gate removed, preserving order.
func removeGate(gates []corev1.PodSchedulingGate, name string) []corev1.PodSchedulingGate {
	out := gates[:0]
	for _, g := range gates {
		if g.Name != name {
			out = append(out, g)
		}
	}
	return out
}

// reapTerminalPod deletes a terminated, Nebula-owned, controller-managed Pod so
// its owner recreates it cleanly instead of leaving a Failed tombstone. It
// returns reaped=true when it issued (or already sees) a delete for this Pod, so
// the caller stops processing it as a placement candidate.
//
// The delete is what actually stamps the Pod's deletionTimestamp — that field is
// server-managed and cannot be written directly, so a real Delete call is the
// only way to remove the object. It is scoped narrowly to avoid touching Pods
// that are not ours to reap:
//   - EnabledLabel: only Pods that opted into Nebula (never a plain workload).
//   - terminal phase: only Failed/Succeeded (a live Pod is never touched).
//   - controller-owned: only Pods a ReplicaSet/Job will recreate; a bare Pod is
//     left as an inspectable record.
//
// Delete is UID-pinned so a Pod already replaced by a same-name recreate is not
// clobbered, and a NotFound (already gone) is treated as success.
func (r *PodPlacementReconciler) reapTerminalPod(ctx context.Context, pod *corev1.Pod) (bool, error) {
	if pod.Labels[nebulav1alpha1.EnabledLabel] != "true" {
		return false, nil
	}
	if !pod.DeletionTimestamp.IsZero() {
		return true, nil // already being deleted; nothing more to place
	}
	if !isTerminal(pod.Status.Phase) {
		return false, nil
	}
	if !isControllerOwned(pod) {
		return false, nil // bare Pod: leave it as a record
	}

	preconditions := metav1.Preconditions{UID: &pod.UID}
	if err := r.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &preconditions}); err != nil {
		return false, client.IgnoreNotFound(err)
	}
	return true, nil
}

// isControllerOwned reports whether the Pod has a controlling owner (e.g. a
// ReplicaSet or Job) that will recreate it after deletion.
func isControllerOwned(pod *corev1.Pod) bool {
	return metav1.GetControllerOf(pod) != nil
}

// SetupWithManager wires the controller. It watches Pods; NodePool edits are not
// watched because a Pod already gated will re-reconcile on the periodic resync,
// and a newly-created Pod always triggers its own event.
func (r *PodPlacementReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.Pod{}).
		Watches(&nebulav1alpha1.NodePool{}, handler.EnqueueRequestsFromMapFunc(r.podsForPool)).
		Named("pod-placement").
		Complete(r)
}

// podsForPool re-enqueues gated Pods that name a pool when that pool changes, so
// a pool edit (e.g. adding a provider that can now serve a stuck Pod) promptly
// retries placement instead of waiting for the resync.
func (r *PodPlacementReconciler) podsForPool(ctx context.Context, obj client.Object) []reconcile.Request {
	pool, ok := obj.(*nebulav1alpha1.NodePool)
	if !ok {
		return nil
	}
	var pods corev1.PodList
	if err := r.List(ctx, &pods, client.MatchingLabels{nebulav1alpha1.PoolLabel: pool.Name}); err != nil {
		return nil
	}
	var reqs []reconcile.Request
	for i := range pods.Items {
		if needsPlacement(&pods.Items[i]) {
			reqs = append(reqs, reconcile.Request{
				NamespacedName: client.ObjectKeyFromObject(&pods.Items[i]),
			})
		}
	}
	return reqs
}
