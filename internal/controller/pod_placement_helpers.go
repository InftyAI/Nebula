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
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/provider"
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
// so every provider's Spot is tried, in every region, before ANY provider's OnDemand.
// Skipping blocklisted candidates is what turns one provision failure into failover: a
// region-scoped block advances to the next region, then the next tier, without hot-looping
// against the placement that just failed. The adapter already swept the region's AZs before
// the failure got here, so this loop walks regions, not zones.
//
// ok=false means every candidate is either unservable (provider unregistered, no such
// accelerator, or cannot serve the tier) or blocked; the caller leaves the Pod gated for a
// pool edit, a provider registering, or a block expiring.
//
// On ok=false, retryAfter is the time until the soonest servable-but-blocked candidate
// frees, or 0 when no TTL can ever unblock anything. A positive hint requeues the Pod, since
// a block expiring emits no event of its own.
//
// It owns the placement metrics for what it decides — a skip counter per candidate, and the
// deferral reason on ok=false — because only this scope can tell an invalid request from an
// all-blocked pool from one that cannot serve the request. The caller files its own two
// deferrals (no resolvable pool, stale NodeClaim).
//
// TODO: Strategy (LowestPrice/Weighted) will replace the inner listed order; nothing in the
// caller depends on the ordering, only on a (provider, capacityType, region) coming back.
func (r *PodPlacementReconciler) selectPlacement(ctx context.Context, pod *corev1.Pod, pool *nebulav1alpha1.NodePool) (placement, bool, time.Duration) {
	log := logf.FromContext(ctx).WithName("placement-select").WithValues(
		"pod", pod.Namespace+"/"+pod.Name, "pool", pool.Name)

	// The (type, count) together select the concrete offering: a provider resolves
	// them through MapAccelerator to its own id (an EC2 instance type on AWS), which
	// is what the blocklist keys on so L4x1 and L4x8 (distinct instance types) block
	// independently. A malformed request is left gated rather than silently routed as
	// CPU-only; the user must fix the Pod spec (or a controller-generated Pod's
	// source object) before placement can proceed.
	accel, count, err := util.AcceleratorRequest(pod)
	if err != nil {
		metrics.RecordDeferral(pool.Name, metrics.DeferInvalidRequest)
		log.Info("invalid accelerator request; leaving Pod gated", "error", err.Error())
		return placement{}, false, 0
	}

	var soonest time.Duration                  // 0 = no blocked-but-servable candidate seen
	for _, tier := range capacityTiers(pool) { // outer: capacity
		for _, ref := range pool.Spec.Providers { // provider (Ordered = listed order)
			prov, ok := r.provider(ref.Name)
			if !ok {
				// No region on this skip and the two below: they are decided before the
				// walk reaches the region axis, so they rule out every region at once.
				metrics.RecordCandidateSkip(ref.Name, tier, "", metrics.SkipProviderUnregistered)
				log.V(1).Info("skipping candidate: provider not registered",
					"provider", ref.Name, "capacityType", tier)
				continue // unregistered; NodePool status surfaces this separately
			}
			if !servesCapacityTier(prov, tier) {
				metrics.RecordCandidateSkip(ref.Name, tier, "", metrics.SkipCapacityUnsupported)
				log.V(1).Info("skipping candidate: provider does not offer the capacity tier",
					"provider", ref.Name, "capacityType", tier)
				continue
			}
			if !servesEgress(prov, pool.Spec.Egress) {
				metrics.RecordCandidateSkip(ref.Name, tier, "", metrics.SkipEgressUnsupported)
				log.V(1).Info("skipping candidate: provider cannot enforce the pool's egress policy",
					"provider", ref.Name, "egressMode", pool.Spec.Egress.ModeOrOpen())
				continue
			}
			// A CPU-only Pod (no accelerator) matches any provider; an accelerator
			// Pod only matches a provider whose catalog serves that (type, count).
			// MapAccelerator is consulted only for that servability check — the block
			// key and the reported identity are the POOL (type:count), not the
			// provider's SKU, so a launch spanning alternates and a post-launch SKU
			// swap never desync the key. Empty for a CPU-only Pod.
			accelerator := util.AcceleratorPool(accel, count)
			if accel != "" {
				if _, offered := prov.MapAccelerator(accel, count); !offered {
					metrics.RecordCandidateSkip(ref.Name, tier, "", metrics.SkipAcceleratorUnsupported)
					log.V(1).Info("skipping candidate: provider does not offer the accelerator",
						"provider", ref.Name, "accelerator", accel, "count", count)
					continue
				}
			}
			for _, region := range regionsFor(prov, ref) { // inner: region
				if until, blocked := r.blockedUntil(ref.Name, accelerator, tier, region); blocked {
					// Servable but failed recently; try the next region, then the next
					// tier, and remember when this one frees so we can requeue for it.
					metrics.RecordCandidateSkip(ref.Name, tier, region, metrics.SkipBlocked)
					log.Info("skipping candidate: blocked by failover blocklist",
						"provider", ref.Name, "accelerator", accelerator,
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
					accelerator:  accelerator,
				}, true, 0
			}
		}
	}
	// The walk is exhausted. Which deferral this is turns on whether anything was
	// merely blocked: a positive soonest means a servable candidate exists and failover
	// is holding it off (self-clearing, and the caller requeues for it), while zero
	// means nothing in the pool can serve this request at all.
	if soonest > 0 {
		metrics.RecordDeferral(pool.Name, metrics.DeferAllBlocked)
	} else {
		metrics.RecordDeferral(pool.Name, metrics.DeferNoCandidate)
	}
	return placement{}, false, soonest
}

// placementLabels renders the metric label set for a completed placement. It mirrors
// the virtual kubelet's Handler.metricLabels field for field — that symmetry is the
// point, not a coincidence: the handler reads region and capacity type back off the
// annotations that place() stamps from this very decision, so both halves of a Pod's
// journey report identical label values and their series join cleanly.
//
// The accelerator type and count are passed apart, not as the joined pool identity
// placement.accelerator carries, because a metric label must be aggregatable (see
// metrics.candidateLabels). The parse cannot fail here: selectPlacement already
// rejected a malformed request before any placement was returned.
func placementLabels(pod *corev1.Pod, p placement) metrics.Labels {
	accel, count, _ := util.AcceleratorRequest(pod)
	return metrics.Labels{
		Provider:         p.provider,
		Region:           p.region,
		CapacityType:     string(p.capacityType),
		Accelerator:      accel,
		AcceleratorCount: count,
	}
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

// servesCapacityTier reports whether prov can deliver the candidate's capacity tier. Only Spot
// is ever refused: an OnDemand-only provider (Modal) has no interruptible tier, so placing a
// Spot candidate there would stamp CapacityType=Spot on the Pod, hand it to an adapter that
// drops the field, and bill OnDemand rates for capacity the user asked to be cheap — with no
// error or event revealing the substitution. Skipping lets the pool's next tier take over
// ([Spot, OnDemand] still lands on Modal, now truthfully labelled), and a Spot-only pool
// leaves the Pod visibly unplaceable rather than quietly overcharged.
//
// The empty tier is "the provider's default", which every provider serves, so it passes.
func servesCapacityTier(prov provider.Provider, tier nebulav1alpha1.CapacityType) bool {
	if tier != nebulav1alpha1.CapacitySpot {
		return true
	}
	return prov.Capabilities().SupportsSpot
}

// servesEgress reports whether prov can enforce the pool's egress policy. Open needs no
// enforcement, so every provider serves it; anything else needs SupportsEgressPolicy.
//
// Same reasoning as servesCapacity, and load-bearing for a different reason: a provider
// that drops the field would put the workload on the open internet while the pool claims
// containment. Skipping makes that visible — an AWS-only pool asking for Blocked leaves the
// Pod unplaceable instead of silently unprotected.
func servesEgress(prov provider.Provider, policy *nebulav1alpha1.EgressPolicy) bool {
	if !policy.RestrictsEgress() {
		return true
	}
	return prov.Capabilities().SupportsEgressPolicy
}

// regionsFor is the inner axis for one provider ref: the concrete regions to try, in
// expansion order. The pool's declaration is a CONSTRAINT, not a list of regions —
// it may be omitted (unconstrained), name a geography group ("us"), or name regions
// literally — so only the provider can resolve it, and ExpandRegions does (see
// provider.Provider for the three levels).
//
// The empty-string fallback covers expansion yielding nothing: a region-simple provider
// whose pool declared no regions still needs ONE candidate, or `range` runs zero times and
// the provider is silently unplaceable. That candidate means "send no region, place freely" —
// Modal's normal and cheapest mode.
//
// This and awsRegionSource (cmd/main.go) are the only readers of ProviderSpec.Regions and
// MUST expand it identically: a region provisioned into but not swept is absent from List,
// and absence is reported as Terminated on a live, billing instance.
func regionsFor(prov provider.Provider, ref nebulav1alpha1.ProviderSpec) []string {
	regions := prov.ExpandRegions(ref.Regions)
	if len(regions) == 0 {
		return []string{""} // unconstrained on a region-simple provider
	}
	return regions
}

// blockedUntil reports whether the (provider, accelerator, tier, region)
// candidate is currently excluded by the failover blocklist and, if so, how long
// until it frees (for the requeue hint). accelerator is the request's pool
// identity (type:count; see selectPlacement), so a capacity block matches only
// candidates on the same pool. It is nil-safe: with no blocklist wired (tests, or
// a blocklist-less build) nothing is ever blocked.
func (r *PodPlacementReconciler) blockedUntil(provName, accelerator string, tier nebulav1alpha1.CapacityType, region string) (time.Duration, bool) {
	if r.Blocklist == nil {
		return 0, false
	}
	return r.Blocklist.BlockedUntil(failover.Candidate{
		Provider:     provName,
		Accelerator:  accelerator,
		CapacityType: tier,
		Region:       region,
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
			Accelerator:  p.accelerator,
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

// place writes the one thing the Pod itself needs — the nodeSelector that routes it to the
// chosen provider's virtual node — and removes the gate, atomically from the Pod's
// perspective (one Update). After this, the scheduler is free to bind it.
//
// Nothing else about the decision goes on the Pod. The provisioning inputs (capacity tier,
// region) are already on the NodeClaim ensureClaim wrote a moment ago, and the pool's policy
// (egress, failover TTL) stays on the NodePool. All of it used to be stamped here for the VK
// handler to read back, which made every one of them patchable between ungate and CreatePod
// by whoever the policy constrains; the handler reads cluster state instead.
func (r *PodPlacementReconciler) place(ctx context.Context, pod *corev1.Pod, p placement) error {
	// Route to the provider's virtual node.
	if pod.Spec.NodeSelector == nil {
		pod.Spec.NodeSelector = map[string]string{}
	}
	pod.Spec.NodeSelector[nebulav1alpha1.ProviderLabel] = p.provider

	// Remove our gate, releasing the Pod to the scheduler. Preserve any other
	// gates a different controller may hold.
	pod.Spec.SchedulingGates = removeGate(pod.Spec.SchedulingGates, nebulav1alpha1.ProviderSelectionGate)

	return r.Update(ctx, pod)
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
	if pod.Labels[nebulav1alpha1.EnabledLabel] != nebulav1alpha1.EnabledValue {
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
		// Every opted-in Pod passes through here before the scheduler may touch it, so this
		// controller's throughput is the fleet's admission rate (see concurrentReconciles).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentReconciles}).
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
