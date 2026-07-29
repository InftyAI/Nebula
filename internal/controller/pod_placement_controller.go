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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// staleClaimRequeue is how long placement waits before re-checking when a
// same-named NodeClaim from a prior Pod incarnation still exists. It only needs
// to outlast the NodeClaim backstop's reap of the stale claim, so a short,
// bounded retry is enough.
const staleClaimRequeue = 5 * time.Second

// blockRequeueJitter is the width of the window a blocked-everywhere Pod's
// requeue is spread across, ON TOP OF the block's expiry. A failover block is
// keyed by scope (provider/accelerator/tier/region), not by Pod, so every Pod
// contending for the same capacity pool sees the same expiry and, without
// jitter, would requeue at the same instant — then all retry together, all hit
// the still-tight pool together, and all re-block with a fresh TTL in lockstep
// (a synchronized retry storm). A deterministic per-Pod offset in [0, jitter)
// breaks the herd apart while staying stable across a Pod's own retries.
const blockRequeueJitter = 30 * time.Second

// PodPlacementReconciler is the middle of the placement flow: it turns a gated,
// opted-in Pod into a placed one. The webhook holds the Pod SchedulingGated
// until a provider is chosen; this controller chooses one, records the decision
// on a NodeClaim (the durable teardown ledger), stamps the routing nodeSelector
// and capacity-type annotation onto the Pod, and removes the gate. The scheduler
// then binds the ungated Pod to that provider's virtual node, where the virtual
// kubelet provisions the external instance.
//
// Policy (v1): "first matching provider". Within the pool's provider list, pick
// the first whose catalog offers the requested GPU type. The richer optimizer
// (LowestPrice/Weighted, capacity-tier fallback, blocklist) is a later swap-in
// behind selectPlacement; the surrounding flow (gate, NodeClaim, ungate) is what
// this controller owns and does not change.
type PodPlacementReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Providers resolves a provider name to its backend; defaults to the
	// process-wide registry. Overridable in tests.
	Providers func(name string) (provider.Provider, bool)

	// Blocklist is the shared failover blocklist the VK handlers write to on a
	// Provision failure; selectPlacement reads it to skip a candidate that just
	// failed. May be nil (no candidate is ever considered blocked), keeping tests
	// and blocklist-less wiring simple.
	Blocklist Blocklister
}

// Blocklister is the read side of pkg/failover.Blocklist the placement controller
// depends on: it asks whether a candidate placement is currently excluded and,
// when it is, how long until it frees (so a Pod stuck on failover can requeue for
// the exact moment a block lapses rather than idling until the periodic resync).
// The concrete type is injected from main; this narrow interface keeps the
// controller decoupled and a nil value a no-op.
type Blocklister interface {
	BlockedUntil(c failover.Candidate) (retryAfter time.Duration, blocked bool)
}

// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodepools,verbs=get;list;watch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodeclaims,verbs=get;list;watch;create

// Reconcile places one gated Pod. It is a no-op for any Pod that is not an
// opted-in, still-gated, not-yet-scheduled workload.
func (r *PodPlacementReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pod corev1.Pod
	if err := r.Get(ctx, req.NamespacedName, &pod); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Reap a terminated Nebula workload Pod. When the external instance goes away
	// (torn down, reclaimed, or exited) VK reports the Pod in a terminal phase, but
	// nothing removes the Pod object: a ReplicaSet leaves a Failed Pod in place
	// (creating a replacement beside it), and VK's own provider-eviction path skips
	// Pods already terminal. Left alone the tombstone lingers until Kubernetes
	// PodGC sweeps it. We hold a Kubernetes client, so delete it here when it is
	// controller-owned — the owner (Deployment/Job) then recreates it cleanly and
	// the now Pod-less NodeClaim self-deletes via its backstop. A bare, un-owned
	// Pod is left intact as an inspectable record (nothing would recreate it).
	if reaped, err := r.reapTerminalPod(ctx, &pod); err != nil || reaped {
		return ctrl.Result{}, err
	}

	// Only act on a Pod still waiting on our gate. A Pod that is unlabeled,
	// already ungated, already bound, or being deleted is not ours to place.
	if !needsPlacement(&pod) {
		return ctrl.Result{}, nil
	}

	pool, err := r.poolFor(ctx, &pod)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pool == nil {
		// No pool to place against. Leave the Pod gated rather than guessing; an
		// operator sees a SchedulingGated Pod and a missing/mislabeled pool.
		log.Info("no NodePool resolved for Pod; leaving it gated", "pod", pod.Name)
		return ctrl.Result{}, nil
	}

	placement, ok, retryAfter := r.selectPlacement(ctx, &pod, pool)
	if !ok {
		// No provider in the pool can serve this Pod's GPU type right now. Leave it
		// gated; a later reconcile (pool edit, provider registered) can place it.
		// When the only thing standing in the way is a failover block, requeue for
		// the moment it lapses — blocklist TTL expiry emits no event, so without this
		// the Pod would idle until the periodic resync (hours) even though a servable
		// candidate frees in minutes.
		if retryAfter > 0 {
			// Spread the requeue past the shared expiry by a stable per-Pod offset so
			// Pods contending for the same (scope-keyed) block don't all wake and retry
			// in lockstep, thundering the same tight pool the instant it frees.
			retryAfter += requeueJitter(pod.UID)
			log.Info("all servable candidates are blocked; requeuing for the soonest to free",
				"pod", pod.Name, "pool", pool.Name, "retryAfter", retryAfter.String())
			return ctrl.Result{RequeueAfter: retryAfter}, nil
		}
		log.Info("no provider in pool can serve the Pod; leaving it gated",
			"pod", pod.Name, "pool", pool.Name)
		return ctrl.Result{}, nil
	}

	// Create the NodeClaim BEFORE ungating: the claim is the durable teardown
	// ledger, so it must exist before the Pod can bind and provision. Idempotent
	// on a fixed, Pod-derived name, so a retry after a crash between create and
	// ungate adopts the existing claim rather than making a second.
	ready, err := r.ensureClaim(ctx, &pod, pool, placement)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !ready {
		// A stale claim for a prior same-named Pod still exists. Do NOT ungate
		// against the wrong ledger; wait for the NodeClaim backstop to reap it,
		// then a requeue re-creates the claim with this Pod's UID.
		log.Info("waiting for a stale NodeClaim to be reclaimed before placing",
			"pod", pod.Name, "claim", util.ClaimName(pod.Namespace, pod.Name))
		return ctrl.Result{RequeueAfter: staleClaimRequeue}, nil
	}

	// Stamp the routing decision and release the Pod to the scheduler.
	if err := r.place(ctx, &pod, pool, placement); err != nil {
		return ctrl.Result{}, err
	}
	log.Info("placed Pod", "pod", pod.Name, "provider", placement.provider,
		"capacityType", placement.capacityType, "region", placement.region)
	return ctrl.Result{}, nil
}

// placement is the resolved decision for one Pod.
type placement struct {
	provider     string
	capacityType nebulav1alpha1.CapacityType
	// region is the provider region to provision in (provider's own vocabulary).
	// Empty means the provider's configured default region; region-simple
	// providers leave it empty.
	region string
	// acceleratorID is the provider's own identifier for what serves this request
	// (e.g. AWS "p5.48xlarge"), resolved via MapAccelerator(type, count). Empty for
	// a CPU-only Pod, which requests no accelerator.
	acceleratorID string
}

// needsPlacement reports whether the Pod is an opted-in workload still held by
// our scheduling gate and not yet bound.
func needsPlacement(pod *corev1.Pod) bool {
	if !pod.DeletionTimestamp.IsZero() {
		return false
	}
	if pod.Labels[nebulav1alpha1.EnabledLabel] != "true" {
		return false
	}
	if pod.Spec.NodeName != "" {
		return false
	}
	return hasGateNamed(pod)
}

// hasGateNamed reports whether the Pod carries the provider-selection scheduling gate.
func hasGateNamed(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == nebulav1alpha1.ProviderSelectionGate {
			return true
		}
	}
	return false
}

// provider resolves the backend for name via the injected resolver, defaulting
// to the process-wide registry.
func (r *PodPlacementReconciler) provider(name string) (provider.Provider, bool) {
	if r.Providers != nil {
		return r.Providers(name)
	}
	return provider.Get(name)
}
