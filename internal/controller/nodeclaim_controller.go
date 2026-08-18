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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// placementGracePeriod is how long a claim whose served Pod has NEVER been
// observed waits before it is treated as genuinely orphaned and self-deletes.
// It guards against a cache-lag false-negative: the reconciler reads the Pod
// from the shared informer cache, which can briefly lag the API server right
// after the Pod is created, and a stale "absent" must not trigger teardown of a
// live instance.
//
// The window is small: a claim is always created AFTER its Pod, so the Pod already exists at
// the API server and only cache propagation is being waited on. A Pod watch re-enqueues the
// claim as soon as the cache catches up, so this is just the backstop for a missed event.
// Once an instance has been confirmed (Phase Bound), a later disappearance is trusted
// immediately — we KNOW the Pod existed and is now gone.
const placementGracePeriod = 15 * time.Second

// podReasonInitializing is the Pod status.Reason the virtual node stamps while the
// external instance EXISTS but is not yet reachable. It is the claim's evidence of
// existence for a Pod that is still Pending: Provisioning and "booting" share the
// Pending phase, and only the reason separates them — so this must be the SAME
// string the virtual node writes, which is why it aliases the shared API constant
// rather than repeating the literal.
const podReasonInitializing = nebulav1alpha1.PodReasonInitializing

// NodeClaimReconciler reconciles a NodeClaim object.
//
// Ownership model: the virtual kubelet owns the instance lifecycle (provision on CreatePod,
// terminate on DeletePod), and workload status is the POD's job. The claim exists for one
// reason: to be a durable, cluster-scoped TEARDOWN BACKSTOP.
//
// Why a backstop is needed: VK's teardown is edge-triggered and its tracking in-memory, so a
// Pod force-deleted while that provider's VK is down leaks a paid instance — on restart VK
// sees no Pod and no delete event, so it never calls Terminate. The claim is cluster-scoped,
// so it outlives the namespaced Pod and is reconciled level-triggered on every restart. When
// the served Pod is gone it self-deletes, and its finalizer reclaims the instance (resolve
// provider → find by claim name via List → Terminate) independent of VK liveness.
//
// Self-delete is guarded by placementGracePeriod, but only for a claim that has never
// observed its Pod, so cache lag never tears down a live workload.
type NodeClaimReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Providers resolves a provider name (NodeClaim.spec.provider) to its
	// backend, used ONLY on the teardown path. Defaults to the process-wide
	// registry; overridable in tests.
	Providers func(name string) (provider.Provider, bool)
}

// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodeclaims,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodeclaims/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodeclaims/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

// Reconcile drives the claim's own lifecycle: hold the terminate finalizer,
// track whether the served Pod exists, self-delete when it is gone (guarded
// against cache lag), and run the teardown backstop on the deletion path. It
// does NOT mirror the Pod's workload status — the Pod is the source of truth for
// that.
func (r *NodeClaimReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var nc nebulav1alpha1.NodeClaim
	if err := r.Get(ctx, req.NamespacedName, &nc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Deletion path: run the teardown backstop before the finalizer is released.
	if !nc.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &nc)
	}

	// Ensure the terminate finalizer is present before the claim is treated as
	// placed, so a delete that races provisioning still triggers teardown.
	if !controllerutil.ContainsFinalizer(&nc, nebulav1alpha1.TerminateInstanceFinalizer) {
		controllerutil.AddFinalizer(&nc, nebulav1alpha1.TerminateInstanceFinalizer)
		if err := r.Update(ctx, &nc); err != nil {
			return ctrl.Result{}, err
		}
		// Requeue via the fresh watch event from our own update.
		return ctrl.Result{}, nil
	}

	pod, err := r.servedPod(ctx, &nc)
	if err != nil {
		return ctrl.Result{}, err
	}

	if pod != nil {
		// The workload is present. We do not mirror the Pod's fine-grained runtime
		// status, but we DO reflect the coarse phase that matters to the ledger.
		// markPhase maps the Pod onto that phase (Terminated / Terminating / Bound /
		// Provisioning, see desiredPhase) and persists it, together with the instance id
		// the Pod carries, idempotently.
		return ctrl.Result{}, r.markPhase(ctx, &nc, pod)
	}

	// The served Pod is absent. Decide whether this is a real teardown or a
	// transient cache-lag false-negative.
	if nc.Status.Phase != "" {
		// Any phase proves this controller already saw the Pod alive: markPhase is the only
		// writer of status.phase and runs only from the pod != nil branch above, while
		// ensureClaim creates the claim with no status. So this is a real teardown — delete
		// the claim so its finalizer fires the backstop. Provisioning counts too: waiting
		// on it delayed every workload deleted while its instance was still booting.
		log.Info("served Pod is gone after being observed placed; deleting claim to trigger teardown",
			"pod", nc.Spec.PodRef.Name, "phase", nc.Status.Phase)
		return ctrl.Result{}, r.deleteSelf(ctx, &nc)
	}

	// No phase yet, so the claim has no evidence of its own and the cached read is all
	// there is. Since the claim is always created after its Pod, the Pod exists at the API
	// server; an "absent" this early is almost certainly the informer cache lagging, not a
	// real teardown. Wait out the short grace window (a Pod watch re-enqueues us as soon
	// as the cache catches up) rather than tearing down a live instance.
	if age := time.Since(nc.CreationTimestamp.Time); age < placementGracePeriod {
		return ctrl.Result{RequeueAfter: placementGracePeriod - age}, nil
	}

	// Grace elapsed and the Pod never appeared: nothing was ever provisioned for
	// this claim. Delete it; the backstop List finds no instance and the finalizer
	// releases cleanly.
	log.Info("served Pod never appeared within grace period; deleting orphaned claim",
		"pod", nc.Spec.PodRef.Name)
	return ctrl.Result{}, r.deleteSelf(ctx, &nc)
}

// reconcileDelete is the teardown backstop. It guarantees the external instance
// is reclaimed before the finalizer is released, independent of whether VK ever
// processed the Pod deletion.
//
// In the HAPPY PATH this is redundant work: VK's DeletePod already called
// provider.Terminate on the instance (see pkg/vnode Handler.DeletePod), so by
// the time we get here the instance is usually already gone and the Terminate
// below is a no-op — which is exactly why the provider contract requires
// Terminate to be idempotent. The finalizer earns its keep only when DeletePod
// never ran: the Pod was force-deleted while this provider's VK was down (VK
// misses the delete event), or VK crashed mid-DeletePod. In those cases nothing
// else will ever terminate the instance, and this backstop is the only path that
// reclaims it — which is why teardown lives here and not solely in VK.
func (r *NodeClaimReconciler) reconcileDelete(ctx context.Context, nc *nebulav1alpha1.NodeClaim) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(nc, nebulav1alpha1.TerminateInstanceFinalizer) {
		return ctrl.Result{}, nil // already cleaned up
	}

	prov, ok := r.provider(nc.Spec.Provider)
	if !ok {
		// No adapter registered: we cannot call Terminate. Do NOT wedge deletion
		// forever on a provider we can't reach — drop the finalizer. A manager
		// restart with the adapter registered is the operator's recovery path if
		// an instance is later found orphaned.
		log.Info("provider not registered while deleting; releasing finalizer to avoid a stuck claim",
			"provider", nc.Spec.Provider)
		return r.releaseFinalizer(ctx, nc)
	}

	// Find the instance by its Pod-derived claim name. We cannot rely on
	// status.InstanceID: VK tracks the id in memory, so if VK died (the very case
	// this backstop exists for) that id was never persisted anywhere we can read.
	// Re-deriving the claim name from PodRef and matching it against List is the
	// only way to reclaim an instance VK has forgotten about. If DeletePod already
	// ran, List simply won't contain it and findInstanceID returns "" (a no-op
	// Terminate).
	claim := util.ClaimName(nc.Spec.PodRef.Namespace, nc.Spec.PodRef.Name)
	id, err := r.findInstanceID(ctx, prov, claim, nc.Status.InstanceID)
	if err != nil {
		log.Error(err, "listing provider instances for teardown; will retry")
		return ctrl.Result{}, err
	}

	// Terminate is idempotent (terminating an already-gone or empty instance
	// returns nil per the provider contract). In the happy path VK's DeletePod
	// already terminated this instance, so this is a redundant no-op; the call is
	// only load-bearing when DeletePod never ran. Idempotency also makes retries
	// after a transient error safe.
	if err := prov.Terminate(ctx, id); err != nil {
		log.Error(err, "terminate failed; will retry", "instanceID", id)
		return ctrl.Result{}, err
	}
	if id != "" {
		log.Info("instance terminated by backstop", "instanceID", id, "provider", prov.Name())
	}

	return r.releaseFinalizer(ctx, nc)
}

// findInstanceID resolves the provider instance id to terminate. It prefers a
// recorded status.InstanceID, then falls back to matching the Pod-derived claim
// name against provider.List(). Returns "" when no instance exists (already gone
// or never provisioned), which Terminate treats as a no-op.
func (r *NodeClaimReconciler) findInstanceID(ctx context.Context, prov provider.Provider, claim, recordedID string) (string, error) {
	if recordedID != "" {
		return recordedID, nil
	}
	instances, err := prov.List(ctx)
	if err != nil {
		return "", err
	}
	for _, inst := range instances {
		if inst.ClaimName == claim {
			return inst.ID, nil
		}
	}
	return "", nil // no live instance for this claim
}

// releaseFinalizer removes the terminate finalizer, allowing the API server to
// delete the NodeClaim.
func (r *NodeClaimReconciler) releaseFinalizer(ctx context.Context, nc *nebulav1alpha1.NodeClaim) (ctrl.Result, error) {
	if controllerutil.RemoveFinalizer(nc, nebulav1alpha1.TerminateInstanceFinalizer) {
		if err := r.Update(ctx, nc); err != nil {
			return ctrl.Result{}, err
		}
	}
	return ctrl.Result{}, nil
}

// provider resolves the backend for name via the injected resolver, defaulting
// to the process-wide registry.
func (r *NodeClaimReconciler) provider(name string) (provider.Provider, bool) {
	if r.Providers != nil {
		return r.Providers(name)
	}
	return provider.Get(name)
}

// desiredPhase maps a present served Pod onto the claim phase to record. The
// claim does NOT mirror the Pod's fine-grained runtime status — it tracks only
// the coarse lifecycle its teardown job needs:
//
//   - Terminal Pod (Failed/Succeeded) => Terminated. A restartPolicy:Never Pod lingers
//     terminal rather than being deleted, so keying off existence alone would wedge the
//     claim at Bound.
//   - Pod being deleted => Terminating. A forward transition from ANY phase, checked before
//     the Bound-hold so a deleting Pod is not stranded on its prior phase. Terminal wins:
//     an already-gone instance is Terminated, not merely terminating.
//   - Bound already => "" (hold). Bound is the durable teardown guard and must never be
//     stripped, or a later disappearance would read as cache lag and skip teardown of a
//     live instance. "" holds the phase while markPhase still refreshes the id.
//   - An instance EXISTS => Bound. Both Running and Pending/Initializing prove existence,
//     and the claim's question is existence, not readiness: a booting GPU box is just as
//     billable as a serving one, so if its Pod vanishes it is reclaimed with no grace.
//   - Otherwise => Provisioning. No capacity allocated yet (the call may be in flight, or
//     returned an unreserved id), so a vanished Pod may be cache lag and grace applies. An
//     unreserved instance does exist, so this understates it for one tick; the grace path
//     still reclaims by asking the provider what exists.
func (r *NodeClaimReconciler) desiredPhase(nc *nebulav1alpha1.NodeClaim, pod *corev1.Pod) nebulav1alpha1.NodeClaimPhase {
	switch {
	case isTerminal(pod.Status.Phase):
		return nebulav1alpha1.NodeClaimTerminated
	case !pod.DeletionTimestamp.IsZero():
		return nebulav1alpha1.NodeClaimTerminating
	case r.wasBound(nc):
		return "" // hold Bound (or Terminated); never downgrade
	case pod.Status.Phase == corev1.PodRunning,
		pod.Status.Reason == podReasonInitializing:
		return nebulav1alpha1.NodeClaimBound
	default:
		return nebulav1alpha1.NodeClaimProvisioning
	}
}

// markPhase reflects the served Pod into the claim's status: the coarse phase
// desiredPhase maps it to, plus best-effort status.InstanceID, in one idempotent
// write. An empty desiredPhase means "leave the phase as-is" (the Bound-flap hold);
// the id is still refreshed. The provider id is the one datum that must not be lost —
// the teardown backstop reclaims the instance by it even if VK (which otherwise holds
// it only in memory) has died — so it is captured as soon as the Pod carries it,
// regardless of phase. A no-op (phase unchanged and id already set) writes nothing.
func (r *NodeClaimReconciler) markPhase(ctx context.Context, nc *nebulav1alpha1.NodeClaim, pod *corev1.Pod) error {
	changed := false
	if phase := r.desiredPhase(nc, pod); phase != "" && nc.Status.Phase != phase {
		nc.Status.Phase = phase
		changed = true
	}
	if recordInstanceID(nc, pod) {
		changed = true
	}
	if !changed {
		return nil
	}
	return r.patchStatus(ctx, nc)
}

// recordInstanceID copies the instance id off the served Pod into status.InstanceID when it
// is not already set, returning whether it mutated the claim.
//
// The id comes from the Pod the caller already holds, not from the provider: VK stamps it
// the moment Provision returns (see InstanceIDAnnotation). Asking the provider instead
// meant listing every instance and matching on claim name on EVERY reconcile until the id
// resolved — a remote API call per claim per reconcile, into APIs that rate limit.
//
// Best-effort: an absent annotation means the Pod has not been provisioned yet, or the
// endpoint patch has not landed, so this is a no-op and the Pod watch brings us back when
// it does. Even if the id never arrives, the teardown backstop re-derives it by claim name.
func recordInstanceID(nc *nebulav1alpha1.NodeClaim, pod *corev1.Pod) bool {
	if nc.Status.InstanceID != "" || pod == nil {
		return false
	}
	id := pod.Annotations[nebulav1alpha1.InstanceIDAnnotation]
	if id == "" {
		return false
	}
	nc.Status.InstanceID = id
	return true
}

// wasBound reports whether an external instance has ever been confirmed to exist
// for this claim — the fact the teardown backstop keys off. A Terminated claim was
// necessarily Bound first, so it also counts.
func (r *NodeClaimReconciler) wasBound(nc *nebulav1alpha1.NodeClaim) bool {
	return nc.Status.Phase == nebulav1alpha1.NodeClaimBound ||
		nc.Status.Phase == nebulav1alpha1.NodeClaimTerminated
}

// isTerminal reports whether a Pod phase is an end state that will not progress.
func isTerminal(phase corev1.PodPhase) bool {
	return phase == corev1.PodFailed || phase == corev1.PodSucceeded
}

// deleteSelf deletes the claim, which triggers reconcileDelete (the backstop)
// via the deletion timestamp + finalizer. Idempotent: a NotFound is treated as
// success.
func (r *NodeClaimReconciler) deleteSelf(ctx context.Context, nc *nebulav1alpha1.NodeClaim) error {
	if err := r.Delete(ctx, nc); err != nil {
		return client.IgnoreNotFound(err)
	}
	return nil
}

// servedPod fetches the Pod named by spec.PodRef, enforcing the UID pin: a
// recreated Pod of the same name (different UID) is treated as gone, so this
// claim is not silently re-attached to a different workload. Returns (nil, nil)
// when the Pod is absent or the UID no longer matches.
func (r *NodeClaimReconciler) servedPod(ctx context.Context, nc *nebulav1alpha1.NodeClaim) (*corev1.Pod, error) {
	var pod corev1.Pod
	key := types.NamespacedName{Namespace: nc.Spec.PodRef.Namespace, Name: nc.Spec.PodRef.Name}
	if err := r.Get(ctx, key, &pod); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	if nc.Spec.PodRef.UID != "" && string(pod.UID) != nc.Spec.PodRef.UID {
		return nil, nil // same name, different object => the pinned Pod is gone
	}
	return &pod, nil
}

// patchStatus writes the status subresource.
func (r *NodeClaimReconciler) patchStatus(ctx context.Context, nc *nebulav1alpha1.NodeClaim) error {
	return r.Status().Update(ctx, nc)
}

// SetupWithManager wires the controller. It watches NodeClaims and also watches
// Pods (mapping a Pod back to the claim that references it) so that a Pod's
// appearance promptly marks the claim Bound and its deletion promptly triggers
// the self-delete/teardown path — rather than waiting for the grace-period
// requeue.
func (r *NodeClaimReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NodeClaim{}).
		Watches(&corev1.Pod{}, handler.EnqueueRequestsFromMapFunc(r.claimsForPod)).
		Named("nodeclaim").
		// One claim per Pod, and the teardown path is the expensive one: per claim a
		// self-delete, a provider call to reclaim the instance, and a finalizer update,
		// each a round trip. Serialized, that made drain the slowest stage of the
		// benchmark by a wide margin (see concurrentReconciles).
		WithOptions(controller.Options{MaxConcurrentReconciles: concurrentReconciles}).
		Complete(r)
}

// claimsForPod maps a Pod event to the claim that serves it. The name is DERIVED rather
// than searched for: util.ClaimName is what ensureClaim named the claim, so the same
// namespace/name yields the same token, truncate-and-hash case included.
//
// This used to List every NodeClaim and filter on PodRef, which cost a full cluster-wide
// list plus a deep copy per claim on EVERY Pod event — and the poll loop re-emits every
// tracked pod each tick, so at 500 replicas that was 500 events × 500 copies per tick,
// all of it in this event-handler goroutine, ahead of the workqueue where extra workers
// cannot help. Deriving the name is O(1).
//
// Reconcile still verifies PodRef and the UID pin, so enqueueing a claim that turns out
// to name a different Pod incarnation is harmless.
func (r *NodeClaimReconciler) claimsForPod(_ context.Context, obj client.Object) []reconcile.Request {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{Name: util.ClaimName(pod.Namespace, pod.Name)},
	}}
}
