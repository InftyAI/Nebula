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
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// sandboxContainerName is the name of the single container in a sandbox's Pod.
// It is fixed rather than user-settable because it is what `kubectl exec` and
// `kubectl logs` default to when no -c is given: a predictable name is the
// difference between `kubectl exec sbx-alice -- bash` working and the user having
// to look up a container name first.
const sandboxContainerName = "sandbox"

// SandboxReconciler reconciles a Sandbox: it synthesizes the one Pod that backs
// the box, projects that Pod's status back onto the Sandbox, and enforces TTL.
//
// It deliberately does NOT talk to any provider. The Pod is the carrier for
// everything already built — the provider-selection gate, placement, the
// NodeClaim teardown ledger with its finalizer, ResourceQuota accounting — so
// this controller's whole job is to produce a correctly-shaped Pod and get out of
// the way. That is also why a Sandbox is not a bespoke provisioning path: bypassing
// the Pod would mean reimplementing the guarantee that a paid GPU is never leaked.
type SandboxReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxes,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxes/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxes/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete

// Reconcile drives one Sandbox: ensure its Pod exists (unless the box is done),
// mirror the Pod's state into status, and release the instance when TTL elapses.
func (r *SandboxReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var sbx nebulav1alpha1.Sandbox
	if err := r.Get(ctx, req.NamespacedName, &sbx); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !sbx.DeletionTimestamp.IsZero() {
		// The Pod is ownerRef'd, so garbage collection deletes it, which triggers the
		// virtual kubelet's teardown and the NodeClaim finalizer behind it. There is
		// nothing for us to clean up, so we hold no finalizer of our own — one here
		// would only add a way for the box to get stuck undeletable.
		return ctrl.Result{}, nil
	}

	// A terminal Sandbox keeps no instance. Its Pod has already been released, and
	// recreating one would silently hand the user a DIFFERENT box (empty filesystem,
	// new endpoint) under the same name, so we stop here and leave the object as the
	// record of what happened.
	if isTerminalSandboxPhase(sbx.Status.Phase) {
		return ctrl.Result{}, nil
	}

	pod, err := r.ensurePod(ctx, &sbx)
	if err != nil {
		return ctrl.Result{}, err
	}
	if pod == nil {
		// A foreign Pod holds the name we need. Surface it and stop; this needs a human
		// (rename the sandbox, or remove the squatter) and retrying cannot fix it.
		log.Info("a Pod of this name exists but is not owned by this Sandbox",
			"sandbox", sbx.Name, "pod", sbx.Name)
		return ctrl.Result{}, r.setStatus(ctx, &sbx, sandboxStatus{
			phase:  nebulav1alpha1.SandboxPending,
			reason: nebulav1alpha1.ReasonPodConflict,
			msg:    "a Pod named " + sbx.Name + " already exists and is not owned by this Sandbox",
		})
	}

	// TTL is measured from the moment the box became READY, not from creation, so a
	// slow provision does not eat into the user's time. That means the deadline only
	// exists once ReadyTime is set, and expiry is checked before status is refreshed
	// so an expiring box reports Expired rather than briefly re-reporting Ready.
	if expired, err := r.enforceTTL(ctx, &sbx, pod); err != nil || expired {
		return ctrl.Result{}, err
	}

	st := sandboxStatusFromPod(pod)
	if err := r.setStatus(ctx, &sbx, st); err != nil {
		return ctrl.Result{}, err
	}

	// Requeue for the exact moment TTL elapses. Nothing emits an event when a
	// deadline passes, so without this the box would bill until the next periodic
	// resync — the failure mode TTL exists to prevent.
	if until, ok := timeUntilExpiry(&sbx); ok {
		return ctrl.Result{RequeueAfter: until}, nil
	}
	return ctrl.Result{}, nil
}

// ensurePod creates the Sandbox's Pod if absent and returns it. It returns
// (nil, nil) when a Pod of the required name exists but is NOT owned by this
// Sandbox: adoption is refused because the Pod could be an unrelated workload,
// and adopting it would subject someone else's Pod to this Sandbox's lifecycle
// (including deletion on TTL expiry). The caller surfaces that as a condition.
func (r *SandboxReconciler) ensurePod(ctx context.Context, sbx *nebulav1alpha1.Sandbox) (*corev1.Pod, error) {
	var existing corev1.Pod
	err := r.Get(ctx, client.ObjectKey{Namespace: sbx.Namespace, Name: sbx.Name}, &existing)
	if err == nil {
		if !isOwnedBy(&existing, sbx) {
			return nil, nil
		}
		return &existing, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, err
	}

	pod := r.buildPod(sbx)
	if err := controllerutil.SetControllerReference(sbx, pod, r.Scheme); err != nil {
		return nil, err
	}
	if err := r.Create(ctx, pod); err != nil {
		if apierrors.IsAlreadyExists(err) {
			// Lost a race with another reconcile (or with a foreign creator). Re-read on
			// the next pass rather than guessing which it was.
			return nil, nil
		}
		return nil, err
	}
	return pod, nil
}

// buildPod synthesizes the Pod that backs the sandbox. Everything Nebula's
// existing placement path requires is stamped here, so a Sandbox's Pod is
// indistinguishable from a correctly hand-written one — the same webhook gates it,
// the same placement controller places it, the same virtual kubelet provisions it.
func (r *SandboxReconciler) buildPod(sbx *nebulav1alpha1.Sandbox) *corev1.Pod {
	labels := map[string]string{
		// EnabledLabel is the opt-in the mutating webhook selects on: without it the
		// Pod would be scheduled by vanilla Kubernetes and never reach a provider.
		nebulav1alpha1.EnabledLabel:   nebulav1alpha1.EnabledValue,
		nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
		nebulav1alpha1.PoolLabel:      sbx.Spec.NodePoolRef,
		nebulav1alpha1.SandboxLabel:   sbx.Name,
	}
	if sbx.Spec.AcceleratorType != "" {
		// The TYPE is a label and the COUNT is an nvidia.com/gpu resource — the split
		// the rest of the system already reads (see util.AcceleratorRequest). A
		// CPU-only sandbox sets neither.
		labels[nebulav1alpha1.AcceleratorTypeLabel] = sbx.Spec.AcceleratorType
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			// Same name as the Sandbox, so `kubectl exec sbx-alice` and
			// `kubectl logs sbx-alice` work with the name the user already knows. The
			// name is still published in status.PodName so tooling reads it from there
			// rather than depending on this being true forever.
			Name:      sbx.Name,
			Namespace: sbx.Namespace,
			Labels:    labels,
		},
		Spec: corev1.PodSpec{
			// Never restart: the provider owns the instance lifecycle, and a sandbox
			// whose instance is gone must surface as Failed rather than being silently
			// replaced by an empty box wearing the same name.
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name:  sandboxContainerName,
				Image: sbx.Spec.Image,
				// Set HERE rather than in each provider's bootstrap, so the Pod stays the
				// single source of truth for what runs on the instance. Every adapter already
				// reads the command off the Pod, so this needs no per-provider code.
				//
				// A sandbox has nothing to run at boot — the box exists to be connected to —
				// so the command's only job is to not exit. Without it the container runs the
				// image's own entrypoint (`bash` for ubuntu:24.04), which finds no TTY, exits,
				// and takes the instance down: the box would surface as Failed seconds after
				// being provisioned.
				//
				// A PLACEHOLDER process, not a control surface. `kubectl exec` and
				// `kubectl logs` do not go through it: the provider starts the command
				// itself, so a bare `sleep` is enough and no binary has to be injected
				// into an arbitrary user image.
				//
				// No tension with a user-supplied command: SandboxSpec has no command field.
				Command:   []string{"sleep", "infinity"},
				Resources: sbx.Spec.Resources,
				Env:       sbx.Spec.Env,
			}},
		},
	}
}

// sandboxStatus is the projection this controller writes, gathered in one struct
// so status is set through a single path (and thus a single Status().Update).
type sandboxStatus struct {
	phase    nebulav1alpha1.SandboxPhase
	reason   string
	msg      string
	endpoint string
	// ready marks the Ready condition True. It is separate from phase because only
	// SandboxReady implies readiness, and conditions and phases are updated together.
	ready bool
}

// sandboxStatusFromPod projects the backing Pod's state onto the Sandbox. The Pod
// (via the virtual kubelet) is the source of truth for what the external instance
// is doing, so this reads it rather than tracking instance state independently —
// two sources for one fact is how they drift.
//
// It deliberately takes only the Pod, not the Sandbox: the projection must depend
// on nothing but observed Pod state, or a stale value already on Sandbox.Status
// could feed back into the next projection and latch.
//
// The mapping keys off the Pod's status REASON, not just its phase, because the
// interesting distinction for a user is inside PodPending: "cannot get capacity"
// (Provisioning) versus "capacity granted, still booting" (Initializing). The
// vnode stamps those reasons (see pkg/vnode/status.go).
func sandboxStatusFromPod(pod *corev1.Pod) sandboxStatus {
	endpoint := pod.Annotations[nebulav1alpha1.EndpointAnnotation]

	switch pod.Status.Phase {
	case corev1.PodRunning:
		if isPodReady(pod) {
			return sandboxStatus{
				phase:    nebulav1alpha1.SandboxReady,
				reason:   nebulav1alpha1.ReasonSandboxReady,
				msg:      "the sandbox instance is running and reachable",
				endpoint: endpoint,
				ready:    true,
			}
		}
		return sandboxStatus{
			phase:    nebulav1alpha1.SandboxInitializing,
			reason:   nebulav1alpha1.ReasonSandboxProvisioning,
			msg:      "the sandbox instance is running but not yet ready",
			endpoint: endpoint,
		}
	case corev1.PodFailed, corev1.PodSucceeded:
		// Succeeded lands here too: a sandbox has no notion of completing — its command
		// only exits when the box goes away — so a terminal Pod means the instance is
		// gone either way.
		return sandboxStatus{
			phase:    nebulav1alpha1.SandboxFailed,
			reason:   nebulav1alpha1.ReasonSandboxFailed,
			msg:      podFailureMessage(pod),
			endpoint: endpoint,
		}
	default:
		// Pending. A Pod still held by the provider-selection gate has not been placed
		// at all, which is a different problem from a provision in flight: it means no
		// provider in the pool can serve this box right now. Distinguishing them is
		// what stops a capacity problem from looking like a slow boot.
		if hasGateNamed(pod) {
			return sandboxStatus{
				phase:  nebulav1alpha1.SandboxPending,
				reason: nebulav1alpha1.ReasonSandboxProvisioning,
				msg:    "waiting for placement onto a provider",
			}
		}
		phase := nebulav1alpha1.SandboxProvisioning
		if pod.Status.Reason == podReasonInitializing {
			phase = nebulav1alpha1.SandboxInitializing
		}
		return sandboxStatus{
			phase:    phase,
			reason:   nebulav1alpha1.ReasonSandboxProvisioning,
			msg:      podStatusMessage(pod, "bringing the sandbox instance up"),
			endpoint: endpoint,
		}
	}
}

// setStatus writes the projection, stamping ReadyTime/ExpiryTime on the first
// transition to Ready. It skips the API call when nothing changed, so a Sandbox
// that is simply sitting Ready does not generate an update per resync.
func (r *SandboxReconciler) setStatus(ctx context.Context, sbx *nebulav1alpha1.Sandbox, st sandboxStatus) error {
	before := sbx.Status.DeepCopy()

	sbx.Status.Phase = st.phase
	sbx.Status.PodName = sbx.Name
	if st.endpoint != "" {
		sbx.Status.Endpoint = st.endpoint
	}

	// ReadyTime is durable and written exactly once: it anchors TTL, so recomputing
	// it from the Pod would let a status blip silently restart the user's clock.
	if st.phase == nebulav1alpha1.SandboxReady && sbx.Status.ReadyTime == nil {
		now := metav1.Now()
		sbx.Status.ReadyTime = &now
		if ttl := sbx.Spec.TTL; ttl != nil && ttl.Duration > 0 {
			expiry := metav1.NewTime(now.Add(ttl.Duration))
			sbx.Status.ExpiryTime = &expiry
		}
	}

	condStatus := metav1.ConditionFalse
	if st.ready {
		condStatus = metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&sbx.Status.Conditions, metav1.Condition{
		Type:               nebulav1alpha1.SandboxConditionReady,
		Status:             condStatus,
		Reason:             st.reason,
		Message:            st.msg,
		ObservedGeneration: sbx.Generation,
	})

	// Skip the write when only the condition's LastTransitionTime would differ —
	// SetStatusCondition preserves it when Status is unchanged, so a semantic compare
	// is enough to keep a steady-state Ready sandbox from generating an update per
	// resync (each of which would wake every watcher).
	if apiequality.Semantic.DeepEqual(before, &sbx.Status) {
		return nil
	}
	return r.Status().Update(ctx, sbx)
}

// enforceTTL releases the instance when the box's deadline has passed, by
// deleting the backing Pod — which is what triggers the virtual kubelet's
// teardown and, behind it, the NodeClaim finalizer that guarantees the paid
// instance is actually reclaimed. Deleting the Pod (rather than the Sandbox)
// leaves the object as the record of why the box went away, so a user returning to
// an expired sandbox gets an answer instead of a NotFound.
//
// It returns expired=true when it acted, so the caller stops reconciling this pass.
func (r *SandboxReconciler) enforceTTL(ctx context.Context, sbx *nebulav1alpha1.Sandbox, pod *corev1.Pod) (bool, error) {
	if sbx.Status.ExpiryTime == nil || time.Now().Before(sbx.Status.ExpiryTime.Time) {
		return false, nil
	}

	// UID-pinned so a Pod already replaced by something else is never clobbered;
	// an already-gone Pod is success, not an error.
	preconditions := metav1.Preconditions{UID: &pod.UID}
	if err := r.Delete(ctx, pod, &client.DeleteOptions{Preconditions: &preconditions}); err != nil {
		if !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			return false, err
		}
	}
	return true, r.setStatus(ctx, sbx, sandboxStatus{
		phase:  nebulav1alpha1.SandboxExpired,
		reason: nebulav1alpha1.ReasonSandboxExpired,
		msg:    "ttl elapsed; the sandbox instance was released",
	})
}

// timeUntilExpiry reports how long until the box's TTL elapses, and whether
// there is a deadline at all. A deadline already in the past returns a small
// positive delay rather than zero, because a zero RequeueAfter means "do not
// requeue" to controller-runtime — precisely the wrong reading for an overdue box.
func timeUntilExpiry(sbx *nebulav1alpha1.Sandbox) (time.Duration, bool) {
	if sbx.Status.ExpiryTime == nil {
		return 0, false
	}
	until := time.Until(sbx.Status.ExpiryTime.Time)
	if until <= 0 {
		return time.Second, true
	}
	return until, true
}

// isTerminalSandboxPhase reports whether the box is done and holds no instance.
func isTerminalSandboxPhase(p nebulav1alpha1.SandboxPhase) bool {
	return p == nebulav1alpha1.SandboxExpired || p == nebulav1alpha1.SandboxFailed
}

// isOwnedBy reports whether obj is controlled by the given Sandbox, matching on
// UID so a recreated Sandbox of the same name does not adopt the old box's Pod.
func isOwnedBy(obj client.Object, sbx *nebulav1alpha1.Sandbox) bool {
	ref := metav1.GetControllerOf(obj)
	return ref != nil && ref.UID == sbx.UID
}

// isPodReady reports whether the Pod's Ready condition is True. The virtual
// kubelet sets it when the provider reports the instance running and reachable.
func isPodReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// podFailureMessage explains why the box died, preferring the Pod's own message
// (the vnode writes a specific one: provision rejected, instance gone, instance
// failed) over a generic fallback.
func podFailureMessage(pod *corev1.Pod) string {
	return podStatusMessage(pod, "the sandbox instance is no longer running")
}

// podStatusMessage returns the Pod's status message, or fallback when it has none.
func podStatusMessage(pod *corev1.Pod, fallback string) string {
	if pod.Status.Message != "" {
		return pod.Status.Message
	}
	return fallback
}

// SetupWithManager wires the controller. It owns its Pod, so a Pod status change
// (the instance coming up, failing, or vanishing) re-reconciles the Sandbox
// immediately instead of waiting for the periodic resync.
func (r *SandboxReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.Sandbox{}).
		Owns(&corev1.Pod{}).
		Named("sandbox").
		Complete(r)
}
