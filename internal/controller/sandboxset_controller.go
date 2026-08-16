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
	"sort"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// SandboxSetReconciler reconciles a SandboxSet: it creates and deletes Sandbox
// objects so that spec.Replicas of them exist, and rolls their readiness up into
// the set's status.
//
// It knows nothing about Pods, instances, or providers — its whole vocabulary is Sandbox
// objects, and the Sandbox controller handles what one box means. That split keeps scaling
// logic and box lifecycle from tangling.
//
// A template change does NOT roll existing boxes: this mirrors ReplicaSet, not Deployment,
// because a rolling update would evict live sessions and burn minutes of provisioning per
// box for a change nobody attached to a running sandbox asked for. New boxes get the new
// template; existing ones are left alone.
type SandboxSetReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxsets/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxsets/finalizers,verbs=update
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxsets/scale,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=sandboxes,verbs=get;list;watch;create;update;patch;delete

// Reconcile brings the owned Sandbox count to spec.Replicas and refreshes status.
func (r *SandboxSetReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var set nebulav1alpha1.SandboxSet
	if err := r.Get(ctx, req.NamespacedName, &set); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !set.DeletionTimestamp.IsZero() {
		// Owned Sandboxes are ownerRef'd, so garbage collection removes them and each
		// Sandbox controller releases its own instance. Nothing to do here, and no
		// finalizer to hold — one would only be a way to get stuck.
		return ctrl.Result{}, nil
	}

	owned, err := r.ownedSandboxes(ctx, &set)
	if err != nil {
		return ctrl.Result{}, err
	}

	// Prune boxes that are dead but still counting toward the total. Without this a
	// set of 3 that loses one instance sits at "3 replicas, 2 usable" forever: the
	// Sandbox controller will not resurrect a terminal box (a fresh instance would be
	// a different box wearing the same name), so the only way back to 3 usable boxes
	// is for the SET to replace it. Deleting here makes the next branch see the
	// shortfall and create a replacement on this same pass.
	if pruned, err := r.pruneTerminal(ctx, owned); err != nil {
		return ctrl.Result{}, err
	} else if pruned > 0 {
		log.Info("pruned terminal sandboxes for replacement",
			"sandboxset", set.Name, "count", pruned)
		if owned, err = r.ownedSandboxes(ctx, &set); err != nil {
			return ctrl.Result{}, err
		}
	}

	switch diff := int(set.Spec.Replicas) - len(owned); {
	case diff > 0:
		if err := r.scaleUp(ctx, &set, diff); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("created sandboxes", "sandboxset", set.Name, "count", diff)
	case diff < 0:
		victims := selectForRemoval(owned, -diff)
		if err := r.scaleDown(ctx, victims); err != nil {
			return ctrl.Result{}, err
		}
		log.Info("deleted sandboxes", "sandboxset", set.Name, "count", len(victims))
	}

	// Re-list after mutating so status reflects what actually exists rather than what
	// we intended. A create that was rejected (quota, admission) must not be reported
	// as a replica.
	if owned, err = r.ownedSandboxes(ctx, &set); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, r.setStatus(ctx, &set, owned)
}

// ownedSandboxes lists the Sandboxes this set owns, sorted by name so both the
// removal choice and the reported list are deterministic.
//
// It filters by ownerReference UID rather than trusting the label selector alone:
// the label is user-visible and could be applied to a foreign Sandbox, and acting
// on that would let anyone get a box they do not own deleted by our scale-in.
func (r *SandboxSetReconciler) ownedSandboxes(ctx context.Context, set *nebulav1alpha1.SandboxSet) ([]nebulav1alpha1.Sandbox, error) {
	var list nebulav1alpha1.SandboxList
	if err := r.List(ctx, &list,
		client.InNamespace(set.Namespace),
		client.MatchingLabels{nebulav1alpha1.SandboxSetLabel: set.Name},
	); err != nil {
		return nil, err
	}

	owned := make([]nebulav1alpha1.Sandbox, 0, len(list.Items))
	for i := range list.Items {
		sbx := list.Items[i]
		if ref := metav1.GetControllerOf(&sbx); ref == nil || ref.UID != set.UID {
			continue
		}
		if !sbx.DeletionTimestamp.IsZero() {
			continue // already going away; do not count it toward the desired total
		}
		owned = append(owned, sbx)
	}
	sort.Slice(owned, func(i, j int) bool { return owned[i].Name < owned[j].Name })
	return owned, nil
}

// scaleUp creates n new Sandboxes from the set's template.
func (r *SandboxSetReconciler) scaleUp(ctx context.Context, set *nebulav1alpha1.SandboxSet, n int) error {
	for range n {
		sbx := r.buildSandbox(set)
		if err := controllerutil.SetControllerReference(set, sbx, r.Scheme); err != nil {
			return err
		}
		if err := r.Create(ctx, sbx); err != nil {
			// Surface the first failure rather than pressing on: if creates are being
			// rejected (quota, admission), the next one fails the same way, and a partial
			// scale-up plus a real error is more useful than n identical errors.
			return err
		}
	}
	return nil
}

// buildSandbox stamps one Sandbox out of the template. The name is GENERATED
// (metadata.generateName) rather than an ordinal: an ordinal implies a slot that
// gets refilled, so a replacement box would wear a dead box's name — same address,
// different filesystem. A generated name makes a replacement visibly a new box.
func (r *SandboxSetReconciler) buildSandbox(set *nebulav1alpha1.SandboxSet) *nebulav1alpha1.Sandbox {
	labelSet := map[string]string{}
	for k, v := range set.Spec.Template.Metadata.Labels {
		labelSet[k] = v
	}
	// Applied last so a template cannot overwrite the ownership label the set
	// selects on — doing so would orphan the box from its own set.
	labelSet[nebulav1alpha1.SandboxSetLabel] = set.Name
	labelSet[nebulav1alpha1.ManagedByLabel] = nebulav1alpha1.ManagedByValue

	var annotations map[string]string
	if len(set.Spec.Template.Metadata.Annotations) > 0 {
		annotations = make(map[string]string, len(set.Spec.Template.Metadata.Annotations))
		for k, v := range set.Spec.Template.Metadata.Annotations {
			annotations[k] = v
		}
	}

	return &nebulav1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: set.Name + "-",
			Namespace:    set.Namespace,
			Labels:       labelSet,
			Annotations:  annotations,
		},
		Spec: *set.Spec.Template.Spec.DeepCopy(),
	}
}

// pruneTerminal deletes owned boxes that have reached a terminal phase, returning
// how many it removed. A terminal box holds no instance and will never come back —
// the Sandbox controller refuses to recreate one, deliberately, so that a user is
// never silently handed an empty box under the name of the one they were working in.
// Replacement is therefore the SET's job, and it starts with removing the corpse.
//
// This is also what makes TTL a recycle interval for a set and a hard deadline for a
// standalone Sandbox: same expiry, but here the set notices the shortfall and creates
// a fresh box.
func (r *SandboxSetReconciler) pruneTerminal(ctx context.Context, owned []nebulav1alpha1.Sandbox) (int, error) {
	var pruned int
	for i := range owned {
		sbx := &owned[i]
		if !isTerminalSandboxPhase(sbx.Status.Phase) {
			continue
		}
		preconditions := metav1.Preconditions{UID: &sbx.UID}
		if err := r.Delete(ctx, sbx, &client.DeleteOptions{Preconditions: &preconditions}); err != nil {
			if err = client.IgnoreNotFound(err); err != nil {
				return pruned, err
			}
		}
		pruned++
	}
	return pruned, nil
}

// scaleDown deletes the chosen boxes.
func (r *SandboxSetReconciler) scaleDown(ctx context.Context, victims []nebulav1alpha1.Sandbox) error {
	for i := range victims {
		v := &victims[i]
		// UID-pinned so a box already replaced by a same-named recreate is never
		// clobbered; an already-gone box is success.
		preconditions := metav1.Preconditions{UID: &v.UID}
		if err := r.Delete(ctx, v, &client.DeleteOptions{Preconditions: &preconditions}); err != nil {
			// An already-gone box is success, and must NOT end the loop: returning here
			// would abandon the remaining victims while reporting the scale-in as done,
			// leaving paid instances running.
			if err = client.IgnoreNotFound(err); err != nil {
				return err
			}
		}
	}
	return nil
}

// selectForRemoval picks which n boxes to delete on scale-in, cheapest-to-lose
// first. Scale-in has to name a victim, and the boxes are NOT interchangeable once
// someone is working in one, so the order is chosen to minimise destroyed work:
//
//  1. Terminal boxes — already worthless. pruneTerminal normally removes these
//     before we get here, so this rank is a safety net for a box that turned
//     terminal between the prune and this call.
//  2. Not-yet-Ready boxes — nobody can have been using a box that was never
//     reachable. Youngest first, so the box closest to becoming useful survives.
//  3. Ready boxes — youngest first, on the reasoning that the most recently created
//     box is the least likely to have been claimed and worked in.
//
// Deliberately NOT StatefulSet's highest-ordinal rule, which would kill whichever box sorts
// last — possibly one in active use while a failed box sits beside it.
//
// TODO: step 3 really wants least-recently-USED, so an idle box goes before one holding a
// live session. That needs per-box activity data nothing reports today; "youngest Ready" is
// the proxy until something does.
func selectForRemoval(owned []nebulav1alpha1.Sandbox, n int) []nebulav1alpha1.Sandbox {
	if n >= len(owned) {
		return owned
	}

	candidates := make([]nebulav1alpha1.Sandbox, len(owned))
	copy(candidates, owned)
	sort.SliceStable(candidates, func(i, j int) bool {
		ri, rj := removalRank(&candidates[i]), removalRank(&candidates[j])
		if ri != rj {
			return ri < rj
		}
		// Within a rank, youngest first.
		return candidates[i].CreationTimestamp.After(candidates[j].CreationTimestamp.Time)
	})
	return candidates[:n]
}

// removalRank orders boxes by how little it costs to lose them: lower goes first.
func removalRank(sbx *nebulav1alpha1.Sandbox) int {
	switch sbx.Status.Phase {
	case nebulav1alpha1.SandboxFailed, nebulav1alpha1.SandboxExpired:
		return 0 // dead already
	case nebulav1alpha1.SandboxReady:
		return 2 // possibly in use — last resort
	default:
		return 1 // still coming up: nobody has used it yet
	}
}

// setStatus rolls the owned boxes up into the set's status, including the selector
// the /scale subresource needs. It skips the write when nothing changed, so a
// steady-state set does not generate an update per resync.
func (r *SandboxSetReconciler) setStatus(ctx context.Context, set *nebulav1alpha1.SandboxSet, owned []nebulav1alpha1.Sandbox) error {
	before := set.Status.DeepCopy()

	var ready int32
	names := make([]string, 0, len(owned))
	for i := range owned {
		names = append(names, owned[i].Name)
		if owned[i].Status.Phase == nebulav1alpha1.SandboxReady {
			ready++
		}
	}

	set.Status.Replicas = int32(len(owned))
	set.Status.ReadyReplicas = ready
	set.Status.Sandboxes = names
	// The /scale subresource requires the selector as a serialized string; HPA reads
	// it from here to find the set's members, so autoscaling silently does nothing
	// without it.
	set.Status.Selector = labels.SelectorFromSet(labels.Set{
		nebulav1alpha1.SandboxSetLabel: set.Name,
	}).String()

	reason, msg, condStatus := nebulav1alpha1.ReasonSandboxSetProgressing,
		"waiting for sandboxes to become ready", metav1.ConditionFalse
	switch {
	case set.Spec.Replicas == 0:
		reason, msg = nebulav1alpha1.ReasonSandboxSetScaledToZero, "scaled to zero"
	case ready == set.Spec.Replicas:
		reason, msg, condStatus = nebulav1alpha1.ReasonSandboxSetReady,
			"all sandboxes are ready", metav1.ConditionTrue
	}
	apimeta.SetStatusCondition(&set.Status.Conditions, metav1.Condition{
		Type:               nebulav1alpha1.SandboxSetConditionReady,
		Status:             condStatus,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: set.Generation,
	})

	if apiequality.Semantic.DeepEqual(before, &set.Status) {
		return nil
	}
	return r.Status().Update(ctx, set)
}

// SetupWithManager wires the controller. It owns Sandboxes, so a box becoming
// ready or failing re-reconciles the set immediately — which is what makes
// self-healing prompt: a Failed box is removed and replaced on that same event
// rather than at the next resync.
func (r *SandboxSetReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.SandboxSet{}).
		Owns(&nebulav1alpha1.Sandbox{}).
		Named("sandboxset").
		Complete(r)
}
