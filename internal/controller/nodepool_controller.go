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
	"fmt"
	"sort"
	"strings"

	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// NodePoolReconciler reconciles a NodePool object. The NodePool is the policy
// object (which providers, which capacity tiers, how to rank); actual placement
// — choosing a provider for a gated Pod — is a separate concern. This controller
// is therefore the pool's health and observability loop: it validates the policy
// against the registered providers and reports the live placement picture
// (running NodeClaims per provider) in status, so a misconfigured pool surfaces
// as a clear condition rather than as silent placement failures.
type NodePoolReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// Providers resolves a provider name to its backend; defaults to the
	// process-wide registry. Overridable in tests.
	Providers func(name string) (provider.Provider, bool)
}

// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodepools,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodepools/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodepools/finalizers,verbs=update
// +kubebuilder:rbac:groups=nebula.inftyai.com,resources=nodeclaims,verbs=get;list;watch

// Reconcile validates the pool's policy and refreshes its status.
func (r *NodePoolReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var pool nebulav1alpha1.NodePool
	if err := r.Get(ctx, req.NamespacedName, &pool); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Validate the policy against the registered providers.
	if reason, msg, ok := r.validate(&pool); !ok {
		log.Info("NodePool invalid", "reason", reason, "message", msg)
		apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
			Type:               nebulav1alpha1.NodePoolConditionReady,
			Status:             metav1.ConditionFalse,
			Reason:             reason,
			Message:            msg,
			ObservedGeneration: pool.Generation,
		})
		// Still refresh Placed so status reflects reality even while invalid.
		if err := r.refreshPlaced(ctx, &pool); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.Status().Update(ctx, &pool)
	}

	if err := r.refreshPlaced(ctx, &pool); err != nil {
		return ctrl.Result{}, err
	}
	apimeta.SetStatusCondition(&pool.Status.Conditions, metav1.Condition{
		Type:               nebulav1alpha1.NodePoolConditionReady,
		Status:             metav1.ConditionTrue,
		Reason:             nebulav1alpha1.ReasonPoolValid,
		Message:            "pool policy is valid",
		ObservedGeneration: pool.Generation,
	})
	return ctrl.Result{}, r.Status().Update(ctx, &pool)
}

// validate checks the pool against runtime state: every referenced provider has
// a registered adapter. Static, spec-only invariants (e.g. Weighted requires a
// weight on every provider) are enforced at admission by a CEL rule on
// NodePoolSpec, so they never reach here. This check stays in the controller
// because it depends on the provider registry — a webhook's view of registered
// providers can differ from the controller's (rollouts, creds-absent skips), so
// coupling admission to it would be a false-negative footgun on a fail-closed
// path. It returns (reason, message, ok=false) on the first problem.
func (r *NodePoolReconciler) validate(pool *nebulav1alpha1.NodePool) (reason, msg string, ok bool) {
	var unknown []string
	for _, p := range pool.Spec.Providers {
		if _, found := r.provider(p.Name); !found {
			unknown = append(unknown, p.Name)
		}
	}
	if len(unknown) > 0 {
		sort.Strings(unknown)
		return nebulav1alpha1.ReasonUnknownProvider,
			fmt.Sprintf("no registered adapter for provider(s): %s", strings.Join(unknown, ", ")),
			false
	}
	return "", "", true
}

// refreshPlaced recomputes status.Placed: the count of placed NodeClaims per
// provider that belong to this pool. It is derived state — a snapshot of the
// live placement picture — so it is fully recomputed each reconcile rather than
// incremented, which keeps it correct after missed events.
//
// "Placed" is a claim with an instance at the provider (phase Bound). That counts
// a BOOTING instance as placed, which is the intent: this is a capacity picture,
// and an instance still coming up already occupies quota and already bills. The
// claim does not mirror the Pod's finer runtime status (the Pod is the source of
// truth for readiness), so Bound is the claim-level signal that an instance exists
// for the workload.
func (r *NodePoolReconciler) refreshPlaced(ctx context.Context, pool *nebulav1alpha1.NodePool) error {
	var claims nebulav1alpha1.NodeClaimList
	if err := r.List(ctx, &claims); err != nil {
		return err
	}
	placed := map[string]int32{}
	for i := range claims.Items {
		nc := &claims.Items[i]
		if nc.Spec.PoolRef != pool.Name {
			continue
		}
		if nc.Status.Phase == nebulav1alpha1.NodeClaimBound {
			placed[nc.Spec.Provider]++
		}
	}
	if len(placed) == 0 {
		placed = nil // keep status clean when nothing is placed
	}
	// Materialize provider names for the kubectl column (JSONPath cannot join arrays).
	names := make([]string, 0, len(pool.Spec.Providers))
	for _, p := range pool.Spec.Providers {
		names = append(names, p.Name)
	}
	pool.Status.Providers = strings.Join(names, ",")
	pool.Status.Placed = placed
	return nil
}

// provider resolves the backend for name via the injected resolver, defaulting
// to the process-wide registry.
func (r *NodePoolReconciler) provider(name string) (provider.Provider, bool) {
	if r.Providers != nil {
		return r.Providers(name)
	}
	return provider.Get(name)
}

// SetupWithManager sets up the controller with the Manager. It watches
// NodeClaims too, mapping each back to its pool, so status.Placed tracks
// instances coming and going without waiting for the periodic resync.
func (r *NodePoolReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nebulav1alpha1.NodePool{}).
		Watches(
			&nebulav1alpha1.NodeClaim{},
			handler.EnqueueRequestsFromMapFunc(claimToPool),
		).
		Named("nodepool").
		Complete(r)
}

// claimToPool maps a NodeClaim to a reconcile request for its owning pool, so a
// claim changing state re-triggers that pool's status refresh. Cluster-scoped:
// the request carries only the pool name.
func claimToPool(_ context.Context, obj client.Object) []reconcile.Request {
	nc, ok := obj.(*nebulav1alpha1.NodeClaim)
	if !ok || nc.Spec.PoolRef == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Name: nc.Spec.PoolRef}}}
}
