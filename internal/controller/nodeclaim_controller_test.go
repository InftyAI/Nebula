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
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// fakeProvider is a minimal provider.Provider. On the happy path the NodeClaim
// controller never touches a provider (VK owns provisioning), but the teardown
// backstop calls List/Terminate on the deletion path, so this records those.
type fakeProvider struct {
	name string

	list         []provider.Instance // what List returns
	listErr      error               // if set, List fails
	terminated   []string            // instance ids passed to Terminate, in order
	regions      []string            // regions passed to Terminate, positionally paired with terminated
	terminateErr error               // if set, Terminate fails
	gpus         []string            // accelerators MapAccelerator offers; nil = offer any
	spot         bool                // Capabilities().SupportsSpot (placement skips Spot without it)
	egress       bool                // Capabilities().SupportsEgressPolicy (placement skips restricted pools without it)
	// expandRegions overrides ExpandRegions; nil = pass the declaration through.
	expandRegions func([]string) []string
}

func (f *fakeProvider) Name() string { return f.name }
func (f *fakeProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{SupportsSpot: f.spot, SupportsEgressPolicy: f.egress}
}
func (f *fakeProvider) Provision(context.Context, *corev1.Pod, provider.ProvisionRequest) (provider.ProvisionResult, error) {
	return provider.ProvisionResult{}, nil
}
func (f *fakeProvider) Terminate(_ context.Context, id, region string) error {
	f.terminated = append(f.terminated, id)
	f.regions = append(f.regions, region)
	return f.terminateErr
}
func (f *fakeProvider) Get(context.Context, string) (*provider.Instance, error) { return nil, nil }
func (f *fakeProvider) List(context.Context) ([]provider.Instance, error) {
	return f.list, f.listErr
}
func (f *fakeProvider) Offerings(context.Context) ([]provider.Offering, error) { return nil, nil }
func (f *fakeProvider) MapAccelerator(c string, _ int32) ([]string, bool) {
	if f.gpus == nil {
		return []string{c}, true // offer any accelerator
	}
	for _, g := range f.gpus {
		if g == c {
			return []string{c}, true
		}
	}
	return nil, false
}

// ExpandRegions passes the declaration through, matching catalog.Base's default (the
// region-simple behaviour). Tests that need group expansion set expandRegions.
func (f *fakeProvider) ExpandRegions(declared []string) []string {
	if f.expandRegions != nil {
		return f.expandRegions(declared)
	}
	return declared
}
func (f *fakeProvider) ClassifyProvisionError(error, string, string) provider.BlockScope {
	return provider.BlockScope{}
}

// resolver returns a Providers func that resolves only the given provider.
func resolver(provs ...*fakeProvider) func(string) (provider.Provider, bool) {
	return func(name string) (provider.Provider, bool) {
		for _, p := range provs {
			if p.name == name {
				return p, true
			}
		}
		return nil, false
	}
}

// testScheme builds a scheme with core + Nebula types registered.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add client-go scheme: %v", err)
	}
	if err := nebulav1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add nebula scheme: %v", err)
	}
	return s
}

// newClaimReconciler wires a NodeClaimReconciler over a fake client seeded with
// objs. Any fakeProviders passed are registered as the reconciler's resolver so
// the teardown backstop can reach them.
func newClaimReconciler(t *testing.T, objs []client.Object, provs ...*fakeProvider) (*NodeClaimReconciler, client.Client) {
	t.Helper()
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&nebulav1alpha1.NodeClaim{}).
		Build()
	r := &NodeClaimReconciler{Client: c, Scheme: s}
	if len(provs) > 0 {
		r.Providers = resolver(provs...)
	}
	return r, c
}

func newPod(name, ns, uid string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID(uid)},
		Spec: corev1.PodSpec{
			NodeName:   "nebula-fake",
			Containers: []corev1.Container{{Name: "main", Image: "img"}},
		},
		Status: corev1.PodStatus{Phase: phase, PodIP: "1.2.3.4"},
	}
}

// withInstanceID stamps the annotation the virtual kubelet writes once Provision returns,
// which is where the claim controller reads the id from (see recordInstanceID).
func withInstanceID(pod *corev1.Pod, id string) {
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[nebulav1alpha1.InstanceIDAnnotation] = id
}

// newClaim builds a steady-state claim: it already carries the terminate
// finalizer, since the normal reconcile adds that before doing anything else.
// Use newClaimNoFinalizer to exercise the finalizer-addition path itself.
func newClaim(name, podName, podNS, podUID, prov string) *nebulav1alpha1.NodeClaim {
	nc := newClaimNoFinalizer(name, podName, podNS, podUID, prov)
	nc.Finalizers = []string{nebulav1alpha1.TerminateInstanceFinalizer}
	return nc
}

func newClaimNoFinalizer(name, podName, podNS, podUID, prov string) *nebulav1alpha1.NodeClaim {
	return &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: nebulav1alpha1.NodeClaimSpec{
			PodRef:   nebulav1alpha1.PodReference{Namespace: podNS, Name: podName, UID: podUID},
			Provider: prov,
		},
	}
}

func reconcileClaim(t *testing.T, r *NodeClaimReconciler, name string) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: name},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return res
}

func getClaim(t *testing.T, c client.Client, name string) *nebulav1alpha1.NodeClaim {
	t.Helper()
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), types.NamespacedName{Name: name}, &nc); err != nil {
		t.Fatalf("get claim %s: %v", name, err)
	}
	return &nc
}

func TestReconcile_AddsFinalizerBeforePlacing(t *testing.T) {
	// A claim with no finalizer: the first reconcile must add the terminate
	// finalizer (and do nothing else) so teardown is guaranteed from the start.
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	claim := newClaimNoFinalizer("c1", "p1", "default", "uid-1", "fake")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if !hasFinalizer(got) {
		t.Fatal("expected terminate finalizer to be added on first reconcile")
	}
	// Phase must NOT yet be Bound: the finalizer-add reconcile returns early.
	if got.Status.Phase == nebulav1alpha1.NodeClaimBound {
		t.Fatal("must not mark Bound in the same reconcile that adds the finalizer")
	}
}

func TestReconcile_MarksBoundWhenPodPresent(t *testing.T) {
	// With the finalizer already present and the served Pod alive, the claim is
	// marked Bound (the durable guard) and nothing is torn down.
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimBound {
		t.Fatalf("expected phase Bound, got %q", got.Status.Phase)
	}
	if !got.DeletionTimestamp.IsZero() {
		t.Fatal("must not delete a claim whose Pod is present")
	}
}

func TestReconcile_PendingPodDoesNotMarkBound(t *testing.T) {
	// A served Pod that exists but is still provisioning (PodPending) must NOT
	// flip the claim to Bound — Bound is the teardown guard, earned only once the
	// instance is actually running. The instance id is still captured early.
	pod := newPod("p1", "default", "uid-1", corev1.PodPending)
	withInstanceID(pod, "inst-1")
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase == nebulav1alpha1.NodeClaimBound {
		t.Fatal("must not mark Bound while the Pod is still provisioning")
	}
	if got.Status.Phase != nebulav1alpha1.NodeClaimProvisioning {
		t.Fatalf("expected phase Provisioning, got %q", got.Status.Phase)
	}
	if got.Status.InstanceID != "inst-1" {
		t.Fatalf("expected instance id captured early, got %q", got.Status.InstanceID)
	}
}

func TestReconcile_BootingPodEarnsBound(t *testing.T) {
	// A served Pod that is Pending with reason Initializing means the instance EXISTS
	// and is booting (e.g. EC2 running but <2/2 checks, or a Modal sandbox whose
	// readiness probe has not passed). vnode stamps that reason only for an instance
	// it observed in the provider's List, so it is positive evidence of existence —
	// and existence, not readiness, is what the claim tracks. It must earn Bound: the
	// box is real and billable, so if its Pod later vanishes it has to be reclaimed
	// immediately rather than waiting out the grace window.
	pod := newPod("p1", "default", "uid-1", corev1.PodPending)
	pod.Status.Reason = podReasonInitializing
	withInstanceID(pod, "inst-1")
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimBound {
		t.Fatalf("expected phase Bound for a booting instance, got %q", got.Status.Phase)
	}
	if got.Status.InstanceID != "inst-1" {
		t.Fatalf("expected instance id captured, got %q", got.Status.InstanceID)
	}
}

func TestReconcile_BootingPodGoneIsTornDownWithoutGrace(t *testing.T) {
	// The point of the change: a claim whose instance was confirmed to exist while
	// still BOOTING, and whose Pod then disappears, must self-delete immediately so
	// the finalizer reclaims the instance. Previously such a claim sat at
	// Initializing, which did not earn the guard, so a real billable GPU box was left
	// running behind the cache-lag grace window.
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Status.Phase = nebulav1alpha1.NodeClaimBound // earned while booting
	prov := &fakeProvider{
		name: "fake",
		list: []provider.Instance{{ID: "inst-1", ClaimName: "default-p1"}},
	}
	// No Pod object: the served Pod is gone.
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	got := &nebulav1alpha1.NodeClaim{}
	err := c.Get(context.Background(), types.NamespacedName{Name: "c1"}, got)
	if err == nil && got.DeletionTimestamp.IsZero() {
		t.Fatal("expected the claim to be deleted (no grace) once its instance was known to exist")
	}
}

func TestReconcile_BoundClaimDoesNotDowngradeOnStatusFlap(t *testing.T) {
	// A claim already Bound whose Pod briefly drops back to Pending (a status-check
	// flap surfacing as reason Initializing) must NOT downgrade: Bound is the durable
	// teardown guard, and losing it would make a later disappearance read as cache
	// lag and skip teardown. The phase holds at Bound.
	pod := newPod("p1", "default", "uid-1", corev1.PodPending)
	pod.Status.Reason = podReasonInitializing
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Status.Phase = nebulav1alpha1.NodeClaimBound
	prov := &fakeProvider{
		name: "fake",
		list: []provider.Instance{{ID: "inst-1", ClaimName: "default-p1"}},
	}
	r, c := newClaimReconciler(t, []client.Object{pod, claim}, prov)

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimBound {
		t.Fatalf("Bound must not downgrade on a transient flap, got %q", got.Status.Phase)
	}
}

func TestReconcile_RecordsInstanceIDOnBound(t *testing.T) {
	// When the served Pod is running, the claim is marked Bound AND its
	// status.InstanceID is copied off the Pod annotation VK stamped, so the teardown
	// backstop can reclaim the instance even if VK forgets the id. No provider is
	// registered here on purpose: the id must come from the Pod, not from a List.
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	withInstanceID(pod, "inst-1")
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimBound {
		t.Fatalf("expected phase Bound, got %q", got.Status.Phase)
	}
	if got.Status.InstanceID != "inst-1" {
		t.Fatalf("expected status.InstanceID recorded as inst-1, got %q", got.Status.InstanceID)
	}
}

func TestReconcile_TerminalPodMarksTerminated(t *testing.T) {
	// A served Pod that has reached a terminal phase (VK reports the external
	// instance gone) must move the claim to Terminated rather than wedge it at
	// Bound. The claim is NOT deleted — its Pod still exists (restartPolicy:
	// Never leaves it around), and the instance is already gone.
	pod := newPod("p1", "default", "uid-1", corev1.PodFailed)
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Status.Phase = nebulav1alpha1.NodeClaimBound
	prov := &fakeProvider{name: "fake"} // instance already gone from List
	r, c := newClaimReconciler(t, []client.Object{pod, claim}, prov)

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimTerminated {
		t.Fatalf("expected phase Terminated, got %q", got.Status.Phase)
	}
	if !got.DeletionTimestamp.IsZero() {
		t.Fatal("must not delete a Terminated claim whose Pod still exists")
	}
}

func TestReconcile_BoundClaimWithGonePodSelfDeletes(t *testing.T) {
	// A claim we previously observed Bound, whose Pod has since vanished, is a
	// real teardown: the claim self-deletes (a finalizer keeps it around for the
	// backstop rather than dropping it immediately).
	claim := newClaim("c1", "gone", "default", "uid-1", "fake")
	claim.Status.Phase = nebulav1alpha1.NodeClaimBound
	prov := &fakeProvider{name: "fake"}
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("expected a Bound claim with a gone Pod to be deleted")
	}
}

func TestReconcile_ProvisioningPodGoneDeletesWithoutGrace(t *testing.T) {
	// A claim at Provisioning has already reconciled on a live Pod, so an absent Pod is a
	// real teardown, not cache lag: delete at once instead of sitting out the grace window.
	claim := newClaim("c1", "pending", "default", "uid-1", "fake")
	claim.CreationTimestamp = metav1.NewTime(time.Now())
	claim.Status.Phase = nebulav1alpha1.NodeClaimProvisioning
	prov := &fakeProvider{name: "fake"}
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	res := reconcileClaim(t, r, "c1")

	if res.RequeueAfter > 0 {
		t.Fatalf("expected no grace requeue for a claim that observed its Pod, got %+v", res)
	}
	got := getClaim(t, c, "c1")
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("expected a Provisioning claim with a gone Pod to be deleted immediately")
	}
}

func TestReconcile_NeverObservedPodWaitsForGrace(t *testing.T) {
	// A claim that never reached Bound and whose Pod is absent is treated as
	// possible cache lag within the grace window: requeue, do NOT delete.
	claim := newClaim("c1", "pending", "default", "uid-1", "fake")
	claim.CreationTimestamp = metav1.NewTime(time.Now())
	r, c := newClaimReconciler(t, []client.Object{claim})

	res := reconcileClaim(t, r, "c1")

	if res.RequeueAfter <= 0 {
		t.Fatalf("expected a requeue within the grace period, got %+v", res)
	}
	got := getClaim(t, c, "c1")
	if !got.DeletionTimestamp.IsZero() {
		t.Fatal("must not delete a never-observed claim inside the grace window (cache-lag guard)")
	}
}

func TestReconcile_NeverObservedPodDeletedAfterGrace(t *testing.T) {
	// Same claim, but the grace window has elapsed and the Pod never appeared:
	// nothing was ever provisioned, so the orphaned claim is deleted.
	claim := newClaim("c1", "pending", "default", "uid-1", "fake")
	claim.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * placementGracePeriod))
	prov := &fakeProvider{name: "fake"}
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	got := getClaim(t, c, "c1")
	if got.DeletionTimestamp.IsZero() {
		t.Fatal("expected an orphaned claim to be deleted after the grace period")
	}
}

func TestReconcileDelete_TerminatesInstanceByClaimName(t *testing.T) {
	// The backstop: on the deletion path, the instance is found by its
	// Pod-derived claim name via List and terminated before the finalizer drops.
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	deleteClaim(t, claim) // set deletionTimestamp; finalizer keeps it alive
	prov := &fakeProvider{
		name: "fake",
		list: []provider.Instance{{ID: "inst-1", ClaimName: "default-p1"}},
	}
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	if len(prov.terminated) != 1 || prov.terminated[0] != "inst-1" {
		t.Fatalf("expected Terminate(inst-1), got %v", prov.terminated)
	}
	if claimExists(t, c, "c1") {
		t.Fatal("expected finalizer released and claim gone after teardown")
	}
}

func TestReconcileDelete_UsesRecordedInstanceID(t *testing.T) {
	// When status.InstanceID is set (VK wrote it), the backstop terminates it
	// directly without needing a List lookup.
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Status.InstanceID = "inst-recorded"
	deleteClaim(t, claim)
	prov := &fakeProvider{name: "fake"} // List would return nothing
	r, _ := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	if len(prov.terminated) != 1 || prov.terminated[0] != "inst-recorded" {
		t.Fatalf("expected Terminate(inst-recorded) from recorded id, got %v", prov.terminated)
	}
}

// The backstop runs when VK never did, so the region VK held in memory is gone too. It
// must pass spec.Region, written before provisioning and never rewritten — otherwise a
// region-partitioned provider has to search for the instance and can conclude "already
// gone" about one it never looked for (see provider.Terminate).
func TestReconcileDelete_PassesTheClaimRegion(t *testing.T) {
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Spec.Region = "eu-west-1"
	claim.Status.InstanceID = "inst-1"
	deleteClaim(t, claim)
	prov := &fakeProvider{name: "fake"}
	r, _ := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	if len(prov.regions) != 1 || prov.regions[0] != "eu-west-1" {
		t.Fatalf("regions passed to Terminate = %v, want [eu-west-1]", prov.regions)
	}
}

func TestReconcileDelete_NoInstanceIsIdempotentNoOp(t *testing.T) {
	// The happy path: VK's DeletePod already terminated, so List finds nothing.
	// Terminate is called with "" (idempotent no-op) and the finalizer releases.
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	deleteClaim(t, claim)
	prov := &fakeProvider{name: "fake"} // empty list
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	if len(prov.terminated) != 1 || prov.terminated[0] != "" {
		t.Fatalf("expected a no-op Terminate(\"\"), got %v", prov.terminated)
	}
	if claimExists(t, c, "c1") {
		t.Fatal("expected claim gone after a no-op teardown")
	}
}

func TestReconcileDelete_UnknownProviderReleasesFinalizer(t *testing.T) {
	// If no adapter is registered for the claim's provider, we must not wedge
	// deletion forever — the finalizer is released so the claim can be removed.
	claim := newClaim("c1", "p1", "default", "uid-1", "ghost")
	deleteClaim(t, claim)
	prov := &fakeProvider{name: "fake"} // does not match "ghost"
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	reconcileClaim(t, r, "c1")

	if len(prov.terminated) != 0 {
		t.Fatalf("must not call Terminate on an unresolved provider, got %v", prov.terminated)
	}
	if claimExists(t, c, "c1") {
		t.Fatal("expected finalizer released so the claim is not stuck")
	}
}

func TestReconcileDelete_ListErrorRetries(t *testing.T) {
	// A transient List error must NOT release the finalizer — teardown retries
	// so the instance is never abandoned.
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	deleteClaim(t, claim)
	prov := &fakeProvider{name: "fake", listErr: errListFailed}
	r, c := newClaimReconciler(t, []client.Object{claim}, prov)

	_, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "c1"},
	})
	if err == nil {
		t.Fatal("expected an error so reconcile retries on a List failure")
	}
	if !claimExists(t, c, "c1") {
		t.Fatal("must not release the finalizer while teardown is unresolved")
	}
}

func TestClaimsForPod_DerivesTheClaimName(t *testing.T) {
	// One Pod event enqueues exactly one request, named the way ensureClaim named the
	// claim — no cluster-wide List, so the cost does not grow with the number of claims.
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	other := newClaim(util.ClaimName("default", "other"), "other", "default", "uid-2", "fake")
	r, _ := newClaimReconciler(t, []client.Object{pod, other})

	reqs := r.claimsForPod(context.Background(), pod)
	if len(reqs) != 1 || reqs[0].Name != util.ClaimName("default", "p1") {
		t.Fatalf("expected only the derived claim name for pod p1, got %+v", reqs)
	}
}

var errListFailed = errTest("list failed")

type errTest string

func (e errTest) Error() string { return string(e) }

func hasFinalizer(nc *nebulav1alpha1.NodeClaim) bool {
	for _, f := range nc.Finalizers {
		if f == nebulav1alpha1.TerminateInstanceFinalizer {
			return true
		}
	}
	return false
}

// deleteClaim stamps a deletionTimestamp on an in-memory claim so it can be
// seeded into the fake client already on the deletion path. (The fake client
// only accepts a deletionTimestamp on objects that carry a finalizer.)
func deleteClaim(t *testing.T, nc *nebulav1alpha1.NodeClaim) {
	t.Helper()
	now := metav1.Now()
	nc.DeletionTimestamp = &now
}

// claimExists reports whether the named claim is still present.
func claimExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	var nc nebulav1alpha1.NodeClaim
	err := c.Get(context.Background(), types.NamespacedName{Name: name}, &nc)
	if err == nil {
		return true
	}
	if apierrors.IsNotFound(err) {
		return false
	}
	t.Fatalf("get claim %s: %v", name, err)
	return false
}
