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
	"fmt"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

const testSetName = "workers"

func newSetReconciler(objs ...client.Object) (*SandboxSetReconciler, client.Client) {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = nebulav1alpha1.AddToScheme(s)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&nebulav1alpha1.SandboxSet{}, &nebulav1alpha1.Sandbox{}).
		Build()
	return &SandboxSetReconciler{Client: c, Scheme: s}, c
}

func newSandboxSet(replicas int32) *nebulav1alpha1.SandboxSet {
	return &nebulav1alpha1.SandboxSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSetName,
			Namespace: testNS,
			UID:       types.UID("set-uid-1"),
		},
		Spec: nebulav1alpha1.SandboxSetSpec{
			Replicas: replicas,
			Template: nebulav1alpha1.SandboxTemplateSpec{
				Metadata: nebulav1alpha1.SandboxTemplateMetadata{
					Labels: map[string]string{"tier": "agent"},
				},
				Spec: nebulav1alpha1.SandboxSpec{
					NodePoolRef: "gpu",
					Image:       "ubuntu:24.04",
				},
			},
		},
	}
}

func reconcileSet(t *testing.T, r *SandboxSetReconciler) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: testSetName},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
}

func listSandboxes(t *testing.T, c client.Client) []nebulav1alpha1.Sandbox {
	t.Helper()
	var list nebulav1alpha1.SandboxList
	if err := c.List(context.Background(), &list, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list sandboxes: %v", err)
	}
	return list.Items
}

func getSet(t *testing.T, c client.Client) *nebulav1alpha1.SandboxSet {
	t.Helper()
	var set nebulav1alpha1.SandboxSet
	key := client.ObjectKey{Namespace: testNS, Name: testSetName}
	if err := c.Get(context.Background(), key, &set); err != nil {
		t.Fatalf("get sandboxset: %v", err)
	}
	return &set
}

// ownedSandbox is a member box as the set would have created it, with the phase
// and age a test needs. The UID is explicit because ownership is matched on it.
func ownedSandbox(name string, phase nebulav1alpha1.SandboxPhase, ageMinutes int) *nebulav1alpha1.Sandbox {
	yes := true
	return &nebulav1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNS,
			UID:       types.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(
				time.Now().Add(-time.Duration(ageMinutes) * time.Minute)),
			Labels: map[string]string{nebulav1alpha1.SandboxSetLabel: testSetName},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: nebulav1alpha1.GroupVersion.String(),
				Kind:       "SandboxSet",
				Name:       testSetName,
				UID:        types.UID("set-uid-1"),
				Controller: &yes,
			}},
		},
		Spec:   nebulav1alpha1.SandboxSpec{NodePoolRef: "gpu", Image: "ubuntu:24.04"},
		Status: nebulav1alpha1.SandboxStatus{Phase: phase},
	}
}

// TestSandboxSetScalesUpFromTemplate: the set must create exactly Replicas boxes,
// each stamped from the template and labelled so the set can find it again.
func TestSandboxSetScalesUpFromTemplate(t *testing.T) {
	r, c := newSetReconciler(newSandboxSet(3))
	reconcileSet(t, r)

	boxes := listSandboxes(t, c)
	if len(boxes) != 3 {
		t.Fatalf("sandboxes = %d, want 3", len(boxes))
	}
	for i := range boxes {
		sbx := &boxes[i]
		if sbx.Spec.Image != "ubuntu:24.04" || sbx.Spec.NodePoolRef != "gpu" {
			t.Errorf("%s: spec not stamped from the template: %+v", sbx.Name, sbx.Spec)
		}
		if got := sbx.Labels[nebulav1alpha1.SandboxSetLabel]; got != testSetName {
			t.Errorf("%s: ownership label = %q, want %q", sbx.Name, got, testSetName)
		}
		if got := sbx.Labels["tier"]; got != "agent" {
			t.Errorf("%s: template label lost: tier = %q", sbx.Name, got)
		}
		if ref := metav1.GetControllerOf(sbx); ref == nil || ref.Kind != "SandboxSet" {
			t.Errorf("%s: controller ref = %+v, want the SandboxSet", sbx.Name, ref)
		}
	}
}

// TestSandboxSetTemplateCannotOrphanBox: the ownership label is applied after the
// template's, so a template that sets it cannot detach the box from its own set —
// which would leave an unowned box billing forever, invisible to the set.
func TestSandboxSetTemplateCannotOrphanBox(t *testing.T) {
	set := newSandboxSet(1)
	set.Spec.Template.Metadata.Labels[nebulav1alpha1.SandboxSetLabel] = "somewhere-else"
	r, c := newSetReconciler(set)
	reconcileSet(t, r)

	boxes := listSandboxes(t, c)
	if len(boxes) != 1 {
		t.Fatalf("sandboxes = %d, want 1", len(boxes))
	}
	if got := boxes[0].Labels[nebulav1alpha1.SandboxSetLabel]; got != testSetName {
		t.Errorf("ownership label = %q, want %q: the template must not override it", got, testSetName)
	}
}

// TestSandboxSetIsIdempotent: re-reconciling a satisfied set must not create more
// boxes. Each box is a paid instance, so a leak here is a bill, not just a bug.
func TestSandboxSetIsIdempotent(t *testing.T) {
	r, c := newSetReconciler(newSandboxSet(2))
	reconcileSet(t, r)
	reconcileSet(t, r)
	reconcileSet(t, r)

	if n := len(listSandboxes(t, c)); n != 2 {
		t.Errorf("sandboxes = %d, want 2: reconcile must be idempotent", n)
	}
}

// TestSandboxSetIgnoresForeignSandbox: a box carrying the set's label but owned by
// someone else must be neither counted nor deleted. Counting it would starve the
// set; deleting it would let anyone destroy a box they do not own by labelling it.
func TestSandboxSetIgnoresForeignSandbox(t *testing.T) {
	foreign := ownedSandbox("imposter", nebulav1alpha1.SandboxReady, 5)
	foreign.OwnerReferences = nil

	r, c := newSetReconciler(newSandboxSet(1), foreign)
	reconcileSet(t, r)

	boxes := listSandboxes(t, c)
	if len(boxes) != 2 {
		t.Fatalf("sandboxes = %d, want 2 (the imposter plus one real box)", len(boxes))
	}
	var found bool
	for i := range boxes {
		if boxes[i].Name == "imposter" {
			found = true
		}
	}
	if !found {
		t.Error("the foreign Sandbox was deleted; a label alone must not grant the set authority over it")
	}
	if got := getSet(t, c).Status.Replicas; got != 1 {
		t.Errorf("status.replicas = %d, want 1: the foreign box must not be counted", got)
	}
}

// TestSandboxSetReplacesTerminalBox is the self-healing path: a terminal box is
// deleted AND replaced on the same pass. Without the prune the set would sit at
// "3 replicas, 2 usable" forever, because the Sandbox controller deliberately never
// resurrects a dead box.
func TestSandboxSetReplacesTerminalBox(t *testing.T) {
	for _, phase := range []nebulav1alpha1.SandboxPhase{
		nebulav1alpha1.SandboxFailed,
		nebulav1alpha1.SandboxExpired,
	} {
		t.Run(string(phase), func(t *testing.T) {
			dead := ownedSandbox("workers-dead", phase, 30)
			alive := ownedSandbox("workers-alive", nebulav1alpha1.SandboxReady, 20)

			r, c := newSetReconciler(newSandboxSet(2), dead, alive)
			reconcileSet(t, r)

			boxes := listSandboxes(t, c)
			if len(boxes) != 2 {
				t.Fatalf("sandboxes = %d, want 2", len(boxes))
			}
			for i := range boxes {
				if boxes[i].Name == "workers-dead" {
					t.Error("the terminal box was not pruned; the set can never return to 2 usable boxes")
				}
			}
			if got := getSet(t, c).Status.Replicas; got != 2 {
				t.Errorf("status.replicas = %d, want 2", got)
			}
		})
	}
}

// TestSandboxSetScalesDown checks scale-in removes exactly the excess.
func TestSandboxSetScalesDown(t *testing.T) {
	objs := []client.Object{newSandboxSet(1)}
	for i := range 3 {
		objs = append(objs, ownedSandbox(fmt.Sprintf("workers-%d", i),
			nebulav1alpha1.SandboxReady, 10+i))
	}
	r, c := newSetReconciler(objs...)
	reconcileSet(t, r)

	boxes := listSandboxes(t, c)
	if len(boxes) != 1 {
		t.Fatalf("sandboxes = %d, want 1", len(boxes))
	}
	// Youngest-first within the Ready rank: workers-0 is the youngest (10m) and
	// workers-2 the oldest (12m), so the OLDEST box is the survivor.
	if boxes[0].Name != "workers-2" {
		t.Errorf("survivor = %q, want workers-2 (the oldest Ready box)", boxes[0].Name)
	}
}

// TestSandboxSetScaleDownRemovesAllExcess: scale-in from 3 to 0 must delete every
// box in ONE pass. Bailing out early would leave paid instances running while the
// set reported the scale-in as done.
func TestSandboxSetScaleDownRemovesAllExcess(t *testing.T) {
	objs := []client.Object{newSandboxSet(0)}
	for i := range 3 {
		objs = append(objs, ownedSandbox(fmt.Sprintf("workers-%d", i),
			nebulav1alpha1.SandboxReady, 10+i))
	}
	r, c := newSetReconciler(objs...)
	reconcileSet(t, r)

	if n := len(listSandboxes(t, c)); n != 0 {
		t.Errorf("sandboxes = %d, want 0: every excess box must be deleted in one pass", n)
	}
	set := getSet(t, c)
	if set.Status.Replicas != 0 {
		t.Errorf("status.replicas = %d, want 0", set.Status.Replicas)
	}
	if reason := readyReason(set.Status.Conditions); reason != nebulav1alpha1.ReasonSandboxSetScaledToZero {
		t.Errorf("condition reason = %q, want %q", reason, nebulav1alpha1.ReasonSandboxSetScaledToZero)
	}
}

// TestSelectForRemovalOrder pins the victim order directly, since it decides whose
// work gets destroyed: dead boxes first, then boxes nobody could have used, and a
// possibly-in-use Ready box only as a last resort.
func TestSelectForRemovalOrder(t *testing.T) {
	ready := *ownedSandbox("ready", nebulav1alpha1.SandboxReady, 30)
	provisioning := *ownedSandbox("provisioning", nebulav1alpha1.SandboxProvisioning, 20)
	failed := *ownedSandbox("failed", nebulav1alpha1.SandboxFailed, 10)
	owned := []nebulav1alpha1.Sandbox{ready, provisioning, failed}

	got := selectForRemoval(owned, 2)
	if len(got) != 2 {
		t.Fatalf("victims = %d, want 2", len(got))
	}
	if got[0].Name != "failed" {
		t.Errorf("first victim = %q, want failed (a dead box costs nothing to lose)", got[0].Name)
	}
	if got[1].Name != "provisioning" {
		t.Errorf("second victim = %q, want provisioning (nobody can have used it)", got[1].Name)
	}

	// Asking for more than exists must return everything, not panic on a slice bound.
	if n := len(selectForRemoval(owned, 5)); n != 3 {
		t.Errorf("victims = %d, want 3 when n exceeds the population", n)
	}
}

// TestSandboxSetStatusSelector: /scale requires the selector as a serialized
// string, and HPA reads it from status to find the set's members — autoscaling
// silently does nothing if it is wrong.
func TestSandboxSetStatusSelector(t *testing.T) {
	r, c := newSetReconciler(newSandboxSet(1))
	reconcileSet(t, r)

	want := nebulav1alpha1.SandboxSetLabel + "=" + testSetName
	if got := getSet(t, c).Status.Selector; got != want {
		t.Errorf("status.selector = %q, want %q", got, want)
	}
}

// TestSandboxSetReadyRollup: the set is Ready only when every box is, and the
// reported names must be the boxes that actually exist.
func TestSandboxSetReadyRollup(t *testing.T) {
	a := ownedSandbox("workers-a", nebulav1alpha1.SandboxReady, 10)
	b := ownedSandbox("workers-b", nebulav1alpha1.SandboxProvisioning, 5)
	r, c := newSetReconciler(newSandboxSet(2), a, b)
	reconcileSet(t, r)

	set := getSet(t, c)
	if set.Status.Replicas != 2 || set.Status.ReadyReplicas != 1 {
		t.Errorf("replicas/ready = %d/%d, want 2/1", set.Status.Replicas, set.Status.ReadyReplicas)
	}
	if readyCondStatus(set.Status.Conditions) != metav1.ConditionFalse {
		t.Error("Ready must be False while one box is still coming up")
	}
	if len(set.Status.Sandboxes) != 2 {
		t.Errorf("status.sandboxes = %v, want both box names", set.Status.Sandboxes)
	}

	// Bring the laggard up: the set must flip to Ready.
	b.Status.Phase = nebulav1alpha1.SandboxReady
	if err := c.Status().Update(context.Background(), b); err != nil {
		t.Fatalf("update sandbox status: %v", err)
	}
	reconcileSet(t, r)

	set = getSet(t, c)
	if set.Status.ReadyReplicas != 2 {
		t.Errorf("readyReplicas = %d, want 2", set.Status.ReadyReplicas)
	}
	if readyCondStatus(set.Status.Conditions) != metav1.ConditionTrue {
		t.Error("Ready must be True once every box is ready")
	}
	if reason := readyReason(set.Status.Conditions); reason != nebulav1alpha1.ReasonSandboxSetReady {
		t.Errorf("condition reason = %q, want %q", reason, nebulav1alpha1.ReasonSandboxSetReady)
	}
}

func readyCondStatus(conds []metav1.Condition) metav1.ConditionStatus {
	for _, c := range conds {
		if c.Type == nebulav1alpha1.SandboxSetConditionReady {
			return c.Status
		}
	}
	return ""
}

func readyReason(conds []metav1.Condition) string {
	for _, c := range conds {
		if c.Type == nebulav1alpha1.SandboxSetConditionReady {
			return c.Reason
		}
	}
	return ""
}
