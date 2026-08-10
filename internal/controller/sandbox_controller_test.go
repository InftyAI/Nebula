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
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/util"
)

const (
	testNS      = "team-ml"
	testSbxName = "alice"
)

// newSandboxReconciler builds a reconciler over a fake client seeded with objs.
func newSandboxReconciler(objs ...client.Object) (*SandboxReconciler, client.Client) {
	s := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(s)
	_ = nebulav1alpha1.AddToScheme(s)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(objs...).
		WithStatusSubresource(&nebulav1alpha1.Sandbox{}).
		Build()
	return &SandboxReconciler{Client: c, Scheme: s}, c
}

// newSandbox is a Sandbox with the fields the controller reads. The UID is set
// because ownership checks compare it, and the fake client does not assign one.
func newSandbox(mutators ...func(*nebulav1alpha1.Sandbox)) *nebulav1alpha1.Sandbox {
	sbx := &nebulav1alpha1.Sandbox{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSbxName,
			Namespace: testNS,
			UID:       types.UID("sbx-uid-1"),
		},
		Spec: nebulav1alpha1.SandboxSpec{
			NodePoolRef:     "gpu",
			Image:           "ubuntu:24.04",
			AcceleratorType: "a100-40gb",
			Resources: corev1.ResourceRequirements{
				Limits: corev1.ResourceList{
					util.NvidiaGPUResource: resource.MustParse("1"),
				},
			},
		},
	}
	for _, m := range mutators {
		m(sbx)
	}
	return sbx
}

func reconcileSandbox(t *testing.T, r *SandboxReconciler) reconcile.Result {
	t.Helper()
	res, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Namespace: testNS, Name: testSbxName},
	})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	return res
}

func getSandboxPod(t *testing.T, c client.Client) *corev1.Pod {
	t.Helper()
	var pod corev1.Pod
	key := client.ObjectKey{Namespace: testNS, Name: testSbxName}
	if err := c.Get(context.Background(), key, &pod); err != nil {
		t.Fatalf("get pod: %v", err)
	}
	return &pod
}

func getSandbox(t *testing.T, c client.Client) *nebulav1alpha1.Sandbox {
	t.Helper()
	var sbx nebulav1alpha1.Sandbox
	key := client.ObjectKey{Namespace: testNS, Name: testSbxName}
	if err := c.Get(context.Background(), key, &sbx); err != nil {
		t.Fatalf("get sandbox: %v", err)
	}
	return &sbx
}

// TestSandboxSynthesizesPod covers the whole contract the synthesized Pod must
// satisfy for the EXISTING placement path to pick it up unchanged. Each assertion
// here is load-bearing: drop the opt-in label and the Pod is scheduled by vanilla
// Kubernetes and never reaches a provider; drop the pool label and placement has
// no policy to resolve.
func TestSandboxSynthesizesPod(t *testing.T) {
	r, c := newSandboxReconciler(newSandbox())
	reconcileSandbox(t, r)
	pod := getSandboxPod(t, c)

	if got := pod.Labels[nebulav1alpha1.EnabledLabel]; got != "true" {
		t.Errorf("opt-in label = %q, want \"true\" (without it the Pod never reaches a provider)", got)
	}
	if got := pod.Labels[nebulav1alpha1.PoolLabel]; got != "gpu" {
		t.Errorf("pool label = %q, want \"gpu\"", got)
	}
	if got := pod.Labels[nebulav1alpha1.SandboxLabel]; got != testSbxName {
		t.Errorf("sandbox label = %q, want %q", got, testSbxName)
	}
	if got := pod.Labels[nebulav1alpha1.AcceleratorTypeLabel]; got != "a100-40gb" {
		t.Errorf("accelerator label = %q, want \"a100-40gb\"", got)
	}
	if pod.Spec.RestartPolicy != corev1.RestartPolicyNever {
		t.Errorf("restartPolicy = %q, want Never", pod.Spec.RestartPolicy)
	}

	if n := len(pod.Spec.Containers); n != 1 {
		t.Fatalf("containers = %d, want 1", n)
	}
	ctr := pod.Spec.Containers[0]
	if ctr.Name != sandboxContainerName {
		t.Errorf("container name = %q, want %q (kubectl exec defaults to it)", ctr.Name, sandboxContainerName)
	}
	if ctr.Image != "ubuntu:24.04" {
		t.Errorf("image = %q, want ubuntu:24.04", ctr.Image)
	}
	// A non-exiting command must be set. Without one the container runs the image's
	// own entrypoint (`bash` for ubuntu:24.04), which exits with no TTY and takes the
	// instance with it, so the box would fail seconds after being provisioned.
	if got := ctr.Command; len(got) != 2 || got[0] != "sleep" || got[1] != "infinity" {
		t.Errorf("command = %v, want [sleep infinity]", got)
	}
	// The GPU count must survive as a standard resource: placement and the
	// scheduler's fit check both read it from here.
	if q, ok := ctr.Resources.Limits[util.NvidiaGPUResource]; !ok || q.Value() != 1 {
		t.Errorf("nvidia.com/gpu limit = %v (present=%v), want 1", q.Value(), ok)
	}

	// Controller-owned, so garbage collection releases the instance when the
	// Sandbox is deleted.
	ref := metav1.GetControllerOf(pod)
	if ref == nil || ref.Kind != "Sandbox" || ref.Name != testSbxName {
		t.Errorf("controller ref = %+v, want the Sandbox", ref)
	}
}

// TestSandboxCPUOnlyOmitsAcceleratorLabel: a CPU-only box must not carry an empty
// accelerator label, which would make placement look for an accelerator named "".
func TestSandboxCPUOnlyOmitsAcceleratorLabel(t *testing.T) {
	sbx := newSandbox(func(s *nebulav1alpha1.Sandbox) {
		s.Spec.AcceleratorType = ""
		s.Spec.Resources = corev1.ResourceRequirements{}
	})
	r, c := newSandboxReconciler(sbx)
	reconcileSandbox(t, r)

	if _, present := getSandboxPod(t, c).Labels[nebulav1alpha1.AcceleratorTypeLabel]; present {
		t.Error("CPU-only sandbox must not set the accelerator-type label")
	}
}

// TestSandboxIsIdempotent: a second reconcile must not create a second Pod nor
// error. Controllers are re-run constantly, so this is the baseline invariant.
func TestSandboxIsIdempotent(t *testing.T) {
	r, c := newSandboxReconciler(newSandbox())
	reconcileSandbox(t, r)
	reconcileSandbox(t, r)

	var pods corev1.PodList
	if err := c.List(context.Background(), &pods, client.InNamespace(testNS)); err != nil {
		t.Fatalf("list pods: %v", err)
	}
	if len(pods.Items) != 1 {
		t.Fatalf("pods = %d, want 1 (reconcile must be idempotent)", len(pods.Items))
	}
}

// TestSandboxPhaseFromPod checks the projection from Pod state to Sandbox phase.
// The distinction that matters most is inside PodPending: a gated Pod means "no
// provider can serve this box" while an ungated one means "provisioning is under
// way" — conflating them would make a capacity problem look like a slow boot.
func TestSandboxPhaseFromPod(t *testing.T) {
	gated := func(p *corev1.Pod) {
		p.Spec.SchedulingGates = []corev1.PodSchedulingGate{
			{Name: nebulav1alpha1.ProviderSelectionGate},
		}
	}
	tests := []struct {
		name    string
		mutate  func(*corev1.Pod)
		want    nebulav1alpha1.SandboxPhase
		wantRdy bool
	}{
		{
			name:   "gated pod is Pending, not Provisioning",
			mutate: func(p *corev1.Pod) { gated(p); p.Status.Phase = corev1.PodPending },
			want:   nebulav1alpha1.SandboxPending,
		},
		{
			name:   "ungated pending pod is Provisioning",
			mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodPending },
			want:   nebulav1alpha1.SandboxProvisioning,
		},
		{
			name: "pending with Initializing reason is Initializing",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodPending
				p.Status.Reason = podReasonInitializing
			},
			want: nebulav1alpha1.SandboxInitializing,
		},
		{
			name: "running but not ready is Initializing",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodRunning
			},
			want: nebulav1alpha1.SandboxInitializing,
		},
		{
			name: "running and ready is Ready",
			mutate: func(p *corev1.Pod) {
				p.Status.Phase = corev1.PodRunning
				p.Status.Conditions = []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionTrue},
				}
			},
			want:    nebulav1alpha1.SandboxReady,
			wantRdy: true,
		},
		{
			name:   "failed pod is Failed",
			mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed },
			want:   nebulav1alpha1.SandboxFailed,
		},
		{
			// The command only exits when the box goes away, so a Succeeded Pod still means
			// the instance is gone — not that the sandbox completed successfully.
			name:   "succeeded pod is Failed too",
			mutate: func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded },
			want:   nebulav1alpha1.SandboxFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sbx := newSandbox()
			r, c := newSandboxReconciler(sbx)
			reconcileSandbox(t, r) // creates the Pod

			pod := getSandboxPod(t, c)
			tc.mutate(pod)
			// Spec and status must be written separately, and the status has to be
			// re-applied after the spec write: the fake client treats Pod status as a
			// subresource, so Update copies the STORED status back over the object it
			// was handed, discarding what mutate just set.
			want := pod.Status
			if err := c.Update(context.Background(), pod); err != nil {
				t.Fatalf("update pod: %v", err)
			}
			pod.Status = want
			if err := c.Status().Update(context.Background(), pod); err != nil {
				t.Fatalf("update pod status: %v", err)
			}

			reconcileSandbox(t, r)
			got := getSandbox(t, c)
			if got.Status.Phase != tc.want {
				t.Errorf("phase = %q, want %q", got.Status.Phase, tc.want)
			}
			ready := false
			for _, cond := range got.Status.Conditions {
				if cond.Type == nebulav1alpha1.SandboxConditionReady {
					ready = cond.Status == metav1.ConditionTrue
				}
			}
			if ready != tc.wantRdy {
				t.Errorf("Ready condition = %v, want %v", ready, tc.wantRdy)
			}
		})
	}
}

// TestSandboxEndpointAndReadyTime: the endpoint must be mirrored from the Pod
// annotation (it is the only way to reach the box), and ReadyTime must be stamped
// once so TTL has a stable anchor.
func TestSandboxEndpointAndReadyTime(t *testing.T) {
	r, c := newSandboxReconciler(newSandbox(func(s *nebulav1alpha1.Sandbox) {
		s.Spec.TTL = &metav1.Duration{Duration: time.Hour}
	}))
	reconcileSandbox(t, r)
	markPodReady(t, c, "ec2-1-2-3-4.compute.amazonaws.com")
	reconcileSandbox(t, r)

	sbx := getSandbox(t, c)
	if sbx.Status.Endpoint != "ec2-1-2-3-4.compute.amazonaws.com" {
		t.Errorf("endpoint = %q, want the Pod's annotation value", sbx.Status.Endpoint)
	}
	if sbx.Status.ReadyTime == nil {
		t.Fatal("ReadyTime must be stamped on the first transition to Ready")
	}
	if sbx.Status.ExpiryTime == nil {
		t.Fatal("ExpiryTime must be derived from ReadyTime + TTL")
	}
	firstReady := sbx.Status.ReadyTime.DeepCopy()

	// A later reconcile must NOT move ReadyTime: it anchors TTL, so re-deriving it
	// would let a status blip silently restart the user's clock.
	reconcileSandbox(t, r)
	if got := getSandbox(t, c).Status.ReadyTime; !got.Equal(firstReady) {
		t.Errorf("ReadyTime moved from %v to %v; it must be written exactly once", firstReady, got)
	}
}

// TestSandboxTTLReleasesInstance: once the deadline passes the Pod must be
// deleted — that is what triggers VK teardown and the NodeClaim finalizer behind
// it — while the Sandbox object survives as the record of why the box went away.
func TestSandboxTTLReleasesInstance(t *testing.T) {
	r, c := newSandboxReconciler(newSandbox(func(s *nebulav1alpha1.Sandbox) {
		s.Spec.TTL = &metav1.Duration{Duration: time.Hour}
	}))
	reconcileSandbox(t, r)
	markPodReady(t, c, "1.2.3.4")
	reconcileSandbox(t, r)

	// Backdate the expiry rather than sleeping.
	sbx := getSandbox(t, c)
	past := metav1.NewTime(time.Now().Add(-time.Minute))
	sbx.Status.ExpiryTime = &past
	if err := c.Status().Update(context.Background(), sbx); err != nil {
		t.Fatalf("update status: %v", err)
	}

	reconcileSandbox(t, r)

	var pod corev1.Pod
	err := c.Get(context.Background(), client.ObjectKey{Namespace: testNS, Name: testSbxName}, &pod)
	if !apierrors.IsNotFound(err) {
		t.Errorf("Pod must be deleted on expiry so the instance is released; get err = %v", err)
	}
	if got := getSandbox(t, c).Status.Phase; got != nebulav1alpha1.SandboxExpired {
		t.Errorf("phase = %q, want Expired", got)
	}
}

// TestSandboxTerminalDoesNotRecreatePod: an expired or failed box must stay dead.
// Recreating the Pod would hand the user a DIFFERENT box (empty filesystem, new
// endpoint) under the same name — the single most confusing thing this controller
// could do.
func TestSandboxTerminalDoesNotRecreatePod(t *testing.T) {
	for _, phase := range []nebulav1alpha1.SandboxPhase{
		nebulav1alpha1.SandboxExpired,
		nebulav1alpha1.SandboxFailed,
	} {
		t.Run(string(phase), func(t *testing.T) {
			sbx := newSandbox()
			sbx.Status.Phase = phase
			r, c := newSandboxReconciler(sbx)

			reconcileSandbox(t, r)

			var pods corev1.PodList
			if err := c.List(context.Background(), &pods, client.InNamespace(testNS)); err != nil {
				t.Fatalf("list pods: %v", err)
			}
			if len(pods.Items) != 0 {
				t.Errorf("pods = %d, want 0: a terminal sandbox must not be resurrected", len(pods.Items))
			}
		})
	}
}

// TestSandboxRefusesForeignPod: a Pod of the required name that belongs to someone
// else must NOT be adopted — adopting would subject an unrelated workload to this
// Sandbox's lifecycle, including deletion on TTL expiry.
func TestSandboxRefusesForeignPod(t *testing.T) {
	foreign := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testSbxName,
			Namespace: testNS,
			Labels:    map[string]string{"app": "someone-elses-thing"},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "app", Image: "nginx"}},
		},
	}
	r, c := newSandboxReconciler(newSandbox(), foreign)
	reconcileSandbox(t, r)

	sbx := getSandbox(t, c)
	if sbx.Status.Phase != nebulav1alpha1.SandboxPending {
		t.Errorf("phase = %q, want Pending", sbx.Status.Phase)
	}
	var reason string
	for _, cond := range sbx.Status.Conditions {
		if cond.Type == nebulav1alpha1.SandboxConditionReady {
			reason = cond.Reason
		}
	}
	if reason != nebulav1alpha1.ReasonPodConflict {
		t.Errorf("condition reason = %q, want %q", reason, nebulav1alpha1.ReasonPodConflict)
	}

	// The foreign Pod must be untouched.
	pod := getSandboxPod(t, c)
	if pod.Labels["app"] != "someone-elses-thing" {
		t.Error("the foreign Pod was mutated; it must be left alone")
	}
	if len(pod.Spec.Containers) != 1 || pod.Spec.Containers[0].Image != "nginx" {
		t.Error("the foreign Pod's spec was overwritten")
	}
}

// markPodReady drives the Sandbox's Pod to Running+Ready with an endpoint, the way
// the virtual kubelet would once the provider reports the instance up.
func markPodReady(t *testing.T, c client.Client, endpoint string) {
	t.Helper()
	pod := getSandboxPod(t, c)
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[nebulav1alpha1.EndpointAnnotation] = endpoint
	if err := c.Update(context.Background(), pod); err != nil {
		t.Fatalf("update pod: %v", err)
	}
	pod.Status.Phase = corev1.PodRunning
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionTrue},
	}
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatalf("update pod status: %v", err)
	}
}
