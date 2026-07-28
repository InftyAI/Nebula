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

package vnode

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeProvider records lifecycle calls and returns canned results so each
// Handler branch can be driven deterministically.
type fakeProvider struct {
	mu           sync.Mutex
	provisionID  string
	provisionErr error
	provisionCnt int
	lastReq      provider.ProvisionRequest
	terminateCnt int
	terminateID  string
	terminateErr error
	list         []provider.Instance
	listErr      error
	capabilities provider.Capabilities
	// classifyScope is what ClassifyProvisionError returns for a failure; the zero
	// value (empty scope) means "not blocklistable". classifyAccel records the
	// accelerator the handler passed in, so a test can assert it resolved it off the
	// Pod (the provider now owns the whole scope; the handler only supplies this).
	classifyScope provider.BlockScope
	classifyAccel string
}

func (f *fakeProvider) Name() string                        { return "fake" }
func (f *fakeProvider) Capabilities() provider.Capabilities { return f.capabilities }

func (f *fakeProvider) Provision(_ context.Context, _ *corev1.Pod, req provider.ProvisionRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisionCnt++
	f.lastReq = req
	return f.provisionID, f.provisionErr
}

func (f *fakeProvider) Terminate(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.terminateCnt++
	f.terminateID = id
	return f.terminateErr
}

func (f *fakeProvider) Get(context.Context, string) (*provider.Instance, error) { return nil, nil }
func (f *fakeProvider) List(context.Context) ([]provider.Instance, error) {
	return f.list, f.listErr
}
func (f *fakeProvider) Offerings(context.Context) ([]provider.Offering, error) { return nil, nil }
func (f *fakeProvider) MapAccelerator(c string) (string, bool)                 { return c, true }
func (f *fakeProvider) ClassifyProvisionError(_ error, accel string) provider.BlockScope {
	f.classifyAccel = accel
	return f.classifyScope
}

// recordingBlocklist captures Record calls so a test can assert what the handler
// blocklisted on a Provision failure.
type recordingBlocklist struct {
	prov  string
	scope provider.BlockScope
	ttl   time.Duration
	calls int
}

func (b *recordingBlocklist) Record(prov string, scope provider.BlockScope, ttl time.Duration) {
	b.prov = prov
	b.scope = scope
	b.ttl = ttl
	b.calls++
}

func testPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
	}
}

func TestCreatePod_ProvisionsAndTracks(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")
	pod.Annotations = map[string]string{nebulav1alpha1.CapacityTypeAnnotation: string(nebulav1alpha1.CapacitySpot)}

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if fp.provisionCnt != 1 {
		t.Fatalf("expected 1 provision, got %d", fp.provisionCnt)
	}
	if fp.lastReq.ClaimName != "default-p1" {
		t.Fatalf("expected claim name default-p1, got %q", fp.lastReq.ClaimName)
	}
	if fp.lastReq.CapacityType != nebulav1alpha1.CapacitySpot {
		t.Fatalf("expected capacity type read from annotation, got %q", fp.lastReq.CapacityType)
	}

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodPending {
		t.Fatalf("expected Pending after provision, got %q", got.Status.Phase)
	}
}

func TestCreatePod_ProvisionErrorSurfaces(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity")}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")

	if err := h.CreatePod(context.Background(), pod); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	// The pod is tracked with a Failed status so state is observable / retriable.
	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodFailed {
		t.Fatalf("expected Failed after provision error, got %q", got.Status.Phase)
	}
}

func TestCreatePod_ProvisionFailureRecordsBlock(t *testing.T) {
	accel, region := "H100", "us-east-1"
	scope := provider.BlockScope{
		AcceleratorType: &accel,
		CapacityType:    nebulav1alpha1.CapacitySpot,
		Region:          &region,
	}
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: scope}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, bl)

	pod := testPod("default", "p1")
	pod.Annotations = map[string]string{nebulav1alpha1.BlocklistTTLAnnotation: "3m"}
	pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: "H100"}

	if err := h.CreatePod(context.Background(), pod); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.calls != 1 {
		t.Fatalf("expected exactly one Record call, got %d", bl.calls)
	}
	// The handler must resolve the accelerator off the Pod and hand it to the
	// provider (which owns scope assembly), not mutate the returned scope itself.
	if fp.classifyAccel != "H100" {
		t.Fatalf("handler passed accelerator %q to ClassifyProvisionError, want H100", fp.classifyAccel)
	}
	if bl.prov != "fake" {
		t.Fatalf("recorded provider = %q, want fake", bl.prov)
	}
	if bl.scope != scope {
		t.Fatalf("recorded scope = %+v, want %+v", bl.scope, scope)
	}
	// TTL comes from the Pod annotation the placement controller stamped.
	if bl.ttl != 3*time.Minute {
		t.Fatalf("recorded ttl = %v, want 3m from the annotation", bl.ttl)
	}
}

func TestCreatePod_EmptyScopeDoesNotBlock(t *testing.T) {
	// A classifier that yields the zero scope must NOT install a wildcard block
	// (which would exclude everything on the provider).
	fp := &fakeProvider{provisionErr: errors.New("weird"), classifyScope: provider.BlockScope{}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, bl)

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.calls != 0 {
		t.Fatalf("expected no Record call for an empty scope, got %d", bl.calls)
	}
}

func TestCreatePod_MissingTTLAnnotationUsesDefault(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: provider.BlockScope{DenyAll: true}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, bl)

	// No BlocklistTTLAnnotation on the Pod => the handler's built-in default.
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.ttl != defaultBlocklistTTL {
		t.Fatalf("recorded ttl = %v, want default %v", bl.ttl, defaultBlocklistTTL)
	}
}

func TestDeletePod_TerminatesAndUntracks(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if err := h.DeletePod(context.Background(), pod); err != nil {
		t.Fatalf("DeletePod: %v", err)
	}
	if fp.terminateCnt != 1 {
		t.Fatalf("expected 1 terminate, got %d", fp.terminateCnt)
	}
	if fp.terminateID != "inst-1" {
		t.Fatalf("expected terminate of recorded instance id, got %q", fp.terminateID)
	}
	if _, err := h.GetPod(context.Background(), "default", "p1"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound after delete, got %v", err)
	}
}

func TestDeletePod_Idempotent(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")
	_ = h.CreatePod(context.Background(), pod)

	_ = h.DeletePod(context.Background(), pod)
	// A second DeletePod (VK may call more than once) must not error; Terminate is
	// idempotent and there is no longer a tracked instance id.
	if err := h.DeletePod(context.Background(), pod); err != nil {
		t.Fatalf("second DeletePod: %v", err)
	}
}

func TestReconcileOnce_ReportsRunning(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")
	_ = h.CreatePod(context.Background(), pod)

	// Capture notifications.
	var mu sync.Mutex
	var notified []*corev1.Pod
	h.NotifyPods(context.Background(), func(p *corev1.Pod) {
		mu.Lock()
		notified = append(notified, p)
		mu.Unlock()
	})

	// Provider now reports the instance running under the derived claim name.
	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning, Endpoint: "5.6.7.8",
	}}
	h.reconcileOnce(context.Background())

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected Running after poll, got %q", got.Status.Phase)
	}
	if got.Status.PodIP != "5.6.7.8" {
		t.Fatalf("expected endpoint set as PodIP, got %q", got.Status.PodIP)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notified) == 0 {
		t.Fatal("expected a status notification on the running transition")
	}
}

func TestReconcileOnce_AbsentInstanceIsTerminated(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	pod := testPod("default", "p1")
	_ = h.CreatePod(context.Background(), pod)

	// Provider list is empty => the instance disappeared => reported Terminated.
	fp.list = nil
	h.reconcileOnce(context.Background())

	got, _ := h.GetPod(context.Background(), "default", "p1")
	if got.Status.Phase != corev1.PodFailed {
		t.Fatalf("expected Failed when instance absent, got %q", got.Status.Phase)
	}
	if got.Status.Reason != "Terminated" {
		t.Fatalf("expected Terminated reason, got %q", got.Status.Reason)
	}
}

func TestNewHandler_PollIntervalFromCapabilities(t *testing.T) {
	// A provider that declares a cadence overrides the default.
	custom := &fakeProvider{capabilities: provider.Capabilities{PollInterval: 5 * time.Second}}
	if got := NewHandler(custom, nil).pollEvery; got != 5*time.Second {
		t.Fatalf("expected the provider's PollInterval, got %v", got)
	}
	// A provider that leaves it zero falls back to the vnode default.
	if got := NewHandler(&fakeProvider{}, nil).pollEvery; got != defaultPollInterval {
		t.Fatalf("expected the default cadence, got %v", got)
	}
}

func TestGetPods_ReturnsTracked(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil)
	_ = h.CreatePod(context.Background(), testPod("default", "p1"))
	_ = h.CreatePod(context.Background(), testPod("default", "p2"))

	pods, err := h.GetPods(context.Background())
	if err != nil {
		t.Fatalf("GetPods: %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("expected 2 tracked pods, got %d", len(pods))
	}
}
