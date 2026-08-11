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
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// fakeProvider records lifecycle calls and returns canned results so each
// Handler branch can be driven deterministically.
type fakeProvider struct {
	mu          sync.Mutex
	provisionID string
	// provisionReserved is the reserved return: whether the provider committed
	// capacity, not merely accepted the request. It defaults to FALSE (the Modal-like
	// case), so a test that cares about the reserved path must opt in — the zero value
	// should not silently assert the stronger guarantee.
	provisionReserved bool
	provisionErr      error
	provisionCnt      int
	lastReq           provider.ProvisionRequest
	terminateCnt      int
	terminateID       string
	terminateErr      error
	list              []provider.Instance
	listErr           error
	capabilities      provider.Capabilities
	// classifyScope is what ClassifyProvisionError returns for a failure; the zero
	// value (empty scope) means "not blocklistable". classifyAccel/classifyRegion
	// record what the handler passed in, so a test can assert it resolved them off the
	// Pod (the provider now owns the whole scope; the handler only supplies these).
	classifyScope  provider.BlockScope
	classifyAccel  string
	classifyRegion string
	// provisionHook runs inside Provision, before it returns, so a test can observe
	// what the handler published for the window in which the call is still in flight.
	provisionHook func()
}

func (f *fakeProvider) Name() string                        { return "fake" }
func (f *fakeProvider) Capabilities() provider.Capabilities { return f.capabilities }

func (f *fakeProvider) Provision(
	_ context.Context, _ *corev1.Pod, req provider.ProvisionRequest,
) (string, bool, error) {
	// Outside the lock: the hook reads Handler state, and holding f.mu here would
	// deadlock a hook that touches the provider.
	if f.provisionHook != nil {
		f.provisionHook()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisionCnt++
	f.lastReq = req
	return f.provisionID, f.provisionReserved, f.provisionErr
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
func (f *fakeProvider) MapAccelerator(c string, _ int32) ([]string, bool)      { return []string{c}, true }
func (f *fakeProvider) ClassifyProvisionError(_ error, accel, region string) provider.BlockScope {
	f.classifyAccel = accel
	f.classifyRegion = region
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
	h := NewHandler(fp, nil, nil)
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

func TestCreatePod_ReservedAdvancesToInitializing(t *testing.T) {
	// reserved=true (AWS: an instant fleet only returns an id once capacity is
	// allocated) means the instance is committed and booting, so the Pod may leave
	// Provisioning immediately instead of waiting a whole poll tick for the same news.
	fp := &fakeProvider{provisionID: "inst-1", provisionReserved: true}
	h := NewHandler(fp, nil, nil)
	pod := testPod("default", "p1")

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if pod.Status.Reason != reasonInitializing {
		t.Fatalf("reason = %q, want %q for a reserved instance", pod.Status.Reason, reasonInitializing)
	}
	if pod.Status.Phase != corev1.PodPending {
		t.Fatalf("phase = %q, want Pending (Initializing is a Pending reason)", pod.Status.Phase)
	}
	// The tracked copy must carry the advanced status: store deep-copies, so a
	// markStatus after it would be invisible to GetPod and to the poll loop.
	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Reason != reasonInitializing {
		t.Fatalf("tracked reason = %q, want %q (markStatus must precede store)", got.Status.Reason, reasonInitializing)
	}
}

func TestCreatePod_UnreservedStaysProvisioning(t *testing.T) {
	// reserved=false (Modal: Create returns on control-plane acceptance, the GPU may
	// still be queued) means nothing has been allocated yet, so Provisioning is still
	// exactly true. Advancing to Initializing here would claim a commitment the
	// provider has not made.
	fp := &fakeProvider{provisionID: "sb-1", provisionReserved: false}
	h := NewHandler(fp, nil, nil)
	pod := testPod("default", "p1")

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if pod.Status.Reason != reasonProvisioning {
		t.Fatalf("reason = %q, want %q for an unreserved instance", pod.Status.Reason, reasonProvisioning)
	}
	// It is still TRACKED: an id was returned, so the instance exists and carries the
	// full teardown obligation regardless of whether capacity was committed.
	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("an unreserved instance must still be tracked: %v", err)
	}
	if got.Status.Reason != reasonProvisioning {
		t.Fatalf("tracked reason = %q, want %q", got.Status.Reason, reasonProvisioning)
	}
}

func TestCreatePod_EmitsProvisioningWhileProvisionInFlight(t *testing.T) {
	// Provision can run for minutes (AWS sweeps a region's zones on a capacity error),
	// and until it returns Provisioning is the only explanation the Pod carries — so it
	// must be emitted BEFORE the call, not after, where it would describe a window that
	// has already closed.
	//
	// Emitting an untracked Pod is what makes this safe: the tracked invariant forbids
	// storing one mid-provision, not reporting one.
	fp := &fakeProvider{provisionID: "inst-1", provisionReserved: true}
	h := NewHandler(fp, nil, nil)

	var mu sync.Mutex
	var reasons []string
	h.NotifyPods(context.Background(), func(p *corev1.Pod) {
		mu.Lock()
		reasons = append(reasons, p.Status.Reason)
		mu.Unlock()
	})

	var inFlight []string
	fp.provisionHook = func() {
		mu.Lock()
		inFlight = slices.Clone(reasons)
		mu.Unlock()
	}

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if !slices.Equal(inFlight, []string{reasonProvisioning}) {
		t.Fatalf("emitted %v while Provision was in flight, want exactly [%s]", inFlight, reasonProvisioning)
	}
	if !slices.Equal(reasons, []string{reasonProvisioning, reasonInitializing}) {
		t.Fatalf("emitted %v, want [%s %s]", reasons, reasonProvisioning, reasonInitializing)
	}
}

func TestCreatePod_PollTickDuringProvisionEmitsNoTerminalStatus(t *testing.T) {
	// The tracked invariant, asserted end to end. reconcileOnce maps a tracked pod
	// that is absent from List() to Terminated, so tracking a pod whose Provision is
	// still in flight reports Failed/Terminated over a SUCCEEDING provision.
	//
	// The assertion is on what was EMITTED, not on the final tracked status: the store
	// after a successful Provision overwrites the tracked entry, so the end state looks
	// correct either way. The damage is the emit — VK writes it to the API server, where
	// Pod phases are terminal-sticky, and the NodeClaim then reclaims a live instance.
	fp := &fakeProvider{provisionID: "inst-1", provisionReserved: true}
	h := NewHandler(fp, nil, nil)

	var mu sync.Mutex
	var emitted []string
	h.NotifyPods(context.Background(), func(p *corev1.Pod) {
		mu.Lock()
		emitted = append(emitted, string(p.Status.Phase)+"/"+p.Status.Reason)
		mu.Unlock()
	})

	// A tick lands mid-provision, when the provider has nothing to list yet.
	fp.provisionHook = func() { h.reconcileOnce(context.Background()) }

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, s := range emitted {
		if strings.HasPrefix(s, string(corev1.PodFailed)) || strings.HasPrefix(s, string(corev1.PodSucceeded)) {
			t.Fatalf("emitted %v; a tick during a succeeding Provision must not report a terminal phase", emitted)
		}
	}
	// Exactly CreatePod's own two emits: the mid-provision tick had nothing to report
	// because the pod was not yet tracked.
	want := []string{
		string(corev1.PodPending) + "/" + reasonProvisioning,
		string(corev1.PodPending) + "/" + reasonInitializing,
	}
	if !slices.Equal(emitted, want) {
		t.Fatalf("emitted %v, want %v", emitted, want)
	}
}

func TestCreatePod_ProvisionErrorSurfaces(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity")}
	h := NewHandler(fp, nil, nil)
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
		Accelerator:  &accel,
		CapacityType: nebulav1alpha1.CapacitySpot,
		Region:       &region,
	}
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: scope}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl)
	h.jitterFn = func() time.Duration { return 0 } // pin jitter so the base TTL is asserted exactly

	pod := testPod("default", "p1")
	// A non-default TTL so this asserts the annotation is honored, not that it
	// happens to equal defaultBlocklistTTL.
	pod.Annotations = map[string]string{nebulav1alpha1.BlocklistTTLAnnotation: "7m"}
	pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: "H100"}

	if err := h.CreatePod(context.Background(), pod); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.calls != 1 {
		t.Fatalf("expected exactly one Record call, got %d", bl.calls)
	}
	// The handler must resolve the accelerator POOL identity (type:count) off the Pod
	// and hand it to the provider (which owns scope assembly), not mutate the returned
	// scope itself. The Pod labels H100 with no explicit count, so the pool is "H100:1".
	if fp.classifyAccel != "H100:1" {
		t.Fatalf("handler passed accelerator %q to ClassifyProvisionError, want H100:1", fp.classifyAccel)
	}
	if bl.prov != "fake" {
		t.Fatalf("recorded provider = %q, want fake", bl.prov)
	}
	if bl.scope != scope {
		t.Fatalf("recorded scope = %+v, want %+v", bl.scope, scope)
	}
	// TTL comes from the Pod annotation the placement controller stamped.
	if bl.ttl != 7*time.Minute {
		t.Fatalf("recorded ttl = %v, want 7m from the annotation", bl.ttl)
	}
}

func TestCreatePod_EmptyScopeDoesNotBlock(t *testing.T) {
	// A classifier that yields the zero scope must NOT install a wildcard block
	// (which would exclude everything on the provider).
	fp := &fakeProvider{provisionErr: errors.New("weird"), classifyScope: provider.BlockScope{}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl)

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
	h := NewHandler(fp, nil, bl)
	h.jitterFn = func() time.Duration { return 0 } // pin jitter so the base default is asserted exactly

	// No BlocklistTTLAnnotation on the Pod => the handler's built-in default.
	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.ttl != defaultBlocklistTTL {
		t.Fatalf("recorded ttl = %v, want default %v", bl.ttl, defaultBlocklistTTL)
	}
}

// The recorded TTL is the base (annotation or default) PLUS the handler's jitter,
// so Pods failing for one scope do not re-probe the freed candidate in lockstep.
func TestCreatePod_BlocklistTTLAddsJitter(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: provider.BlockScope{DenyAll: true}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl)
	h.jitterFn = func() time.Duration { return 20 * time.Second } // deterministic jitter

	pod := testPod("default", "p1")
	pod.Annotations = map[string]string{nebulav1alpha1.BlocklistTTLAnnotation: "30s"}
	if err := h.CreatePod(context.Background(), pod); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if want := 30*time.Second + 20*time.Second; bl.ttl != want {
		t.Fatalf("recorded ttl = %v, want base+jitter %v", bl.ttl, want)
	}
}

// The production jitter draw stays within [0, blocklistJitter) — never negative
// (which would shorten the block below its base) and never at/over the bound.
func TestProductionJitterInRange(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil)
	for i := 0; i < 1000; i++ {
		j := h.jitterFn()
		if j < 0 || j >= blocklistJitter {
			t.Fatalf("jitter %v out of [0, %v)", j, blocklistJitter)
		}
	}
}

func TestDeletePod_TerminatesAndUntracks(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil)
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
	h := NewHandler(fp, nil, nil)
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
	h := NewHandler(fp, nil, nil)
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

	if got.Annotations[nebulav1alpha1.EndpointAnnotation] != "5.6.7.8" {
		t.Fatalf("expected endpoint annotation set, got %q", got.Annotations[nebulav1alpha1.EndpointAnnotation])
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notified) == 0 {
		t.Fatal("expected a status notification on the running transition")
	}
}

func TestReconcileOnce_DNSEndpointNotWrittenToPodIP(t *testing.T) {
	// AWS reports a public DNS name as the endpoint. PodIP is validated by the API
	// server as a literal IP, so a DNS name there fails the whole status write; it
	// must be left empty. The reachable address is surfaced on the annotation
	// instead, which accepts any form.
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil)
	pod := testPod("default", "p1")
	_ = h.CreatePod(context.Background(), pod)

	const dns = "ec2-54-161-33-206.compute-1.amazonaws.com"
	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning, Endpoint: dns,
	}}
	h.reconcileOnce(context.Background())

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected Running, got %q", got.Status.Phase)
	}
	if got.Status.PodIP != "" {
		t.Fatalf("a DNS endpoint must not be written to PodIP, got %q", got.Status.PodIP)
	}
	if got.Annotations[nebulav1alpha1.EndpointAnnotation] != dns {
		t.Fatalf("expected DNS endpoint on the annotation, got %q", got.Annotations[nebulav1alpha1.EndpointAnnotation])
	}
}

func TestNotify_PersistsEndpointAnnotationOnce(t *testing.T) {
	// The endpoint must reach the API-server Pod metadata (VK writes only status),
	// so the notify wrapper issues a metadata patch. It must fire when the endpoint
	// first appears and then dedup: a steady Running pod re-emitted every tick must
	// not re-patch.
	const dns = "ec2-54-161-33-206.compute-1.amazonaws.com"
	pod := testPod("default", "p1")
	client := fake.NewSimpleClientset(pod)

	var patches int
	client.PrependReactor("patch", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil // fall through to the tracker so the object updates
	})

	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, client, nil)
	_ = h.CreatePod(context.Background(), pod)
	// Register the notify wrapper (this is where persistEndpoint is injected).
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})

	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning, Endpoint: dns,
	}}

	// First tick: endpoint appears -> one patch.
	h.reconcileOnce(context.Background())
	if patches != 1 {
		t.Fatalf("expected exactly one patch when the endpoint first appears, got %d", patches)
	}

	// The annotation landed on the API-server object.
	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if live.Annotations[nebulav1alpha1.EndpointAnnotation] != dns {
		t.Fatalf("expected endpoint persisted to API server, got %q", live.Annotations[nebulav1alpha1.EndpointAnnotation])
	}

	// Second tick with the same endpoint: deduped, no additional patch.
	h.reconcileOnce(context.Background())
	if patches != 1 {
		t.Fatalf("an unchanged endpoint must not re-patch; got %d patches", patches)
	}
}

func TestReconcileOnce_NotifiesOnProvisioningToInitializing(t *testing.T) {
	// The Pod starts at "Provisioning" (phase Pending), and the instance comes up
	// but has not yet passed its readiness checks => InstancePending, which maps to
	// the "Initializing" reason at the SAME phase (Pending). A phase-only change
	// check would swallow this and strand the Pod on the stale "Provisioning"
	// reason; the reason must move and a notification must fire.
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil)
	pod := testPod("default", "p1")
	_ = h.CreatePod(context.Background(), pod)

	// Sanity: CreatePod left it at the Provisioning reason (still Pending).
	if got, _ := h.GetPod(context.Background(), "default", "p1"); got.Status.Reason != reasonProvisioning {
		t.Fatalf("precondition: expected %q, got %q", reasonProvisioning, got.Status.Reason)
	}

	var mu sync.Mutex
	var notified []*corev1.Pod
	h.NotifyPods(context.Background(), func(p *corev1.Pod) {
		mu.Lock()
		notified = append(notified, p)
		mu.Unlock()
	})

	// Instance is up but not yet reachable (2/2 checks pending) => InstancePending.
	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstancePending,
	}}
	h.reconcileOnce(context.Background())

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodPending {
		t.Fatalf("expected phase to stay Pending, got %q", got.Status.Phase)
	}
	if got.Status.Reason != reasonInitializing {
		t.Fatalf("expected reason to advance to %q, got %q", reasonInitializing, got.Status.Reason)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(notified) == 0 {
		t.Fatal("expected a notification on the Provisioning->Initializing reason change (same phase)")
	}
}

func TestReconcileOnce_AbsentInstanceIsTerminated(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil)
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

func TestReconcileOnce_ListErrorLeavesStatusUntouched(t *testing.T) {
	// A List error must not advance anything. It means the fleet is half-known, and
	// the only unsafe reading is "absent" — which maps to Terminated, a terminal
	// phase the Pod can never leave.
	//
	// This is load-bearing beyond transport failures: the Modal adapter deliberately
	// FAILS List when a sandbox's tags are unreadable, because a sandbox with no
	// recoverable ClaimName cannot be reported at all (an empty claim and an omitted
	// sandbox both read as absent here). That choice is only safe because of this.
	fp := &fakeProvider{provisionID: "inst-1", provisionReserved: true}
	h := NewHandler(fp, nil, nil)
	_ = h.CreatePod(context.Background(), testPod("default", "p1"))

	var mu sync.Mutex
	var emitted int
	h.NotifyPods(context.Background(), func(*corev1.Pod) {
		mu.Lock()
		emitted++
		mu.Unlock()
	})

	fp.list, fp.listErr = nil, errors.New("tags unreadable")
	h.reconcileOnce(context.Background())

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	if got.Status.Phase != corev1.PodPending || got.Status.Reason != reasonInitializing {
		t.Fatalf("status = %s/%s after a failed List, want the pre-tick Pending/%s",
			got.Status.Phase, got.Status.Reason, reasonInitializing)
	}
	mu.Lock()
	defer mu.Unlock()
	if emitted != 0 {
		t.Fatalf("emitted %d notifications on a failed List, want 0", emitted)
	}
}

func TestNewHandler_PollIntervalFromCapabilities(t *testing.T) {
	// A provider that declares a cadence overrides the default.
	custom := &fakeProvider{capabilities: provider.Capabilities{PollInterval: 5 * time.Second}}
	if got := NewHandler(custom, nil, nil).pollEvery; got != 5*time.Second {
		t.Fatalf("expected the provider's PollInterval, got %v", got)
	}
	// A provider that leaves it zero falls back to the vnode default.
	if got := NewHandler(&fakeProvider{}, nil, nil).pollEvery; got != defaultPollInterval {
		t.Fatalf("expected the default cadence, got %v", got)
	}
}

func TestGetPod_ReAdoptsLiveInstanceAfterRestart(t *testing.T) {
	// Simulate a VK restart: the tracking map is cold (no CreatePod ran this
	// process), but the external instance for the Pod's claim is still running. A
	// GetPod must re-adopt it from the provider's List and report its true state,
	// so VK takes the adopt path instead of re-issuing CreatePod (which would reset
	// the Pod to Provisioning). This is the fix for "stuck on Provisioning while the
	// instance is actually running".
	fp := &fakeProvider{list: []provider.Instance{{
		ID: "inst-9", ClaimName: "default-p1", State: provider.InstanceRunning, Endpoint: "1.2.3.4",
	}}}
	h := NewHandler(fp, nil, nil)

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod after restart: %v", err)
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected re-adopted Pod reported Running, got %q", got.Status.Phase)
	}
	if got.Status.PodIP != "1.2.3.4" {
		t.Fatalf("expected endpoint adopted as PodIP, got %q", got.Status.PodIP)
	}

	// It must now be tracked, so the poll loop advances it and DeletePod can find
	// the instance id to terminate.
	if _, err := h.GetPod(context.Background(), "default", "p1"); err != nil {
		t.Fatalf("expected the re-adopted Pod to be tracked: %v", err)
	}
}

func TestGetPod_UnknownClaimStaysNotFound(t *testing.T) {
	// No tracking and no live instance for this claim => genuinely absent. GetPod
	// must report NotFound so VK creates it, not silently adopt a phantom.
	fp := &fakeProvider{list: []provider.Instance{{
		ID: "inst-1", ClaimName: "default-other", State: provider.InstanceRunning,
	}}}
	h := NewHandler(fp, nil, nil)

	if _, err := h.GetPod(context.Background(), "default", "p1"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound for an unknown, unlisted claim, got %v", err)
	}
}

func TestGetPods_ReturnsTracked(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil)
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
