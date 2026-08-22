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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
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
	// provisionURL/provisionToken are the connect credential Provision returns — the
	// one-shot value the handler must persist, since it can never be re-read.
	provisionURL   string
	provisionToken string
	provisionErr   error
	provisionCnt   int
	lastReq        provider.ProvisionRequest
	terminateCnt   int
	terminateID    string
	terminateErr   error
	list           []provider.Instance
	listErr        error
	capabilities   provider.Capabilities
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
	// provisionBlocksUntilDeadline makes Provision consume its whole ctx budget before
	// returning successfully, the way a real backend that answers just under the
	// timeout does. It exists to prove the provision deadline does not leak into the
	// writes that follow.
	provisionBlocksUntilDeadline bool
}

func (f *fakeProvider) Name() string                        { return "fake" }
func (f *fakeProvider) Capabilities() provider.Capabilities { return f.capabilities }

func (f *fakeProvider) Provision(
	ctx context.Context, _ *corev1.Pod, req provider.ProvisionRequest,
) (provider.ProvisionResult, error) {
	// Outside the lock: the hook reads Handler state, and holding f.mu here would
	// deadlock a hook that touches the provider.
	if f.provisionHook != nil {
		f.provisionHook()
	}
	if f.provisionBlocksUntilDeadline {
		<-ctx.Done() // burn the provision budget, then succeed anyway
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.provisionCnt++
	f.lastReq = req
	if f.provisionErr != nil {
		return provider.ProvisionResult{}, f.provisionErr
	}
	return provider.ProvisionResult{
		InstanceID:   f.provisionID,
		Reserved:     f.provisionReserved,
		ConnectURL:   f.provisionURL,
		ConnectToken: f.provisionToken,
	}, nil
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
func (f *fakeProvider) ExpandRegions(declared []string) []string               { return declared }
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

// testPoolName is the pool every testPod is placed against. Provisioning resolves the
// pool by this label, so a Pod without it cannot be provisioned at all (see egressFor).
const testPoolName = "pool-a"

func testPod(ns, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ns, Name: name,
			Labels: map[string]string{nebulav1alpha1.PoolLabel: testPoolName},
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "img"}}},
	}
}

// fakePools stands in for the manager's NodePool cache, serving pools from a map so a test
// can pin the policy the handler is supposed to read (and count that it read it).
type fakePools struct {
	pools map[string]*nebulav1alpha1.NodePool
	err   error // when set, every Get fails with it
	calls int
}

func (f *fakePools) Get(_ context.Context, name string) (*nebulav1alpha1.NodePool, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	pool, ok := f.pools[name]
	if !ok {
		return nil, apierrors.NewNotFound(
			schema.GroupResource{Group: nebulav1alpha1.GroupVersion.Group, Resource: "nodepools"}, name)
	}
	return pool, nil
}

// testPools serves testPoolName with the given policy; nil is an unrestricted pool, which
// is what a pool that never set spec.egress means.
func testPools(egress *nebulav1alpha1.EgressPolicy) *fakePools {
	return &fakePools{pools: map[string]*nebulav1alpha1.NodePool{
		testPoolName: {
			ObjectMeta: metav1.ObjectMeta{Name: testPoolName},
			Spec:       nebulav1alpha1.NodePoolSpec{Egress: egress},
		},
	}}
}

// openPools is the default for the many tests that provision without caring about egress.
func openPools() *fakePools { return testPools(nil) }

// poolsWithTTL serves testPoolName with an explicit failover TTL and no egress policy, for
// the blocklist tests: the TTL is pool policy, read when a block is recorded.
func poolsWithTTL(ttl time.Duration) *fakePools {
	pools := openPools()
	pools.pools[testPoolName].Spec.Failover = &nebulav1alpha1.FailoverPolicy{
		BlocklistTTL: metav1.Duration{Duration: ttl},
	}
	return pools
}

func TestCreatePod_ProvisionsAndTracks(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())

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
	h := NewHandler(fp, nil, nil, openPools())

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
	h := NewHandler(fp, nil, nil, openPools())
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

// An error the provider never attributed to the request is not a rejection, so it
// must not be acted on like one: no terminal status (the request may have been
// accepted, and a Failed Pod is reaped out from under a paid instance), no blocklist
// entry against a candidate that never misbehaved, and no tracking (a tracked pod with
// no instance id is written Terminated by the very next poll tick). The Pod stays
// Provisioning with the error as its message and VK retries with backoff.
func TestCreatePod_UnattributableErrorLeavesPodProvisioning(t *testing.T) {
	provErr := errors.New("rpc error: code = Unavailable desc = transport is closing")
	// A non-empty classifyScope proves the guard runs BEFORE classification: a provider
	// willing to hand back a blockable scope still must not have one recorded.
	accel := "H100:1"
	fp := &fakeProvider{
		provisionErr:  provErr,
		classifyScope: provider.BlockScope{Accelerator: &accel},
	}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl, openPools())

	var mu sync.Mutex
	var emitted []string
	h.NotifyPods(context.Background(), func(p *corev1.Pod) {
		mu.Lock()
		emitted = append(emitted, string(p.Status.Phase)+"/"+p.Status.Reason)
		mu.Unlock()
	})

	pod := testPod("default", "p1")
	err := h.CreatePod(context.Background(), pod)
	if !errors.Is(err, provErr) {
		t.Fatalf("CreatePod must return the provision error for VK to back off, got %v", err)
	}
	if pod.Status.Phase != corev1.PodPending || pod.Status.Reason != reasonProvisioning {
		t.Fatalf("status = %s/%s, want the non-terminal %s/%s",
			pod.Status.Phase, pod.Status.Reason, corev1.PodPending, reasonProvisioning)
	}
	if !strings.Contains(pod.Status.Message, provErr.Error()) {
		t.Fatalf("expected the error surfaced as the Pod message, got %q", pod.Status.Message)
	}
	if bl.calls != 0 {
		t.Fatalf("expected no blocklist entry for an unattributable error, got %d", bl.calls)
	}
	if len(h.tracked) != 0 {
		t.Fatalf("expected the Pod left untracked, got %d tracked", len(h.tracked))
	}
	mu.Lock()
	defer mu.Unlock()
	// Both emits are the same non-terminal status: the pre-call stamp and the retry
	// message. Neither may be Failed.
	for _, e := range emitted {
		if strings.HasPrefix(e, string(corev1.PodFailed)) {
			t.Fatalf("emitted a terminal status %q for an unattributable error: %v", e, emitted)
		}
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
	// A non-default TTL on the POOL, so this asserts the pool's policy is honored rather
	// than that it happens to equal defaultBlocklistTTL.
	h := NewHandler(fp, nil, bl, poolsWithTTL(7*time.Minute))
	h.jitterFn = func() time.Duration { return 0 } // pin jitter so the base TTL is asserted exactly

	pod := testPod("default", "p1")
	pod.Labels[nebulav1alpha1.AcceleratorTypeLabel] = "H100"

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
	// TTL comes from the pool's FailoverPolicy, read at the moment the block is recorded.
	if bl.ttl != 7*time.Minute {
		t.Fatalf("recorded ttl = %v, want 7m from the pool", bl.ttl)
	}
}

func TestCreatePod_EmptyScopeDoesNotBlock(t *testing.T) {
	// A classifier that yields the zero scope must NOT install a wildcard block
	// (which would exclude everything on the provider).
	fp := &fakeProvider{provisionErr: errors.New("weird"), classifyScope: provider.BlockScope{}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl, openPools())

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if bl.calls != 0 {
		t.Fatalf("expected no Record call for an empty scope, got %d", bl.calls)
	}
}

// TestCreatePod_BlocklistTTLFallsBackToDefault covers the two ways a READABLE pool can fail
// to supply a usable TTL. An unreadable pool is not among them: poolFor fails closed before
// Provision is ever called, so there is no failure to blocklist — which is why the fallback
// is a pure function of the pool rather than a second read that would have to decide.
func TestCreatePod_BlocklistTTLFallsBackToDefault(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pools PoolReader
	}{{
		name:  "pool sets no failover policy",
		pools: openPools(),
	}, {
		// Zero would install a PERMANENT block, so it has to read as "unset".
		name:  "pool sets a zero TTL",
		pools: poolsWithTTL(0),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: provider.BlockScope{DenyAll: true}}
			bl := &recordingBlocklist{}
			h := NewHandler(fp, nil, bl, tc.pools)
			h.jitterFn = func() time.Duration { return 0 } // pin jitter so the base default is exact

			if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
				t.Fatal("expected CreatePod to return the provision error")
			}
			if bl.calls != 1 {
				t.Fatalf("expected the block to be recorded anyway, got %d Record calls", bl.calls)
			}
			if bl.ttl != defaultBlocklistTTL {
				t.Fatalf("recorded ttl = %v, want default %v", bl.ttl, defaultBlocklistTTL)
			}
		})
	}
}

// TestCreatePod_UnreadablePoolBlocksNothing is the other half: a pool that cannot be read
// fails closed, so no instance is requested and — the part worth pinning — no blocklist entry
// is filed either. Blocking a candidate over OUR failure to read a pool would exclude
// serviceable capacity for every tenant sharing the blocklist.
func TestCreatePod_UnreadablePoolBlocksNothing(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: provider.BlockScope{DenyAll: true}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl, &fakePools{err: errors.New("cache not synced")})

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to fail closed on an unreadable pool")
	}
	if fp.provisionCnt != 0 {
		t.Errorf("provision calls = %d, want 0", fp.provisionCnt)
	}
	if bl.calls != 0 {
		t.Errorf("Record calls = %d, want 0; our own read failure must not blocklist a candidate", bl.calls)
	}
}

// The recorded TTL is the base (the pool's, or the default) PLUS the handler's jitter, so
// Pods failing for one scope do not re-probe the freed candidate in lockstep.
func TestCreatePod_BlocklistTTLAddsJitter(t *testing.T) {
	fp := &fakeProvider{provisionErr: errors.New("no capacity"), classifyScope: provider.BlockScope{DenyAll: true}}
	bl := &recordingBlocklist{}
	h := NewHandler(fp, nil, bl, poolsWithTTL(30*time.Second))
	h.jitterFn = func() time.Duration { return 20 * time.Second } // deterministic jitter

	if err := h.CreatePod(context.Background(), testPod("default", "p1")); err == nil {
		t.Fatal("expected CreatePod to return the provision error")
	}
	if want := 30*time.Second + 20*time.Second; bl.ttl != want {
		t.Fatalf("recorded ttl = %v, want base+jitter %v", bl.ttl, want)
	}
}

// The production jitter draw stays within [0, blocklistJitter) — never negative
// (which would shorten the block below its base) and never at/over the bound.
func TestProductionJitterInRange(t *testing.T) {
	h := NewHandler(&fakeProvider{}, nil, nil, openPools())
	for i := 0; i < 1000; i++ {
		j := h.jitterFn()
		if j < 0 || j >= blocklistJitter {
			t.Fatalf("jitter %v out of [0, %v)", j, blocklistJitter)
		}
	}
}

func TestDeletePod_TerminatesAndUntracks(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, client, nil, openPools())
	_ = h.CreatePod(context.Background(), pod)
	// Register the notify wrapper (this is where persistMetadata is injected).
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

func TestCreatePod_PersistsInstanceIDAlongsideEndpoint(t *testing.T) {
	// The instance id is VK's alone — Provision returns it and nothing re-derives it — so
	// it has to reach the Pod for the NodeClaim controller to record status.InstanceID
	// without asking the provider for a full instance list on every reconcile.
	//
	// It rides the SAME patch as the endpoint: both are known the moment Provision returns
	// for a provider that mints an address at create, and a second write here would be a
	// second round trip per Pod on the provisioning path.
	const url = "https://sb-1.modal.host"
	pod := testPod("default", "p1")
	client := fake.NewSimpleClientset(pod)

	var patches int
	client.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		return false, nil, nil // fall through to the tracker so the object updates
	})

	fp := &fakeProvider{provisionID: "inst-1", provisionURL: url, provisionToken: "tok-abc"}
	h := NewHandler(fp, client, nil, openPools())
	// Wrapper registered BEFORE the create, so the create-path emit is the write.
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get patched pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.InstanceIDAnnotation]; got != "inst-1" {
		t.Fatalf("instance id annotation = %q, want inst-1", got)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != url {
		t.Fatalf("endpoint annotation = %q, want %q", got, url)
	}
	if patches != 1 {
		t.Fatalf("endpoint and instance id must ride one patch, got %d patches", patches)
	}

	// A steady pod is re-emitted every tick with both values unchanged: no further write.
	fp.list = []provider.Instance{{ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning}}
	h.reconcileOnce(context.Background())
	if patches != 1 {
		t.Fatalf("unchanged metadata must not re-patch; got %d patches", patches)
	}
}

// connectSecret fetches the connect Secret for a pod, or nil when absent.
func connectSecret(t *testing.T, client *fake.Clientset, ns, podName string) *corev1.Secret {
	t.Helper()
	s, err := client.CoreV1().Secrets(ns).Get(context.Background(), ConnectSecretName(podName), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get connect secret: %v", err)
	}
	return s
}

// secretValue reads a key from either half of a Secret. The API server folds
// StringData into Data on write, but the fake client stores it verbatim, so a test
// asserting on Data alone would only be exercising the fake.
func secretValue(s *corev1.Secret, key string) string {
	if v, ok := s.StringData[key]; ok {
		return v
	}
	return string(s.Data[key])
}

// The credential must land in a Secret and NOT on the Pod: an annotation is
// readable by anyone with `get pod` in the namespace and sits unencrypted in etcd.
// The URL rides along so the pair is usable from one object, and separately goes on
// the Pod's endpoint annotation, which is not secret.
func TestCreatePod_WritesConnectSecret(t *testing.T) {
	const url, token = "https://sb-1.modal.host", "tok-abc"
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	fp := &fakeProvider{provisionID: "inst-1", provisionURL: url, provisionToken: token}
	h := NewHandler(fp, client, nil, openPools())
	// Register the notifier BEFORE CreatePod, as VK does (it wires NotifyPods before
	// any pod work starts). The endpoint reaches the API server through it.
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	got := connectSecret(t, client, "default", "p1")
	if got == nil {
		t.Fatal("expected a connect Secret from the credential Provision returned")
	}
	if v := secretValue(got, "token"); v != token {
		t.Fatalf("secret token = %q, want %q", v, token)
	}
	if v := secretValue(got, "url"); v != url {
		t.Fatalf("secret url = %q, want %q", v, url)
	}
	// ownerReferenced to the Pod BY UID — a name alone is ignored by the GC — so
	// teardown of the Pod reclaims the Secret on every path, including a force delete.
	if len(got.OwnerReferences) != 1 {
		t.Fatalf("ownerReferences = %v, want exactly one (the Pod)", got.OwnerReferences)
	}
	if ref := got.OwnerReferences[0]; ref.Kind != "Pod" || ref.Name != "p1" || ref.UID != "uid-1" {
		t.Fatalf("ownerReference = %+v, want Pod/p1 with the Pod's UID", ref)
	}

	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	// The address IS published on the Pod — that is how a consumer finds it, and how
	// the Sandbox controller reports Status.Endpoint.
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != url {
		t.Fatalf("endpoint annotation = %q, want the connect URL %q", got, url)
	}
	// The token must never reach the Pod itself.
	for k, v := range live.Annotations {
		if strings.Contains(v, token) {
			t.Fatalf("token leaked onto Pod annotation %q", k)
		}
	}
}

// The Secret is written ONCE, on the create path — not per poll tick. This is the
// difference between one write per workload and one per workload per tick forever
// (at 10k pods on a 15s cadence, ~666 writes/sec that can only ever say
// AlreadyExists). It is also the only place it CAN be written: the token is minted
// once and cannot be re-read, so no later tick has anything to write.
func TestConnectSecret_WrittenOnceNotPerTick(t *testing.T) {
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	var creates int
	client.PrependReactor("create", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		creates++
		return false, nil, nil // fall through to the tracker
	})

	fp := &fakeProvider{
		provisionID:    "inst-1",
		provisionURL:   "https://sb-1.modal.host",
		provisionToken: "tok-abc",
	}
	h := NewHandler(fp, client, nil, openPools())
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if creates != 1 {
		t.Fatalf("CreatePod issued %d secret creates, want exactly 1", creates)
	}
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})

	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning,
	}}
	for i := 0; i < 3; i++ {
		h.reconcileOnce(context.Background())
	}
	if creates != 1 {
		t.Fatalf("the poll loop touched the Secret: %d creates after 3 ticks, want 1", creates)
	}
	if got := connectSecret(t, client, "default", "p1"); got == nil ||
		secretValue(got, "token") != "tok-abc" {
		t.Fatalf("secret = %v, want the original credential intact", got)
	}
}

// An endpoint-less workload (a training job, a batch script) gets no credential from
// the provider, and so must get no Secret: creating an empty one would imply a
// reachable surface that does not exist. This is also every AWS instance, whose
// address is observed later rather than minted here.
func TestCreatePod_NoSecretWithoutCredential(t *testing.T) {
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	fp := &fakeProvider{provisionID: "inst-1"} // no URL, no token
	h := NewHandler(fp, client, nil, openPools())
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	if got := connectSecret(t, client, "default", "p1"); got != nil {
		t.Fatalf("expected no connect Secret for a credential-less instance, got %+v", got)
	}
}

// A URL with no token still publishes the address: the endpoint is how the workload
// is found, and a Secret holding only a URL would imply a credential that does not
// exist.
func TestCreatePod_URLWithoutTokenPatchesEndpointOnly(t *testing.T) {
	const url = "https://sb-1.modal.host"
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	fp := &fakeProvider{provisionID: "inst-1", provisionURL: url}
	h := NewHandler(fp, client, nil, openPools())
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != url {
		t.Fatalf("endpoint annotation = %q, want %q", got, url)
	}
	if got := connectSecret(t, client, "default", "p1"); got != nil {
		t.Fatalf("expected no Secret without a token, got %+v", got)
	}
}

// A UID-less Pod would get an ownerReference the GC cannot resolve, so the Secret
// would never be collected — leaking a live credential. Skip instead.
func TestCreateConnectSecret_SkipsUIDLessPod(t *testing.T) {
	client := fake.NewSimpleClientset()
	h := NewHandler(&fakeProvider{}, client, nil, openPools())

	h.createConnectSecret(context.Background(), testPod("default", "p1"), // no UID
		"https://sb-9.modal.host", "tok-abc")

	if got := connectSecret(t, client, "default", "p1"); got != nil {
		t.Fatalf("expected no Secret for a UID-less Pod (it would never be GC'd), got %+v", got)
	}
}

// A failed Provision publishes nothing. There is no credential to publish, and an
// endpoint annotation would advertise an instance that does not exist.
func TestCreatePod_NoCredentialPersistedOnProvisionFailure(t *testing.T) {
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	fp := &fakeProvider{provisionErr: errors.New("no capacity")}
	h := NewHandler(fp, client, nil, openPools())
	if err := h.CreatePod(context.Background(), pod); err == nil {
		t.Fatal("expected CreatePod to fail")
	}

	if got := connectSecret(t, client, "default", "p1"); got != nil {
		t.Fatalf("expected no Secret after a failed Provision, got %+v", got)
	}
	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != "" {
		t.Fatalf("endpoint annotation = %q, want empty after a failed Provision", got)
	}
}

// The endpoint annotation is where the address LIVES, so a restart does not lose it:
// nothing on the read path clears it, and a provider that reports no observed
// endpoint (Modal, which published at create) leaves the stored value alone.
func TestReconcileOnce_EmptyObservedEndpointDoesNotClearAnnotation(t *testing.T) {
	const url = "https://sb-1.modal.host"
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	fp := &fakeProvider{provisionID: "inst-1", provisionURL: url, provisionToken: "tok-abc"}
	h := NewHandler(fp, client, nil, openPools())
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})

	// The instance is observed with NO endpoint, the way Modal reports it.
	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning,
	}}
	h.reconcileOnce(context.Background())

	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != url {
		t.Fatalf("endpoint annotation = %q, want %q retained across a tick that observed none", got, url)
	}
}

// A create-time URL comes from the provider ONCE and is never re-observed (Modal
// reports no endpoint on the read path), so a failed patch cannot be recovered from the
// provider. It is recovered from the tracked Pod instead: CreatePod stamps the address
// there, and every poll tick re-emits it, so persistMetadata keeps patching until one
// write lands — then dedups. Without that, one transient 500 at create leaves the
// workload permanently unreachable.
func TestCreatePod_FailedEndpointPatchIsRetriedByPollLoop(t *testing.T) {
	const url = "https://sb-1.modal.host"
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	var patches int
	failing := true
	client.PrependReactor("patch", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		patches++
		if failing {
			return true, nil, errors.New("boom")
		}
		return false, nil, nil // fall through to the tracker
	})

	fp := &fakeProvider{provisionID: "inst-1", provisionURL: url, provisionToken: "tok-abc"}
	h := NewHandler(fp, client, nil, openPools())
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod must not fail on a failed endpoint patch: %v", err)
	}
	if patches != 1 {
		t.Fatalf("expected CreatePod's emit to attempt one patch, got %d", patches)
	}
	live, err := client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != "" {
		t.Fatalf("precondition: the patch was supposed to fail, but the annotation is %q", got)
	}

	// Modal's read path reports no endpoint, so the retry can only come from what was
	// stamped at create.
	fp.list = []provider.Instance{{
		ID: "inst-1", ClaimName: "default-p1", State: provider.InstanceRunning,
	}}

	// Still failing: the tick retries rather than giving up.
	h.reconcileOnce(context.Background())
	if patches != 2 {
		t.Fatalf("expected the poll tick to retry the patch, got %d patches total", patches)
	}

	failing = false
	h.reconcileOnce(context.Background())
	if patches != 3 {
		t.Fatalf("expected a third patch attempt, got %d", patches)
	}
	live, err = client.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get pod: %v", err)
	}
	if got := live.Annotations[nebulav1alpha1.EndpointAnnotation]; got != url {
		t.Fatalf("endpoint annotation = %q, want the create-time URL %q recovered", got, url)
	}

	// And once it lands, it dedups — the retry must not become a per-tick write.
	h.reconcileOnce(context.Background())
	if patches != 3 {
		t.Fatalf("a landed endpoint must not re-patch; got %d patches", patches)
	}
}

// ctxRecordingClient wraps a clientset to capture the ctx a Secret create receives.
// The fake clientset ignores ctx entirely, so asserting the Secret merely EXISTS would
// pass whether or not the provision deadline leaked; the ctx itself has to be inspected.
// Embedding keeps this to the one method under test.
type ctxRecordingClient struct {
	kubernetes.Interface
	seen *context.Context
}

func (c ctxRecordingClient) CoreV1() corev1client.CoreV1Interface {
	return ctxRecordingCoreV1{c.Interface.CoreV1(), c.seen}
}

type ctxRecordingCoreV1 struct {
	corev1client.CoreV1Interface
	seen *context.Context
}

func (c ctxRecordingCoreV1) Secrets(ns string) corev1client.SecretInterface {
	return ctxRecordingSecrets{c.CoreV1Interface.Secrets(ns), c.seen}
}

type ctxRecordingSecrets struct {
	corev1client.SecretInterface
	seen *context.Context
}

func (s ctxRecordingSecrets) Create(
	ctx context.Context, secret *corev1.Secret, opts metav1.CreateOptions,
) (*corev1.Secret, error) {
	*s.seen = ctx
	return s.SecretInterface.Create(ctx, secret, opts)
}

// The provision timeout bounds the Provision CALL, not the writes that follow it. A
// backend that answers just under the deadline would otherwise hand those writes a ctx
// with no budget left, so they would fail with DeadlineExceeded on the SUCCESS path —
// and the Secret is never retried, so the one-shot token would be lost for good.
//
// Only the Secret is at risk: the endpoint patch runs on the notify wrapper's own
// long-lived ctx (see NotifyPods), not on CreatePod's.
func TestCreatePod_ProvisionDeadlineDoesNotLeakIntoCredentialWrite(t *testing.T) {
	const url, token = "https://sb-1.modal.host", "tok-abc"
	pod := testPod("default", "p1")
	pod.UID = "uid-1"
	client := fake.NewSimpleClientset(pod)

	var secretCtx context.Context
	fp := &fakeProvider{
		provisionID:                  "inst-1",
		provisionURL:                 url,
		provisionToken:               token,
		provisionBlocksUntilDeadline: true,
		// A tiny budget the call is guaranteed to exhaust.
		capabilities: provider.Capabilities{ProvisionTimeout: time.Millisecond},
	}
	h := NewHandler(fp, ctxRecordingClient{client, &secretCtx}, nil, openPools())
	h.NotifyPods(context.Background(), func(*corev1.Pod) {})
	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	if secretCtx == nil {
		t.Fatal("the Secret write never ran")
	}
	if err := secretCtx.Err(); err != nil {
		t.Fatalf("the Secret write got an already-expired ctx (%v): the provision deadline "+
			"leaked into it, and this token can never be re-minted", err)
	}
	if got := connectSecret(t, client, "default", "p1"); got == nil ||
		secretValue(got, "token") != token {
		t.Fatalf("connect Secret = %v, want the token %q", got, token)
	}
}

// setEndpoint ignores an empty endpoint rather than clearing it. That is what lets the

// setEndpoint ignores an empty endpoint rather than clearing it. That is what lets the
// create-time and observed paths coexist: Modal publishes its URL at create and then
// reports no endpoint at all, which must not erase the address.
func TestSetEndpoint_EmptyValueDoesNotClear(t *testing.T) {
	pod := testPod("default", "p1")
	pod.Annotations = map[string]string{nebulav1alpha1.EndpointAnnotation: "https://sb-1.modal.host"}

	setEndpoint(pod, "")

	if got := pod.Annotations[nebulav1alpha1.EndpointAnnotation]; got != "https://sb-1.modal.host" {
		t.Fatalf("endpoint = %q, want the previous value retained", got)
	}
}

func TestReconcileOnce_NotifiesOnProvisioningToInitializing(t *testing.T) {
	// The Pod starts at "Provisioning" (phase Pending), and the instance comes up
	// but has not yet passed its readiness checks => InstancePending, which maps to
	// the "Initializing" reason at the SAME phase (Pending). A phase-only change
	// check would swallow this and strand the Pod on the stale "Provisioning"
	// reason; the reason must move and a notification must fire.
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	h := NewHandler(fp, nil, nil, openPools())
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
	if got := NewHandler(custom, nil, nil, openPools()).pollEvery; got != 5*time.Second {
		t.Fatalf("expected the provider's PollInterval, got %v", got)
	}
	// A provider that leaves it zero falls back to the vnode default.
	if got := NewHandler(&fakeProvider{}, nil, nil, openPools()).pollEvery; got != defaultPollInterval {
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
	h := NewHandler(fp, nil, nil, openPools())

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

// A List error means "we do not know whether an instance exists", which must NOT be
// reported as NotFound. VK discards GetPod's error and branches on nil-ness alone, so
// a nil pod re-issues CreatePod against an instance that may already be running — the
// path that reaps a live workload after a restart. A non-nil pod suppresses the create;
// the non-NotFound error makes VK's delete path requeue instead of terminating.
func TestGetPod_ListErrorIsUnknownNotAbsent(t *testing.T) {
	fp := &fakeProvider{listErr: errors.New("rpc error: code = Unavailable")}
	h := NewHandler(fp, nil, nil, openPools())

	got, err := h.GetPod(context.Background(), "default", "p1")
	if err == nil {
		t.Fatal("expected an error when the provider cannot be listed")
	}
	if errdefs.IsNotFound(err) {
		t.Fatalf("a List failure must not read as NotFound (VK would then CreatePod): %v", err)
	}
	if got == nil {
		t.Fatal("expected a non-nil Pod so VK takes the adopt branch instead of creating")
	}
	if got.Namespace != "default" || got.Name != "p1" {
		t.Fatalf("stub Pod identity = %s/%s, want default/p1", got.Namespace, got.Name)
	}
	// Nothing may be tracked off a failed List: a tracked pod with no instance id would
	// be found absent from the next List and written Terminated.
	if len(h.tracked) != 0 {
		t.Fatalf("expected nothing tracked after a failed List, got %d", len(h.tracked))
	}

	// Once the provider answers, the same call re-adopts normally.
	fp.listErr = nil
	fp.list = []provider.Instance{{
		ID: "inst-9", ClaimName: "default-p1", State: provider.InstanceRunning,
	}}
	got, err = h.GetPod(context.Background(), "default", "p1")
	if err != nil {
		t.Fatalf("GetPod after the provider recovered: %v", err)
	}
	if got.Status.Phase != corev1.PodRunning {
		t.Fatalf("expected re-adoption once List succeeds, got %q", got.Status.Phase)
	}
}

func TestGetPod_UnknownClaimStaysNotFound(t *testing.T) {
	// No tracking and no live instance for this claim => genuinely absent. GetPod
	// must report NotFound so VK creates it, not silently adopt a phantom.
	fp := &fakeProvider{list: []provider.Instance{{
		ID: "inst-1", ClaimName: "default-other", State: provider.InstanceRunning,
	}}}
	h := NewHandler(fp, nil, nil, openPools())

	if _, err := h.GetPod(context.Background(), "default", "p1"); !errdefs.IsNotFound(err) {
		t.Fatalf("expected NotFound for an unknown, unlisted claim, got %v", err)
	}
}

func TestGetPods_ReturnsTracked(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, openPools())
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

// TestCreatePod_ReadsEgressPolicyFromPool pins where the policy comes FROM. It reverses the
// earlier decision to reconstruct it from the Pod's annotations: they are writable by anyone
// with patch on the Pod, so the object being contained was deciding its own containment.
func TestCreatePod_ReadsEgressPolicyFromPool(t *testing.T) {
	for _, tc := range []struct {
		name        string
		pool        *nebulav1alpha1.EgressPolicy
		wantMode    nebulav1alpha1.EgressMode
		wantTargets []string
	}{{
		// A pool that never set spec.egress, which is the common case.
		name:     "unset is Open",
		pool:     nil,
		wantMode: nebulav1alpha1.EgressOpen,
	}, {
		name:     "blocked needs no targets",
		pool:     &nebulav1alpha1.EgressPolicy{Mode: nebulav1alpha1.EgressBlocked},
		wantMode: nebulav1alpha1.EgressBlocked,
	}, {
		name: "allowlist targets reach the provider verbatim",
		pool: &nebulav1alpha1.EgressPolicy{
			Mode:    nebulav1alpha1.EgressAllowlist,
			Targets: []string{"10.0.0.0/8", "*.huggingface.co"},
		},
		wantMode:    nebulav1alpha1.EgressAllowlist,
		wantTargets: []string{"10.0.0.0/8", "*.huggingface.co"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakeProvider{provisionID: "inst-1"}
			pools := testPools(tc.pool)
			h := NewHandler(fp, nil, nil, pools)

			if err := h.CreatePod(context.Background(), testPod("default", "p1")); err != nil {
				t.Fatalf("CreatePod: %v", err)
			}
			if pools.calls != 1 {
				t.Errorf("pool reads = %d, want 1; the policy must be read from the pool", pools.calls)
			}
			if got := fp.lastReq.Egress.ModeOrOpen(); got != tc.wantMode {
				t.Errorf("req.Egress mode = %q, want %q", got, tc.wantMode)
			}
			if got := fp.lastReq.Egress.GetTargets(); !slices.Equal(got, tc.wantTargets) {
				t.Errorf("req.Egress targets = %v, want %v", got, tc.wantTargets)
			}
		})
	}
}

// TestCreatePod_FailsClosedWithoutAPool covers the three ways the trusted state can be
// missing. None may fall back to a default: the pool IS the policy, so an unreadable pool
// means the policy is unknown, and provisioning under an unknown containment policy is the
// failure this whole path exists to prevent. Nothing is provisioned, and the Pod carries the
// reason rather than failing silently.
func TestCreatePod_FailsClosedWithoutAPool(t *testing.T) {
	for _, tc := range []struct {
		name  string
		pools PoolReader
		pod   func() *corev1.Pod
	}{{
		// A Pod that never went through placement: a scheduling gate can be removed by
		// anyone who can patch the Pod, so reaching a virtual node proves nothing.
		name:  "no pool label",
		pools: openPools(),
		pod: func() *corev1.Pod {
			pod := testPod("default", "p1")
			delete(pod.Labels, nebulav1alpha1.PoolLabel)
			return pod
		},
	}, {
		name:  "pool does not exist",
		pools: &fakePools{},
		pod:   func() *corev1.Pod { return testPod("default", "p1") },
	}, {
		name:  "no reader wired",
		pools: nil,
		pod:   func() *corev1.Pod { return testPod("default", "p1") },
	}} {
		t.Run(tc.name, func(t *testing.T) {
			fp := &fakeProvider{provisionID: "inst-1"}
			h := NewHandler(fp, nil, nil, tc.pools)
			pod := tc.pod()

			if err := h.CreatePod(context.Background(), pod); err == nil {
				t.Fatal("CreatePod succeeded; an unresolvable egress policy must refuse to provision")
			}
			if fp.provisionCnt != 0 {
				t.Errorf("provision calls = %d, want 0; nothing may be requested", fp.provisionCnt)
			}
			if pod.Status.Reason != nebulav1alpha1.PodReasonConfigError {
				t.Errorf("pod reason = %q, want %q", pod.Status.Reason, nebulav1alpha1.PodReasonConfigError)
			}
			if _, tracked := h.tracked[key(pod.Namespace, pod.Name)]; tracked {
				t.Error("pod is tracked; nothing was provisioned for it")
			}
		})
	}
}
