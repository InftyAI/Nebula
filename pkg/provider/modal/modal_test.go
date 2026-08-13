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

package modal

import (
	"context"
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// fakeClient is an in-memory Client for tests. It records the last CreateSandbox
// spec and lets tests seed existing sandboxes and inject errors.
type fakeClient struct {
	sandboxes  []Sandbox
	lastSpec   SandboxSpec
	createCnt  int
	createErr  error
	createID   string
	cred       Credential // credential CreateSandbox returns; zero = none minted
	terminated []string
}

func (f *fakeClient) CreateSandbox(_ context.Context, spec SandboxSpec) (string, Credential, error) {
	f.createCnt++
	f.lastSpec = spec
	if f.createErr != nil {
		return "", Credential{}, f.createErr
	}
	id := f.createID
	if id == "" {
		id = "sb-new"
	}
	f.sandboxes = append(f.sandboxes, Sandbox{ID: id, Tags: spec.Tags, Status: "pending"})
	return id, f.cred, nil
}

func (f *fakeClient) TerminateSandbox(_ context.Context, id string) error {
	f.terminated = append(f.terminated, id)
	return nil
}

func (f *fakeClient) GetSandbox(_ context.Context, id string) (*Sandbox, error) {
	for i := range f.sandboxes {
		if f.sandboxes[i].ID == id {
			s := f.sandboxes[i]
			return &s, nil
		}
	}
	return nil, nil
}

func (f *fakeClient) ListSandboxes(_ context.Context) ([]Sandbox, error) {
	return f.sandboxes, nil
}

// fakeCatalog is a trivial provider.Catalog for tests.
type fakeCatalog struct{ rows []provider.Offering }

func (c fakeCatalog) Offerings(_ string) []provider.Offering { return c.rows }

// newTestProvider builds a Provider with a fake client and a small catalog.
func newTestProvider(f *fakeClient) *Provider {
	return New(f, fakeCatalog{rows: []provider.Offering{
		{AcceleratorType: "H100", CapacityType: nebulav1alpha1.CapacityOnDemand, PricePerHour: 3.95, Available: true},
		{AcceleratorType: "A100-80GB", CapacityType: nebulav1alpha1.CapacityOnDemand, PricePerHour: 2.50, Available: true},
	}})
}

// gpuPod builds a Pod whose accelerator type rides on the accelerator-type label
// and whose count rides on the container's nvidia.com/gpu resource; count<=0
// means CPU-only (no label, no GPU resource). accel is passed through verbatim
// so tests can also exercise non-canonical casing (e.g. "h100").
func gpuPod(claim, accel string, count int64) *corev1.Pod {
	c := corev1.Container{
		Name:    "main",
		Image:   "myimg:latest",
		Command: []string{"run"},
		Args:    []string{"--flag"},
		Env:     []corev1.EnvVar{{Name: "FOO", Value: "bar"}},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{c}},
	}
	if accel != "" && count > 0 {
		pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: accel}
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
			util.NvidiaGPUResource: *resource.NewQuantity(count, resource.DecimalSI),
		}
	}
	return pod
}

func TestProvision_GPUPod(t *testing.T) {
	f := &fakeClient{createID: "sb-1"}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 2), provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	id, reserved := res.InstanceID, res.Reserved
	if id != "sb-1" {
		t.Fatalf("id = %q, want sb-1", id)
	}
	// Create only means the control plane ACCEPTED the sandbox — the GPU may still be
	// queued — so a fresh sandbox is never reserved. Claiming otherwise would let the
	// Pod report Initializing while nothing has been allocated.
	if reserved {
		t.Fatal("reserved = true; a freshly created sandbox may still be queued for capacity")
	}
	if f.lastSpec.GPU != "H100" || f.lastSpec.GPUCount != 2 {
		t.Fatalf("spec GPU=%q count=%d, want H100/2", f.lastSpec.GPU, f.lastSpec.GPUCount)
	}
	if f.lastSpec.Image != "myimg:latest" {
		t.Fatalf("image = %q", f.lastSpec.Image)
	}
	if got := f.lastSpec.Tags[ClaimTagKey]; got != "claim-a" {
		t.Fatalf("claim tag = %q, want claim-a", got)
	}
	if len(f.lastSpec.Command) != 2 || f.lastSpec.Command[0] != "run" {
		t.Fatalf("command = %v", f.lastSpec.Command)
	}
}

func TestProvision_LowercaseGPUAnnotation(t *testing.T) {
	f := &fakeClient{createID: "sb-lc"}
	p := newTestProvider(f)

	// A user may write the accelerator-type label in any case (e.g. "h100"). It must
	// resolve to the canonical catalog accelerator ("H100") so the provisioned
	// sandbox — and any downstream key (blocklist/catalog) — uses one casing.
	_, err := p.Provision(context.Background(), gpuPod("claim-lc", "h100", 1), provider.ProvisionRequest{
		ClaimName:    "claim-lc",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.GPU != "H100" {
		t.Fatalf("spec GPU = %q, want canonical H100 from a lowercase annotation", f.lastSpec.GPU)
	}
}

func TestProvision_MapsResourcesPortsAndTimeout(t *testing.T) {
	f := &fakeClient{createID: "sb-res"}
	p := newTestProvider(f)

	pod := gpuPod("claim-res", "H100", 1)
	c := &pod.Spec.Containers[0]
	c.Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("2500m"),
			corev1.ResourceMemory: resource.MustParse("4Gi"),
		},
	}
	c.Ports = []corev1.ContainerPort{{ContainerPort: 8000}, {ContainerPort: 9090}}
	deadline := int64(3600)
	pod.Spec.ActiveDeadlineSeconds = &deadline

	if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{ClaimName: "claim-res"}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.CPU != 2.5 {
		t.Fatalf("CPU = %v, want 2.5", f.lastSpec.CPU)
	}
	if f.lastSpec.MemoryMiB != 4096 {
		t.Fatalf("MemoryMiB = %d, want 4096", f.lastSpec.MemoryMiB)
	}
	if len(f.lastSpec.Ports) != 2 || f.lastSpec.Ports[0] != 8000 || f.lastSpec.Ports[1] != 9090 {
		t.Fatalf("Ports = %v, want [8000 9090]", f.lastSpec.Ports)
	}
	if f.lastSpec.Timeout != time.Hour {
		t.Fatalf("Timeout = %v, want 1h (from activeDeadlineSeconds)", f.lastSpec.Timeout)
	}
}

func TestProvision_DefaultsTimeoutWhenNoDeadline(t *testing.T) {
	f := &fakeClient{createID: "sb-dt"}
	p := newTestProvider(f)

	// No activeDeadlineSeconds: the adapter must still set a non-zero timeout, else
	// Modal applies its 5-minute default and the workload dies almost immediately.
	req := provider.ProvisionRequest{ClaimName: "claim-dt"}
	if _, err := p.Provision(context.Background(), gpuPod("claim-dt", "H100", 1), req); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.Timeout != defaultSandboxTimeout {
		t.Fatalf("Timeout = %v, want the long default %v", f.lastSpec.Timeout, defaultSandboxTimeout)
	}
}

func TestProvision_CPUOnly(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)

	_, err := p.Provision(context.Background(), gpuPod("claim-cpu", "", 0), provider.ProvisionRequest{
		ClaimName:    "claim-cpu",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.GPU != "" || f.lastSpec.GPUCount != 0 {
		t.Fatalf("CPU-only spec should have no GPU, got %q/%d", f.lastSpec.GPU, f.lastSpec.GPUCount)
	}
}

func TestProvision_Idempotent(t *testing.T) {
	f := &fakeClient{
		sandboxes: []Sandbox{{
			ID:     "sb-existing",
			Tags:   map[string]string{ClaimTagKey: "claim-a"},
			Status: "running",
		}},
	}
	p := newTestProvider(f)

	req := provider.ProvisionRequest{ClaimName: "claim-a"}
	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1), req)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	id, reserved := res.InstanceID, res.Reserved
	if id != "sb-existing" {
		t.Fatalf("id = %q, want sb-existing (idempotent reuse)", id)
	}
	if f.createCnt != 0 {
		t.Fatalf("CreateSandbox called %d times, want 0 (idempotent)", f.createCnt)
	}
	// An adopted sandbox has been OBSERVED, unlike a fresh create, so its state is
	// known: this one is running, which means capacity was necessarily allocated.
	if !reserved {
		t.Fatal("reserved = false for an adopted RUNNING sandbox; observed state proves capacity was allocated")
	}
}

// A sandbox adopted while still coming up is NOT reserved: "initializing" is what
// Modal reports for both a queued sandbox and a booting one, so it does not prove
// capacity was allocated. Only a running sandbox does.
func TestProvision_IdempotentInitializingIsNotReserved(t *testing.T) {
	f := &fakeClient{
		sandboxes: []Sandbox{{
			ID:     "sb-existing",
			Tags:   map[string]string{ClaimTagKey: "claim-a"},
			Status: statusInitializing,
		}},
	}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1),
		provider.ProvisionRequest{ClaimName: "claim-a"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	id, reserved := res.InstanceID, res.Reserved
	if id != "sb-existing" {
		t.Fatalf("id = %q, want sb-existing (idempotent reuse)", id)
	}
	if reserved {
		t.Fatal("reserved = true for an adopted INITIALIZING sandbox; it may still be queued")
	}
}

func TestProvision_UnsupportedAccelerator(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)
	req := provider.ProvisionRequest{ClaimName: "claim-x"}
	_, err := p.Provision(context.Background(), gpuPod("claim-x", "TPU-v4", 1), req)
	if err == nil {
		t.Fatal("expected error for unsupported accelerator")
	}
}

func TestClassifyProvisionError(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	denyAll := provider.BlockScope{DenyAll: true}
	onDemand := provider.BlockScope{CapacityType: nebulav1alpha1.CapacityOnDemand}
	tests := []struct {
		name string
		err  error
		want provider.BlockScope
	}{
		{"auth sentinel", provider.ErrAuth, denyAll},
		// Quota is scoped like capacity (accelerator + tier), not DenyAll: an
		// exhausted quota for one accelerator must not fence off the whole provider.
		{"quota sentinel", provider.ErrQuota, onDemand},
		{"capacity sentinel", provider.ErrNoCapacity, onDemand},
		{"wrapped capacity", fmt.Errorf("provision: %w", provider.ErrNoCapacity), onDemand},
		{"string unauthorized", fmt.Errorf("401 unauthorized"), denyAll},
		{"string no capacity", fmt.Errorf("no capacity available in region"), onDemand},
		// An unrecognized error is scoped like capacity, not DenyAll: a whole-provider
		// block on an unidentifiable failure is too broad; failover routes around it.
		{"unknown capacity-scoped", fmt.Errorf("weird transient blip"), onDemand},
		{"nil", nil, provider.BlockScope{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.ClassifyProvisionError(tt.err, "", "")
			if got != tt.want {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCapabilities(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	caps := p.Capabilities()
	if caps.SupportsStop || caps.SupportsSpot || caps.PreemptionNotice != 0 || !caps.NativeTags {
		t.Fatalf("unexpected caps: %+v", caps)
	}
	if p.Name() != provider.ProviderModal {
		t.Fatalf("name = %q", p.Name())
	}
}

func TestOfferings(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	offs, err := p.Offerings(context.Background())
	if err != nil {
		t.Fatalf("Offerings: %v", err)
	}
	if len(offs) == 0 {
		t.Fatal("expected non-empty catalog")
	}
	for _, o := range offs {
		if o.CapacityType != nebulav1alpha1.CapacityOnDemand {
			t.Fatalf("offering %q not OnDemand: %v", o.AcceleratorType, o.CapacityType)
		}
	}
}

func TestToState(t *testing.T) {
	// The status→state mapping is load-bearing. observe emits "running" (live AND
	// ready), "initializing" (live but the readiness probe has not passed),
	// "terminated" (exited), or "" (Poll errored); everything else, including the
	// empty string, must map to Pending so the poll loop keeps watching rather than
	// declaring a premature terminal state.
	cases := map[string]provider.InstanceState{
		"running": provider.InstanceRunning,
		// The whole point of the readiness work: a live-but-not-ready sandbox must NOT
		// reach Running, or the Pod (and its Deployment's ready count) advances while
		// the box is still queued/pulling/booting.
		"initializing": provider.InstancePending,
		"pending":      provider.InstancePending,
		"":             provider.InstancePending, // Poll errored, status left unset
		"terminated":   provider.InstanceTerminated,
		// A sandbox that exited nonzero — crashed, or never came up (INIT_FAILURE).
		// Must NOT read as terminated: "gone" looks like a clean teardown and hides
		// the failure.
		"failed":    provider.InstanceFailed,
		"weird-new": provider.InstancePending, // unknown => keep watching, not terminal
	}
	for in, want := range cases {
		if got := toState(in); got != want {
			t.Fatalf("toState(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestExitStatus pins the exit-code classification. Poll collapses eight
// control-plane statuses into one int (see exitStatus), so this split is inference
// and its direction matters: only a clean exit and the two codes Modal SUBSTITUTES
// for a non-exit outcome are "gone"; any other nonzero exit is a real failure.
func TestExitStatus(t *testing.T) {
	cases := map[int]string{
		0:   statusTerminated, // ran to completion
		137: statusTerminated, // Modal terminated it (our Terminate, or Modal's)
		124: statusTerminated, // sandbox Timeout elapsed
		1:   statusFailed,     // the workload crashed
		2:   statusFailed,
		127: statusFailed, // command not found
		// The case this whole split exists for: a sandbox that never started (bad
		// image, no GPU available, OOM at init) must surface as failed, not gone.
		139: statusFailed,
	}
	for code, want := range cases {
		if got := exitStatus(code); got != want {
			t.Fatalf("exitStatus(%d) = %q, want %q", code, got, want)
		}
	}
}

// TestToInstance_ReadinessGatesRunning pins the end-to-end consequence through the
// public surface: only a "running" sandbox becomes InstanceRunning, which is what
// applyState turns into PodRunning + Ready=True.
func TestToInstance_ReadinessGatesRunning(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	cases := []struct {
		status string
		want   provider.InstanceState
	}{
		{statusRunning, provider.InstanceRunning},
		{statusInitializing, provider.InstancePending},
		{statusTerminated, provider.InstanceTerminated},
	}
	for _, tc := range cases {
		got := p.toInstance(Sandbox{ID: "sb-1", Status: tc.status})
		if got.State != tc.want {
			t.Errorf("toInstance(status=%q).State = %q, want %q", tc.status, got.State, tc.want)
		}
	}
}

// TestProvision_ProbeTagStampedOnlyWithProbe: the tag is how observe recovers
// probe-ness after a restart (it cannot be re-derived — see ProbeTagKey), so it
// must be present exactly when the Pod carries a readinessProbe. Stamping it on a
// probe-less sandbox would make observe call WaitUntilReady, which errors on one.
func TestProvision_ProbeTagStampedOnlyWithProbe(t *testing.T) {
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{Command: []string{"true"}},
		},
	}
	for _, tc := range []struct {
		name  string
		probe *corev1.Probe
		want  bool
	}{
		{"with probe", probe, true},
		{"without probe", nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeClient{createID: "sb-1"}
			p := newTestProvider(f)
			pod := gpuPod("claim-a", "H100", 1)
			pod.Spec.Containers[0].ReadinessProbe = tc.probe

			if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{ClaimName: "claim-a"}); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			_, present := f.lastSpec.Tags[ProbeTagKey]
			if present != tc.want {
				t.Errorf("%s tag present = %v, want %v (tags=%v)", ProbeTagKey, present, tc.want, f.lastSpec.Tags)
			}
			if tc.want && f.lastSpec.Tags[ProbeTagKey] != probeTagValue {
				t.Errorf("%s = %q, want %q", ProbeTagKey, f.lastSpec.Tags[ProbeTagKey], probeTagValue)
			}
			// Identity must survive alongside it.
			if f.lastSpec.Tags[ClaimTagKey] != "claim-a" {
				t.Errorf("%s = %q, want claim-a", ClaimTagKey, f.lastSpec.Tags[ClaimTagKey])
			}
		})
	}
}

func TestProvision_ReadinessProbeCarriedThrough(t *testing.T) {
	f := &fakeClient{createID: "sb-1"}
	p := newTestProvider(f)

	pod := gpuPod("claim-a", "H100", 1)
	probe := &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(8000)},
		},
	}
	pod.Spec.Containers[0].ReadinessProbe = probe

	if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{ClaimName: "claim-a"}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	// The probe is carried onto the spec so the Client can configure Modal's own
	// readiness probe; Modal enforces it internally (Nebula does not read it back).
	if f.lastSpec.ReadinessProbe != probe {
		t.Fatalf("ReadinessProbe not carried onto the spec: got %v", f.lastSpec.ReadinessProbe)
	}
}

func TestProvision_NoProbeLeavesSpecUnset(t *testing.T) {
	f := &fakeClient{createID: "sb-1"}
	p := newTestProvider(f)

	req := provider.ProvisionRequest{ClaimName: "claim-a"}
	if _, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1), req); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.ReadinessProbe != nil {
		t.Fatalf("ReadinessProbe should be nil when the Pod declares none, got %v", f.lastSpec.ReadinessProbe)
	}
}

// TestProvision_ConnectPortFromFirstDeclaredPort: ConnectPort selects which port the
// connect URL routes to. Every workload is credentialed, so 0 is not "no endpoint"
// but "let Modal pick" (it defaults to 8080).
func TestProvision_ConnectPortFromFirstDeclaredPort(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ports []corev1.ContainerPort
		want  int
	}{
		{"no ports leaves the port to Modal", nil, 0},
		{"single port", []corev1.ContainerPort{{ContainerPort: 8000}}, 8000},
		// Modal routes one port per token, so the first declared port wins.
		{"first of several wins", []corev1.ContainerPort{{ContainerPort: 8000}, {ContainerPort: 9090}}, 8000},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeClient{createID: "sb-1"}
			p := newTestProvider(f)
			pod := gpuPod("claim-a", "H100", 1)
			pod.Spec.Containers[0].Ports = tc.ports

			req := provider.ProvisionRequest{ClaimName: "claim-a"}
			if _, err := p.Provision(context.Background(), pod, req); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			if f.lastSpec.ConnectPort != tc.want {
				t.Fatalf("ConnectPort = %d, want %d", f.lastSpec.ConnectPort, tc.want)
			}
		})
	}
}

func TestFirstPort(t *testing.T) {
	if got := firstPort(nil); got != 0 {
		t.Fatalf("firstPort(nil) = %d, want 0 (leave the port to Modal)", got)
	}
	if got := firstPort([]int{9090, 8000}); got != 9090 {
		t.Fatalf("firstPort = %d, want the first declared port 9090", got)
	}
}

// The credential reaches the caller through Provision and NOWHERE else: minting is
// one-shot, so this return value is the only copy that will ever exist.
func TestProvision_ReturnsMintedCredential(t *testing.T) {
	f := &fakeClient{
		createID: "sb-1",
		cred:     Credential{URL: "https://x.modal.host", Token: "tok-abc"},
	}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1),
		provider.ProvisionRequest{ClaimName: "claim-a"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if res.ConnectURL != "https://x.modal.host" {
		t.Fatalf("ConnectURL = %q, want the minted URL", res.ConnectURL)
	}
	if res.ConnectToken != "tok-abc" {
		t.Fatalf("ConnectToken = %q, want the minted token", res.ConnectToken)
	}
	// The token must never be written where a reader of the sandbox could find it: the
	// tags are plaintext and one ListSandboxes dumps them all.
	for k, v := range f.lastSpec.Tags {
		if v == "tok-abc" {
			t.Fatalf("token leaked into sandbox tag %q", k)
		}
	}
}

// A sandbox that minted nothing yields no credential rather than an error: it still
// exists, still costs money, and must still be reported and reclaimed.
func TestProvision_NoCredentialWhenNoneMinted(t *testing.T) {
	f := &fakeClient{createID: "sb-1"} // zero cred
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1),
		provider.ProvisionRequest{ClaimName: "claim-a"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if res.InstanceID != "sb-1" {
		t.Fatalf("InstanceID = %q, want sb-1 even with no credential", res.InstanceID)
	}
	if res.ConnectURL != "" || res.ConnectToken != "" {
		t.Fatalf("expected no credential, got url=%q token set=%t", res.ConnectURL, res.ConnectToken != "")
	}
}

// An idempotent re-Provision carries NO credential. The original was minted once and
// cannot be re-read, and minting a second one here would hand the consumer a token
// that changes on every retry.
func TestProvision_IdempotentReturnsNoCredential(t *testing.T) {
	f := &fakeClient{
		sandboxes: []Sandbox{{
			ID:     "sb-existing",
			Tags:   map[string]string{ClaimTagKey: "claim-a"},
			Status: statusRunning,
		}},
		cred: Credential{URL: "https://x.modal.host", Token: "tok-abc"},
	}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1),
		provider.ProvisionRequest{ClaimName: "claim-a"})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if res.InstanceID != "sb-existing" {
		t.Fatalf("InstanceID = %q, want sb-existing", res.InstanceID)
	}
	if res.ConnectURL != "" || res.ConnectToken != "" {
		t.Fatalf("an adopted sandbox must carry no credential, got url=%q token set=%t",
			res.ConnectURL, res.ConnectToken != "")
	}
}

// Modal reports NO observed endpoint. Its address is the connect URL, published from
// the create path onto the Pod's annotation, where it persists; re-deriving it per tick
// would be a round trip for a value the API server already holds. The alternative —
// falling back to a tunnel URL — is worse than nothing, since a tunnel is public to
// whoever learns it.
func TestToInstance_ReportsNoEndpoint(t *testing.T) {
	p := newTestProvider(&fakeClient{})

	got := p.toInstance(Sandbox{
		ID:     "sb-1",
		Status: statusRunning,
		Tags:   map[string]string{ClaimTagKey: "claim-a"},
	})
	if got.Endpoint != "" {
		t.Fatalf("Endpoint = %q, want empty; the address comes from the create path", got.Endpoint)
	}
	if got.ClaimName != "claim-a" || got.State != provider.InstanceRunning {
		t.Fatalf("claim/state = %q/%q, want claim-a/Running", got.ClaimName, got.State)
	}
}

func TestModalProbe(t *testing.T) {
	// nil Pod probe => no Modal probe (probe-less workload).
	if got, err := modalProbe(nil); err != nil || got != nil {
		t.Fatalf("modalProbe(nil) = (%v, %v), want (nil, nil)", got, err)
	}

	// A probe with no supported handler also degrades to no Modal probe rather
	// than an error, so it never wedges provisioning.
	empty := &corev1.Probe{}
	if got, err := modalProbe(empty); err != nil || got != nil {
		t.Fatalf("modalProbe(empty) = (%v, %v), want (nil, nil)", got, err)
	}

	// Supported handlers each produce a Modal probe. HTTPGet degrades to TCP.
	supported := []*corev1.Probe{
		{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(8000)}}},
		{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"true"}}}},
		{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt(80)}}, PeriodSeconds: 5},
	}
	for i, pr := range supported {
		got, err := modalProbe(pr)
		if err != nil {
			t.Fatalf("case %d: modalProbe error: %v", i, err)
		}
		if got == nil {
			t.Fatalf("case %d: expected a Modal probe, got nil", i)
		}
	}

	// A NAMED port cannot be resolved here (needs the container's ports list), so
	// TCP/HTTPGet with a named port omits the probe rather than emitting port 0.
	named := []*corev1.Probe{
		{ProbeHandler: corev1.ProbeHandler{TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("http")}}},
		{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromString("http")}}},
	}
	for i, pr := range named {
		if got, err := modalProbe(pr); err != nil || got != nil {
			t.Fatalf("named-port case %d: modalProbe = (%v, %v), want (nil, nil)", i, got, err)
		}
	}
}
