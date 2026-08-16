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
	"io"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/provider/catalog"
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

	// logs is the stream SandboxLogs hands back, and logsFor records the id it was
	// asked for — the one thing the adapter decides on that path.
	logs    string
	logsErr error
	logsFor string

	// The exec path, recorded the same way: what the adapter passed down.
	execErr  error
	execFor  string
	execCmd  []string
	execOpts provider.ExecOptions
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

func (f *fakeClient) SandboxLogs(_ context.Context, id string) (io.ReadCloser, error) {
	f.logsFor = id
	if f.logsErr != nil {
		return nil, f.logsErr
	}
	return io.NopCloser(strings.NewReader(f.logs)), nil
}

func (f *fakeClient) SandboxExec(
	_ context.Context, id string, cmd []string, opts provider.ExecOptions,
) (provider.Process, error) {
	f.execFor, f.execCmd, f.execOpts = id, cmd, opts
	if f.execErr != nil {
		return nil, f.execErr
	}
	return fakeProcess{}, nil
}

// fakeProcess is a command that produced nothing and exited 0.
type fakeProcess struct{}

func (fakeProcess) Stdin() io.WriteCloser               { return nil }
func (fakeProcess) Stdout() io.Reader                   { return strings.NewReader("") }
func (fakeProcess) Stderr() io.Reader                   { return nil }
func (fakeProcess) Wait(_ context.Context) (int, error) { return 0, nil }
func (fakeProcess) Close() error                        { return nil }

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

// TestPlanProbe: the Pod-probe -> Modal-probe mapping, which is where the two
// vocabularies disagree. Modal offers exec and TCP only, so the interesting cases
// are the ones Kubernetes can express and Modal cannot:
//
//   - httpGet DEGRADES to a TCP connect on the same port. The path is never
//     fetched, so the bar weakens from "answers 2xx" to "is listening" — accepted
//     because the alternative (dropping the probe) reports a sandbox ready the
//     moment it is created, which is far more wrong.
//   - a NAMED port is dropped entirely rather than degraded. Resolving it needs the
//     container spec, which the SDK probe has no access to, and IntValue() yields 0
//     for a name — emitting port 0 would create a probe that can never pass.
//
// ok=false is load-bearing beyond the create call: it also gates ProbeTagKey, so a
// dropped probe must not be tagged (see TestProvision_ProbeTagStampedOnlyWithProbe).
func TestPlanProbe(t *testing.T) {
	for _, tc := range []struct {
		name     string
		probe    *corev1.Probe
		wantOK   bool
		wantExec []string
		wantPort int
		wantIvl  time.Duration
	}{{
		name: "httpGet numeric port degrades to a TCP probe on that port",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromInt(8080)},
			},
			PeriodSeconds: 5,
		},
		wantOK:   true,
		wantPort: 8080,
		wantIvl:  5 * time.Second,
	}, {
		// The path and scheme are simply lost: nothing in a Modal TCP probe can carry
		// them, so two httpGet probes differing only by path plan identically.
		name: "httpGet on an https path still degrades by port alone",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path:   "/ready",
					Port:   intstr.FromInt(8443),
					Scheme: corev1.URISchemeHTTPS,
				},
			},
		},
		wantOK:   true,
		wantPort: 8443,
	}, {
		name: "httpGet named port is dropped, not degraded to port 0",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{Path: "/healthz", Port: intstr.FromString("http")},
			},
			PeriodSeconds: 5,
		},
	}, {
		name: "tcpSocket numeric port maps straight through",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(8000)},
			},
			PeriodSeconds: 10,
		},
		wantOK:   true,
		wantPort: 8000,
		wantIvl:  10 * time.Second,
	}, {
		name: "tcpSocket named port is dropped",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromString("web")},
			},
		},
	}, {
		// exec wins when a Pod somehow declares both: it is the only handler Modal runs
		// faithfully, so degrading to the httpGet's port would weaken the bar for nothing.
		name: "exec takes precedence over httpGet",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				Exec:    &corev1.ExecAction{Command: []string{"nvidia-smi", "-L"}},
				HTTPGet: &corev1.HTTPGetAction{Port: intstr.FromInt(8080)},
			},
		},
		wantOK:   true,
		wantExec: []string{"nvidia-smi", "-L"},
	}, {
		// Zero interval is deliberate, not a default filled in here: the SDK
		// constructors reject a zero interval, so this leaves Modal's own default.
		name: "no periodSeconds leaves the interval zero",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				TCPSocket: &corev1.TCPSocketAction{Port: intstr.FromInt(80)},
			},
		},
		wantOK:   true,
		wantPort: 80,
	}, {
		name:  "nil probe",
		probe: nil,
	}, {
		name:  "probe with no handler",
		probe: &corev1.Probe{PeriodSeconds: 5},
	}, {
		name: "exec with an empty command is not a probe",
		probe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{}},
		},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := planProbe(tc.probe)
			if ok != tc.wantOK {
				t.Fatalf("planProbe ok = %v, want %v (plan=%+v)", ok, tc.wantOK, got)
			}
			if !ok {
				return
			}
			if !slices.Equal(got.exec, tc.wantExec) {
				t.Errorf("exec = %v, want %v", got.exec, tc.wantExec)
			}
			if got.port != tc.wantPort {
				t.Errorf("port = %d, want %d", got.port, tc.wantPort)
			}
			if got.interval != tc.wantIvl {
				t.Errorf("interval = %v, want %v", got.interval, tc.wantIvl)
			}
			// Exactly one of the two handlers is planned: modalProbe branches on
			// exec != nil, so a plan carrying both would silently drop the port.
			if (got.exec != nil) == (got.port > 0) {
				t.Errorf("plan must set exactly one of exec/port, got %+v", got)
			}
		})
	}
}

// The spec carries the container's declared ports verbatim: they are the set Modal is
// told to accept traffic on, and the connect URL routes to the first of them (the
// client derives it, so the routed port can never name one outside the set). No
// declared port is not "no endpoint" — every workload is credentialed — it means Modal
// picks, defaulting to 8080.
func TestProvision_CarriesRegion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		region string
		want   []string
	}{{
		// The unconstrained case, and the one that must NOT become []string{""}: an
		// empty region means "no placement constraint", which is Modal's widest pool
		// and its un-multiplied price. A one-element slice holding "" would instead ask
		// Modal to place in a region named "".
		name:   "no region leaves placement unconstrained",
		region: "",
		want:   nil,
	}, {
		name:   "broad region is forwarded",
		region: "us",
		want:   []string{"us"},
	}, {
		// Modal owns this vocabulary and gains regions faster than the adapter ships,
		// so a value it does not recognize is forwarded rather than rejected here.
		name:   "narrow region is forwarded verbatim",
		region: "us-east",
		want:   []string{"us-east"},
	}, {
		// The whole point of the join: a pool declaring several regions must hand Modal
		// ALL of them in the one create call, because that call cannot fail over — it
		// returns an accepted id with no capacity error, so no second region would ever
		// be attempted. Modal's scheduler picks among these itself.
		name:   "a multi-region candidate is split back into the full set",
		region: "us-east" + regionSeparator + "us-west" + regionSeparator + "eu-west",
		want:   []string{"us-east", "us-west", "eu-west"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeClient{createID: "sb-1"}
			p := newTestProvider(f)
			req := provider.ProvisionRequest{ClaimName: "claim-a", Region: tc.region}
			if _, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1), req); err != nil {
				t.Fatalf("Provision: %v", err)
			}
			if !slices.Equal(f.lastSpec.Regions, tc.want) {
				t.Fatalf("spec.Regions = %v, want %v", f.lastSpec.Regions, tc.want)
			}
		})
	}
}

// TestExpandRegions_CollapsesToOneCandidate pins the axis decision that matters most
// for Modal: a pool's whole region declaration becomes exactly ONE placement
// candidate. Modal's create accepts a sandbox and queues it without a capacity error,
// so nothing re-drives placement afterwards — one candidate per region would mean the
// first region walked is the only one ever tried, silently discarding the rest of the
// operator's declaration. Collapsing hands the full set to Modal's own scheduler.
func TestExpandRegions_CollapsesToOneCandidate(t *testing.T) {
	p := newTestProvider(&fakeClient{})

	for _, tc := range []struct {
		name     string
		declared []string
		want     []string
	}{{
		name:     "no declaration stays unconstrained",
		declared: nil,
		want:     nil,
	}, {
		// Not []string{""}: an empty candidate and no candidate must not be confused,
		// and regionsFor supplies the one candidate the walk needs.
		name:     "a declaration of only blanks is unconstrained, not an empty region",
		declared: []string{"", "  "},
		want:     nil,
	}, {
		name:     "a single region is one candidate holding it",
		declared: []string{"us"},
		want:     []string{"us"},
	}, {
		name:     "several regions are ONE candidate, not several",
		declared: []string{"us-east", "eu-west"},
		want:     []string{"us-east" + regionSeparator + "eu-west"},
	}, {
		name:     "duplicates collapse and order is the operator's",
		declared: []string{"eu-west", "us-east", "eu-west"},
		want:     []string{"eu-west" + regionSeparator + "us-east"},
	}} {
		t.Run(tc.name, func(t *testing.T) {
			got := p.ExpandRegions(tc.declared)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ExpandRegions(%v) = %v, want %v", tc.declared, got, tc.want)
			}
			if len(got) > 1 {
				t.Fatalf("ExpandRegions(%v) produced %d candidates; Modal cannot fail "+
					"over, so every extra candidate is a region silently never tried", tc.declared, len(got))
			}
		})
	}
}

// TestExpandRegions_RoundTripsThroughProvision is the invariant that makes the
// collapse safe: whatever ExpandRegions joins, regionsOf must split back to the exact
// declared set by the time it reaches Modal's API. The two are inverses, and this
// asserts it end to end through Provision rather than on the helpers alone — a
// mismatch here would send Modal a region name it has never heard of (the joined
// token), which is precisely the failure a unit test on either half would miss.
func TestExpandRegions_RoundTripsThroughProvision(t *testing.T) {
	for _, declared := range [][]string{
		nil,
		{"us"},
		{"us-east", "us-west"},
		{"us", "eu", "ap", "jp"},
	} {
		f := &fakeClient{createID: "sb-1"}
		p := newTestProvider(f)

		candidates := p.ExpandRegions(declared)
		// Placement's own fallback when expansion is empty: one unconstrained candidate.
		if len(candidates) == 0 {
			candidates = []string{""}
		}
		req := provider.ProvisionRequest{ClaimName: "claim-a", Region: candidates[0]}
		if _, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1), req); err != nil {
			t.Fatalf("Provision: %v", err)
		}

		// A nil declaration must stay nil all the way down, not become [""].
		var want []string
		if len(declared) > 0 {
			want = declared
		}
		if !slices.Equal(f.lastSpec.Regions, want) {
			t.Fatalf("declared %v reached Modal as %v, want %v", declared, f.lastSpec.Regions, want)
		}
	}
}

func TestClassifyProvisionError_ConfinesToFailingRegion(t *testing.T) {
	p := newTestProvider(&fakeClient{})

	// A capacity shortage in one region must not disqualify the others: Modal's
	// regions are independent pools, so without this the first regional failure would
	// block every region the pool lists.
	got := p.ClassifyProvisionError(provider.ErrNoCapacity, "H100:1", "us-east")
	if got.Region == nil || *got.Region != "us-east" {
		t.Fatalf("expected the block confined to us-east, got Region=%v", got.Region)
	}

	// Unconstrained: no region axis was used, so Region stays nil — which per
	// BlockScope's three-state rule matches only an empty-region candidate, and so
	// cannot leak onto region-pinned ones.
	if got := p.ClassifyProvisionError(provider.ErrNoCapacity, "H100:1", ""); got.Region != nil {
		t.Fatalf("expected nil Region for an unconstrained request, got %q", *got.Region)
	}

	// Auth fails in every region, so DenyAll must not be narrowed to one.
	if got := p.ClassifyProvisionError(provider.ErrAuth, "H100:1", "us-east"); got.Region != nil {
		t.Fatalf("DenyAll must not be confined to a region, got %q", *got.Region)
	}

	// No error, no block. recordBlock installs any non-empty scope it is handed, so
	// decorating the zero scope with a region would turn "nothing failed" into a live
	// block on that region.
	if got := p.ClassifyProvisionError(nil, "H100:1", "us-east"); got != (provider.BlockScope{}) {
		t.Fatalf("a nil error must classify to the zero scope, got %+v", got)
	}
}

func TestProvision_CarriesDeclaredPorts(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ports []corev1.ContainerPort
		want  []int
		// wantRouted is the port the connect URL ends up on, which the client derives
		// from want; 0 leaves it to Modal.
		wantRouted int
	}{
		{"no ports leaves the port to Modal", nil, nil, 0},
		{"single port", []corev1.ContainerPort{{ContainerPort: 8000}}, []int{8000}, 8000},
		// Modal routes one port per token, so the first declared port wins — but the
		// whole set is still exposed.
		{
			"all exposed, first routed",
			[]corev1.ContainerPort{{ContainerPort: 8000}, {ContainerPort: 9090}},
			[]int{8000, 9090},
			8000,
		},
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
			if !slices.Equal(f.lastSpec.Ports, tc.want) {
				t.Fatalf("Ports = %v, want %v", f.lastSpec.Ports, tc.want)
			}
			if got := firstPort(f.lastSpec.Ports); got != tc.wantRouted {
				t.Fatalf("routed port = %d, want %d", got, tc.wantRouted)
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

// Modal may silently place a bare gpu="H100" request on an H200, which would make
// Nebula's own bookkeeping lie: the optimizer picked the row on the H100 price, the
// blocklist keys failures on "H100:1", and the NodeClaim reports accelerator=H100 —
// while a workload benchmarking an H100 would measure a different card. modal.csv
// therefore pins the row's accelerator_id to Modal's documented opt-out, "H100!".
// This runs against the REAL embedded catalog, not a fake, because the pin IS the
// data — a fake would assert nothing about what ships.
func TestProvision_PinsH100AgainstAutoUpgrade(t *testing.T) {
	cat, err := catalog.Load()
	if err != nil {
		t.Fatalf("load embedded catalog: %v", err)
	}
	f := &fakeClient{createID: "sb-1"}
	p := New(f, cat)

	if _, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 2),
		provider.ProvisionRequest{ClaimName: "claim-a"}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if f.lastSpec.GPU != "H100!" {
		t.Fatalf("spec GPU = %q, want H100! — a bare H100 lets Modal substitute an H200", f.lastSpec.GPU)
	}
	// The suffix must survive the count suffix too: "H100!:2", not "H100:2!".
	if got := gpuReservation(f.lastSpec.GPU, f.lastSpec.GPUCount); got != "H100!:2" {
		t.Fatalf("gpuReservation = %q, want H100!:2", got)
	}
}

// The counterpart to the H100 pin: every other Modal row maps by identity. A100 is
// the one to watch — Modal upgrades a bare "A100" to the 80GB card, but the catalog
// never emits that token; it ships the already-exact A100-40GB/A100-80GB names.
func TestMapAccelerator_OtherRowsMapByIdentity(t *testing.T) {
	cat, err := catalog.Load()
	if err != nil {
		t.Fatalf("load embedded catalog: %v", err)
	}
	p := New(&fakeClient{}, cat)

	for _, canonical := range []string{"T4", "L4", "A10G", "L40S", "A100-40GB", "A100-80GB", "H200"} {
		ids, ok := p.MapAccelerator(canonical, 1)
		if !ok || len(ids) != 1 || ids[0] != canonical {
			t.Errorf("MapAccelerator(%q, 1) = (%v, %v), want ([%s], true)", canonical, ids, ok, canonical)
		}
	}
	// And bare A100 is not an offering at all, so it can never reach Modal.
	if ids, ok := p.MapAccelerator("A100", 1); ok {
		t.Errorf("MapAccelerator(A100, 1) = (%v, true), want not-offered: the bare token is the fuzzy one", ids)
	}
}

// Modal implements the optional provider.LogStreamer, which is what makes
// `kubectl logs` work. A thin pass-through: the id must reach the client unchanged,
// since it is the only thing selecting whose output comes back.
func TestLogs_StreamsSandboxOutput(t *testing.T) {
	f := &fakeClient{logs: "sandbox says hi\n"}
	p := newTestProvider(f)

	rc, err := p.Logs(context.Background(), "sb-1")
	if err != nil {
		t.Fatalf("Logs: %v", err)
	}
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "sandbox says hi\n" {
		t.Fatalf("logs = %q", got)
	}
	if f.logsFor != "sb-1" {
		t.Fatalf("client asked for sandbox %q, want sb-1", f.logsFor)
	}
}

// An empty id is a Pod still inside Provision. It is rejected here rather than handed
// on, and as an error — an empty stream would read as "printed nothing".
func TestLogs_NoSandboxIDErrors(t *testing.T) {
	f := &fakeClient{logs: "should not be read\n"}
	p := newTestProvider(f)

	if _, err := p.Logs(context.Background(), ""); err == nil {
		t.Fatal("Logs(\"\"): expected an error")
	}
	if f.logsFor != "" {
		t.Fatalf("client was called with %q; an empty id must not reach Modal", f.logsFor)
	}
}

// A sandbox past Modal's retention, or an expired token, must surface as an error.
func TestLogs_ClientErrorPropagates(t *testing.T) {
	f := &fakeClient{logsErr: fmt.Errorf("sandbox sb-gone not found")}
	p := newTestProvider(f)

	if _, err := p.Logs(context.Background(), "sb-gone"); err == nil {
		t.Fatal("expected the client error to propagate")
	}
}

// Exec is the same shape of pass-through: the command, the TTY request and the id must all
// reach the client unchanged, since the adapter decides nothing else here.
func TestExec_PassesCommandThrough(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)

	proc, err := p.Exec(context.Background(), "sb-1", []string{"bash", "-lc", "ls /"},
		provider.ExecOptions{TTY: true})
	if err != nil {
		t.Fatalf("Exec: %v", err)
	}
	defer func() { _ = proc.Close() }()

	if f.execFor != "sb-1" {
		t.Errorf("client asked for sandbox %q, want sb-1", f.execFor)
	}
	if !slices.Equal(f.execCmd, []string{"bash", "-lc", "ls /"}) {
		t.Errorf("command = %v, want it passed through verbatim", f.execCmd)
	}
	if !f.execOpts.TTY {
		t.Error("TTY was not passed through; an interactive shell needs it")
	}
}

// An empty id is a Pod still inside Provision: there is no sandbox to run in, so this
// fails here instead of reaching Modal.
func TestExec_NoSandboxIDErrors(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)

	if _, err := p.Exec(context.Background(), "", []string{"sh"}, provider.ExecOptions{}); err == nil {
		t.Fatal("Exec(\"\"): expected an error")
	}
	if f.execFor != "" {
		t.Fatalf("client was called with %q; an empty id must not reach Modal", f.execFor)
	}
}

// A sandbox that is gone, or still queued for a GPU (no task to exec into), must surface
// as an error rather than a silent no-op.
func TestExec_ClientErrorPropagates(t *testing.T) {
	f := &fakeClient{execErr: fmt.Errorf("timed out waiting for task ID")}
	p := newTestProvider(f)

	if _, err := p.Exec(context.Background(), "sb-queued", []string{"sh"}, provider.ExecOptions{}); err == nil {
		t.Fatal("expected the client error to propagate")
	}
}

// The handler resolves both optional halves by type assertion, so drift would compile fine
// and silently revert `kubectl logs`/`kubectl exec` to NotFound. Assert them here.
var (
	_ provider.LogStreamer = (*Provider)(nil)
	_ provider.Executor    = (*Provider)(nil)
)

// --- mergeStreams -----------------------------------------------------------

// errStream yields s, then fails. A Modal log stream that dies mid-poll looks like this.
type errStream struct {
	s   string
	err error
	n   int
}

func (e *errStream) Read(p []byte) (int, error) {
	if e.n < len(e.s) {
		n := copy(p, e.s[e.n:])
		e.n += n
		return n, nil
	}
	return 0, e.err
}

func (e *errStream) Close() error { return nil }

// Modal serves only recent output, so an empty log is ORDINARY. A broken stream must
// therefore say so: read as EOF, an outage is indistinguishable from a quiet workload.
func TestMergeStreams_StreamFailureSurfaces(t *testing.T) {
	boom := fmt.Errorf("error getting output stream: unavailable")
	rc := mergeStreams(context.Background(), &errStream{s: "some output\n", err: boom})
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if err == nil {
		t.Fatal("ReadAll: expected the stream failure to surface")
	}
	if !strings.Contains(err.Error(), "unavailable") {
		t.Fatalf("err = %v, want the provider's own message", err)
	}
	// Output already received is still delivered: the error explains the END of the log,
	// it does not discard it.
	if string(got) != "some output\n" {
		t.Fatalf("logs = %q, want what arrived before the failure", got)
	}
}

// One failed stream must not truncate its sibling — stdout is the interesting one, and a
// stderr that breaks would otherwise cut it short.
func TestMergeStreams_OneFailureKeepsTheOther(t *testing.T) {
	ok := io.NopCloser(strings.NewReader("stdout line\n"))
	bad := &errStream{err: fmt.Errorf("stderr gone")}

	rc := mergeStreams(context.Background(), ok, bad)
	defer func() { _ = rc.Close() }()

	got, err := io.ReadAll(rc)
	if string(got) != "stdout line\n" {
		t.Fatalf("logs = %q, want the healthy stream in full", got)
	}
	if err == nil {
		t.Fatal("expected the failure of the other stream to be reported")
	}
}

// A clean end stays clean: both streams EOF, so `kubectl logs` must not report an error
// for a workload that simply stopped logging.
func TestMergeStreams_CleanEndIsEOF(t *testing.T) {
	rc := mergeStreams(context.Background(),
		io.NopCloser(strings.NewReader("a\n")), io.NopCloser(strings.NewReader("")))
	defer func() { _ = rc.Close() }()

	if _, err := io.ReadAll(rc); err != nil {
		t.Fatalf("ReadAll: %v, want a clean EOF", err)
	}
}

// blockingStream is a following log stream: silent until closed, and then it reports a
// failure — which is what a source WE closed looks like from the copier.
type blockingStream struct {
	closed chan struct{}
	once   sync.Once
}

func (b *blockingStream) Read([]byte) (int, error) {
	<-b.closed
	return 0, fmt.Errorf("stream closed by teardown")
}

func (b *blockingStream) Close() error {
	b.once.Do(func() { close(b.closed) })
	return nil
}

// Our own teardown is not a provider failure: a client walking away from `kubectl logs -f`
// closes the sources, and the read errors that follow must not be reported as an outage.
func TestMergeStreams_TeardownIsNotAFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	rc := mergeStreams(ctx, &blockingStream{closed: make(chan struct{})})
	cancel()

	// The context error or a clean EOF, never the source's own failure.
	_, err := io.ReadAll(rc)
	if err != nil && strings.Contains(err.Error(), "stream closed by teardown") {
		t.Fatalf("err = %v, want teardown not to be reported as a provider failure", err)
	}
	_ = rc.Close()
}
