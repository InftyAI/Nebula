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
	terminated []string
}

func (f *fakeClient) CreateSandbox(_ context.Context, spec SandboxSpec) (string, error) {
	f.createCnt++
	f.lastSpec = spec
	if f.createErr != nil {
		return "", f.createErr
	}
	id := f.createID
	if id == "" {
		id = "sb-new"
	}
	f.sandboxes = append(f.sandboxes, Sandbox{ID: id, Tags: spec.Tags, Status: "pending"})
	return id, nil
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

	id, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 2), provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if id != "sb-1" {
		t.Fatalf("id = %q, want sb-1", id)
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
	id, err := p.Provision(context.Background(), gpuPod("claim-a", "H100", 1), req)
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if id != "sb-existing" {
		t.Fatalf("id = %q, want sb-existing (idempotent reuse)", id)
	}
	if f.createCnt != 0 {
		t.Fatalf("CreateSandbox called %d times, want 0 (idempotent)", f.createCnt)
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
		{"quota sentinel", provider.ErrQuota, denyAll},
		{"capacity sentinel", provider.ErrNoCapacity, onDemand},
		{"wrapped capacity", fmt.Errorf("provision: %w", provider.ErrNoCapacity), onDemand},
		{"string unauthorized", fmt.Errorf("401 unauthorized"), denyAll},
		{"string no capacity", fmt.Errorf("no capacity available in region"), onDemand},
		{"unknown", fmt.Errorf("weird transient blip"), denyAll},
		{"nil", nil, provider.BlockScope{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.ClassifyProvisionError(tt.err)
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
	// The status→state mapping is load-bearing. observe emits only "running"/
	// "ready" (live), "terminated" (exited), or "" (Poll errored); everything else,
	// including the empty string, must map to Pending so the poll loop keeps
	// watching rather than declaring a premature terminal state.
	cases := map[string]provider.InstanceState{
		"running":    provider.InstanceRunning,
		"ready":      provider.InstanceRunning,
		"pending":    provider.InstancePending,
		"":           provider.InstancePending, // Poll errored, status left unset
		"terminated": provider.InstanceTerminated,
		"weird-new":  provider.InstancePending, // unknown => keep watching, not terminal
	}
	for in, want := range cases {
		if got := toState(in); got != want {
			t.Fatalf("toState(%q) = %q, want %q", in, got, want)
		}
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
