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

package runpod

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// fakeClient is an in-memory Client. It records the last CreatePod spec — the thing the
// adapter actually decides — and lets a test seed existing Pods or inject an error.
type fakeClient struct {
	pods      []Pod
	lastSpec  PodSpec
	createCnt int
	createErr error
	createID  string

	terminated []string

	// authID is what EnsureRegistryAuth resolves to, authFor the credential it was handed,
	// and authCnt how many times it was called — Provision must not pay for it when the
	// spec was going to be refused anyway.
	authID  string
	authErr error
	authFor *provider.RegistryAuth
	authCnt int
}

func (f *fakeClient) CreatePod(_ context.Context, spec PodSpec) (string, error) {
	f.createCnt++
	f.lastSpec = spec
	if f.createErr != nil {
		return "", f.createErr
	}
	id := f.createID
	if id == "" {
		id = "pod-new"
	}
	f.pods = append(f.pods, Pod{ID: id, Name: spec.Name, DesiredStatus: statusRunning})
	return id, nil
}

func (f *fakeClient) TerminatePod(_ context.Context, id string) error {
	f.terminated = append(f.terminated, id)
	return nil
}

func (f *fakeClient) GetPod(_ context.Context, id string) (*Pod, error) {
	for i := range f.pods {
		if f.pods[i].ID == id {
			pd := f.pods[i]
			return &pd, nil
		}
	}
	return nil, nil
}

func (f *fakeClient) ListPods(_ context.Context) ([]Pod, error) { return f.pods, nil }

func (f *fakeClient) EnsureRegistryAuth(_ context.Context, a *provider.RegistryAuth) (string, error) {
	f.authCnt++
	f.authFor = a
	if f.authErr != nil {
		return "", f.authErr
	}
	return f.authID, nil
}

// fakeCatalog is a trivial catalog.Lookup.
type fakeCatalog struct{ rows []provider.Offering }

func (c fakeCatalog) Offerings(_ string) []provider.Offering { return c.rows }

// newTestProvider builds a Provider over a fake client and a catalog shaped like
// runpod.csv: H100 carries THREE interchangeable RunPod ids (so MapAccelerator's
// primary-then-alternates order is observable), A100-80GB one, and L4 a Spot row.
func newTestProvider(f *fakeClient) *Provider {
	od, spot := nebulav1alpha1.CapacityOnDemand, nebulav1alpha1.CapacitySpot
	row := func(typ, id string, tier nebulav1alpha1.CapacityType, price float64) provider.Offering {
		return provider.Offering{
			AcceleratorType: typ, AcceleratorID: id, CapacityType: tier,
			PricePerHour: price, Available: true,
		}
	}
	return New(f, fakeCatalog{rows: []provider.Offering{
		row("H100", "NVIDIA H100 80GB HBM3", od, 2.99),
		row("H100", "NVIDIA H100 NVL", od, 2.79),
		row("H100", "NVIDIA H100 PCIe", od, 2.39),
		row("A100-80GB", "NVIDIA A100-SXM4-80GB", od, 1.74),
		row("L4", "NVIDIA L4", spot, 0.22),
	}})
}

// gpuPod builds a Pod whose accelerator type rides on the label and whose count rides on
// the container's nvidia.com/gpu limit. count<=0 means CPU-only (neither is set). accel is
// passed through verbatim so a test can also exercise non-canonical casing.
func gpuPod(accel string, count int64) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:    "main",
			Image:   "myimg:latest",
			Command: []string{"/entry.sh"},
			Args:    []string{"--flag", "v"},
		}}},
	}
	if accel != "" && count > 0 {
		pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: accel}
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
			util.NvidiaGPUResource: *resource.NewQuantity(count, resource.DecimalSI),
		}
	}
	return pod
}

// requests sets the container's resource requests from a k8s-notation map.
func requests(pod *corev1.Pod, m map[corev1.ResourceName]string) *corev1.Pod {
	c := &pod.Spec.Containers[0]
	if c.Resources.Requests == nil {
		c.Resources.Requests = corev1.ResourceList{}
	}
	for k, v := range m {
		c.Resources.Requests[k] = resource.MustParse(v)
	}
	return pod
}

// compile-time check that the fake really satisfies the seam the adapter is written to.
var _ Client = (*fakeClient)(nil)

func TestProvision_GPUPod(t *testing.T) {
	f := &fakeClient{createID: "pod-1"}
	p := newTestProvider(f)

	pod := requests(gpuPod("H100", 2), map[corev1.ResourceName]string{
		corev1.ResourceCPU:              "9",
		corev1.ResourceMemory:           "100Gi",
		corev1.ResourceEphemeralStorage: "80Gi",
	})
	pod.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8000}, {ContainerPort: 9090}}

	res, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
		Region:       "US-KS-2",
		Env:          map[string]string{"HF_TOKEN": "hf_secret"},
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	// A RunPod create allocates a machine before it answers — a shortage comes back as an
	// error, not a queued Pod — so an id here means real capacity was reserved. This is the
	// one place RunPod differs from Modal, and getting it wrong would report capacity that
	// was never granted.
	if !res.Reserved {
		t.Error("Reserved = false; a successful RunPod create means a host was allocated")
	}
	if res.InstanceID != "pod-1" {
		t.Errorf("InstanceID = %q, want pod-1", res.InstanceID)
	}
	// Derived from the id and the FIRST declared port, so it needs no read-back.
	if want := "https://pod-1-8000.proxy.runpod.net"; res.ConnectURL != want {
		t.Errorf("ConnectURL = %q, want %q", res.ConnectURL, want)
	}
	// RunPod's HTTP proxy is unauthenticated: there is no credential to hand back, and a
	// placeholder would look like one the Pod could authenticate with.
	if res.ConnectToken != "" {
		t.Errorf("ConnectToken = %q, want empty (the proxy is unauthenticated)", res.ConnectToken)
	}

	s := f.lastSpec
	if s.Name != "nebula-claim-a" {
		t.Errorf("Name = %q, want nebula-claim-a", s.Name)
	}
	if s.Image != "myimg:latest" {
		t.Errorf("Image = %q", s.Image)
	}
	// command → ENTRYPOINT and args → CMD stay SEPARATE, unlike Modal where both
	// concatenate into one command.
	if strings.Join(s.Entrypoint, " ") != "/entry.sh" || strings.Join(s.StartCmd, " ") != "--flag v" {
		t.Errorf("Entrypoint = %v, StartCmd = %v", s.Entrypoint, s.StartCmd)
	}
	if s.Env["HF_TOKEN"] != "hf_secret" {
		t.Errorf("Env = %v; the caller's resolved env must go out whole", provider.RedactedEnv(s.Env))
	}
	// All three H100 ids ride one create, primary first: RunPod picks from the array by
	// availability, so alternates broaden a SINGLE launch.
	if len(s.GPUTypeIDs) != 3 || s.GPUTypeIDs[0] != "NVIDIA H100 80GB HBM3" {
		t.Errorf("GPUTypeIDs = %v, want all three H100 ids with the primary first", s.GPUTypeIDs)
	}
	if s.GPUCount != 2 {
		t.Errorf("GPUCount = %d, want 2", s.GPUCount)
	}
	// RunPod sizes cpu/memory PER GPU, so the Pod's totals divide by 2 and round UP:
	// 9 vCPU → 5, 100 GiB → 50. Rounding down would under-provision the request.
	if s.VCPUPerGPU != 5 || s.RAMPerGPUGiB != 50 {
		t.Errorf("VCPUPerGPU = %d, RAMPerGPUGiB = %d, want 5/50 (per-GPU, rounded up)",
			s.VCPUPerGPU, s.RAMPerGPUGiB)
	}
	if s.VCPUCount != 0 {
		t.Errorf("VCPUCount = %d, want 0; it is only read for a CPU-only Pod", s.VCPUCount)
	}
	if s.ContainerDiskGiB != 80 {
		t.Errorf("ContainerDiskGiB = %d, want 80", s.ContainerDiskGiB)
	}
	if strings.Join(s.Ports, ",") != "8000/http,9090/http" {
		t.Errorf("Ports = %v, want both as /http", s.Ports)
	}
	// A compound token is a data center; the country-code field stays empty.
	if len(s.DataCenterIDs) != 1 || s.DataCenterIDs[0] != "US-KS-2" || len(s.CountryCodes) != 0 {
		t.Errorf("DataCenterIDs = %v, CountryCodes = %v", s.DataCenterIDs, s.CountryCodes)
	}
	if s.Interruptible {
		t.Error("Interruptible = true on the OnDemand tier")
	}
}

func TestProvision_SpotAndCountryRegion(t *testing.T) {
	f := &fakeClient{createID: "pod-spot"}
	p := newTestProvider(f)

	if _, err := p.Provision(context.Background(), gpuPod("l4", 1), provider.ProvisionRequest{
		ClaimName:    "claim-s",
		CapacityType: nebulav1alpha1.CapacitySpot,
		Region:       "us",
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	s := f.lastSpec
	if !s.Interruptible {
		t.Error("Interruptible = false on the Spot tier; RunPod's spot tier is this one boolean")
	}
	// A bare two-letter token is an ISO country code RunPod takes natively, upper-cased —
	// so nothing has to expand it and one declared token stays one blocklistable candidate.
	if len(s.CountryCodes) != 1 || s.CountryCodes[0] != "US" || len(s.DataCenterIDs) != 0 {
		t.Errorf("CountryCodes = %v, DataCenterIDs = %v, want [US]/[]", s.CountryCodes, s.DataCenterIDs)
	}
	// The label's casing is the user's; the catalog id that goes out is not.
	if len(s.GPUTypeIDs) != 1 || s.GPUTypeIDs[0] != "NVIDIA L4" {
		t.Errorf("GPUTypeIDs = %v, want [NVIDIA L4] from a lowercase label", s.GPUTypeIDs)
	}
}

func TestProvision_CPUOnlyPod(t *testing.T) {
	f := &fakeClient{createID: "pod-cpu"}
	p := newTestProvider(f)

	pod := requests(gpuPod("", 0), map[corev1.ResourceName]string{corev1.ResourceCPU: "2500m"})
	if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{
		ClaimName:    "claim-c",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	s := f.lastSpec
	// No accelerator: RunPod sizes the Pod with an ABSOLUTE vCPU count (rounded up from
	// 2500m), not the per-GPU pair, and no GPU fields are set at all.
	if s.VCPUCount != 3 || s.VCPUPerGPU != 0 || s.GPUCount != 0 || len(s.GPUTypeIDs) != 0 {
		t.Errorf("VCPUCount = %d, VCPUPerGPU = %d, GPUCount = %d, GPUTypeIDs = %v",
			s.VCPUCount, s.VCPUPerGPU, s.GPUCount, s.GPUTypeIDs)
	}
	// No region declared leaves both placement fields empty — the widest capacity pool.
	if len(s.DataCenterIDs) != 0 || len(s.CountryCodes) != 0 {
		t.Errorf("region fields = %v/%v, want both empty when unconstrained",
			s.DataCenterIDs, s.CountryCodes)
	}
}

func TestProvision_Idempotent(t *testing.T) {
	// A Pod already carrying this claim's name is the ONLY record of ownership RunPod
	// offers, so a repeat after a partial create must find it rather than pay twice.
	f := &fakeClient{pods: []Pod{{
		ID: "pod-existing", Name: "nebula-claim-a", DesiredStatus: statusRunning,
		LastStartedAt: "2026-08-29T00:00:00Z",
	}}}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("H100", 1), provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if res.InstanceID != "pod-existing" {
		t.Errorf("InstanceID = %q, want the existing pod-existing", res.InstanceID)
	}
	if !res.Reserved {
		t.Error("Reserved = false; the Pod exists, so a machine was allocated")
	}
	if f.createCnt != 0 {
		t.Errorf("CreatePod called %d times; a second Pod would be billed twice", f.createCnt)
	}
	// The interface forbids re-minting a credential on a repeat, and there is nothing to
	// mint here anyway — the proxy URL is derivable from the id.
	if res.ConnectToken != "" {
		t.Errorf("ConnectToken = %q, want empty on a re-Provision", res.ConnectToken)
	}
}

func TestProvision_RefusesOverlongClaimName(t *testing.T) {
	// RunPod caps a pod name at 191 chars and the name is Nebula's ONLY carrier of
	// identity, so a name that does not fit is refused rather than truncated: two
	// truncated claims would collide onto one Pod, which bills one twice and lets the
	// other's teardown reap the survivor.
	f := &fakeClient{}
	p := newTestProvider(f)

	long := strings.Repeat("a", maxNameLen-len(namePrefix)+1)
	_, err := p.Provision(context.Background(), gpuPod("H100", 1), provider.ProvisionRequest{
		ClaimName:    long,
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if err == nil {
		t.Fatal("Provision succeeded with an over-long claim name; the name would have collided")
	}
	if f.createCnt != 0 {
		t.Errorf("CreatePod called %d times despite the refusal", f.createCnt)
	}

	// One char shorter fits exactly, so the boundary is not off by one.
	if _, err := podName(strings.Repeat("a", maxNameLen-len(namePrefix))); err != nil {
		t.Errorf("podName rejected a name that fits exactly: %v", err)
	}
}

func TestProvision_RefusesRestrictedEgress(t *testing.T) {
	// RunPod has no outbound-allowlist knob at all. Placement should never route such a
	// pool here, but a request can be built by anyone, and provisioning it anyway would
	// put the workload on the open internet under a policy that says otherwise.
	f := &fakeClient{}
	p := newTestProvider(f)

	_, err := p.Provision(context.Background(), gpuPod("H100", 1), provider.ProvisionRequest{
		ClaimName:    "claim-e",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
		Egress:       &nebulav1alpha1.EgressPolicy{Mode: nebulav1alpha1.EgressBlocked},
	})
	if err == nil {
		t.Fatal("Provision accepted a restricted egress policy RunPod cannot enforce")
	}
	if f.createCnt != 0 {
		t.Errorf("CreatePod called %d times despite the refusal", f.createCnt)
	}
}

func TestProvision_RegistryAuth(t *testing.T) {
	basic := &provider.RegistryAuth{
		Registry: "ghcr.io",
		Basic:    &provider.BasicAuth{Username: "u", Password: "p4ssw0rd"},
	}

	t.Run("basic resolves to an auth id", func(t *testing.T) {
		f := &fakeClient{createID: "pod-auth", authID: "cra-123"}
		p := newTestProvider(f)

		if _, err := p.Provision(context.Background(), gpuPod("H100", 1), provider.ProvisionRequest{
			ClaimName:    "claim-r",
			CapacityType: nebulav1alpha1.CapacityOnDemand,
			RegistryAuth: basic,
		}); err != nil {
			t.Fatalf("Provision: %v", err)
		}
		// RunPod's create takes an OBJECT ID, never an inline username/password, so the
		// indirection has to happen before the create.
		if f.lastSpec.RegistryAuthID != "cra-123" {
			t.Errorf("RegistryAuthID = %q, want cra-123", f.lastSpec.RegistryAuthID)
		}
		if f.authCnt != 1 {
			t.Errorf("EnsureRegistryAuth called %d times, want 1", f.authCnt)
		}
	})

	t.Run("aws role is refused without an API call", func(t *testing.T) {
		// An ECR "password" is a 12-hour token, so there is no honest way to flatten a role
		// into RunPod's static credential object: a Pod pulling from it would stop being able
		// to pull halfway through the day. Refuse, and never fall back to an anonymous pull.
		f := &fakeClient{}
		p := newTestProvider(f)

		_, err := p.Provision(context.Background(), gpuPod("H100", 1), provider.ProvisionRequest{
			ClaimName:    "claim-ecr",
			CapacityType: nebulav1alpha1.CapacityOnDemand,
			RegistryAuth: &provider.RegistryAuth{
				Registry: "1234.dkr.ecr.us-east-1.amazonaws.com",
				AWSRole:  &provider.AWSRoleAuth{RoleARN: "arn:aws:iam::1234:role/pull", Region: "us-east-1"},
			},
		})
		if err == nil {
			t.Fatal("Provision accepted an AWSRole credential RunPod has no equivalent for")
		}
		// A rejection of the REQUEST, not of the candidate: the Pod fails with the reason
		// instead of retrying, and nothing gets blocklisted.
		if !errors.Is(err, provider.ErrImagePull) {
			t.Errorf("error = %v, want it to wrap ErrImagePull", err)
		}
		if f.authCnt != 0 || f.createCnt != 0 {
			t.Errorf("authCnt = %d, createCnt = %d; a refused credential must cost no API calls",
				f.authCnt, f.createCnt)
		}
	})
}

func TestProvision_UnsupportedAccelerator(t *testing.T) {
	// A type the catalog has no RunPod id for cannot be launched, and the sentinel is what
	// turns this into a capacity-class block rather than nebula_provision_failures_total
	// {reason="other"}.
	f := &fakeClient{}
	p := newTestProvider(f)

	_, err := p.Provision(context.Background(), gpuPod("TPUv5", 1), provider.ProvisionRequest{
		ClaimName:    "claim-x",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
	})
	if !errors.Is(err, provider.ErrUnsupportedAccelerator) {
		t.Fatalf("error = %v, want it to wrap ErrUnsupportedAccelerator", err)
	}
	if f.createCnt != 0 {
		t.Errorf("CreatePod called %d times for an accelerator with no RunPod id", f.createCnt)
	}
}

func TestPodSpecString_Redacts(t *testing.T) {
	// A spec reaches logs and error strings, and Env holds everything envFrom/valueFrom
	// resolved — Secret values included. Key NAMES are already in the Pod spec, so they
	// may print; values never may.
	s := PodSpec{
		Name:  "nebula-claim-a",
		Image: "myimg:latest",
		Env:   map[string]string{"HF_TOKEN": "hf_supersecret", "PLAIN": "visible"},
	}
	for _, form := range []string{s.String(), fmt.Sprintf("%v", s), fmt.Sprintf("%#v", s)} {
		if strings.Contains(form, "hf_supersecret") || strings.Contains(form, "visible") {
			t.Errorf("rendered spec leaks an env VALUE: %s", form)
		}
		if !strings.Contains(form, "HF_TOKEN") {
			t.Errorf("rendered spec dropped the env key names, which are safe: %s", form)
		}
	}
}

func TestList_FiltersToNebulaPods(t *testing.T) {
	// The name prefix stands in for the tags RunPod does not have. A Pod without it belongs
	// to someone else in the same account: reporting it would have the poll loop adopt it
	// and the NodeClaim controller eventually TERMINATE it.
	f := &fakeClient{pods: []Pod{
		{ID: "pod-1", Name: "nebula-claim-a", DesiredStatus: statusRunning, LastStartedAt: "t"},
		{ID: "pod-2", Name: "my-own-dev-box", DesiredStatus: statusRunning, LastStartedAt: "t"},
		{ID: "pod-3", Name: "nebula-claim-b", DesiredStatus: statusExited},
	}}
	p := newTestProvider(f)

	got, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("List returned %d instances, want the 2 Nebula-owned ones: %+v", len(got), got)
	}
	// ClaimName is recovered by stripping the prefix — the tag read the naming scheme
	// stands in for.
	if got[0].ClaimName != "claim-a" || got[1].ClaimName != "claim-b" {
		t.Errorf("claim names = %q/%q, want claim-a/claim-b", got[0].ClaimName, got[1].ClaimName)
	}
	if got[0].State != provider.InstanceRunning || got[1].State != provider.InstanceTerminated {
		t.Errorf("states = %v/%v", got[0].State, got[1].State)
	}
}

func TestGetAndTerminate(t *testing.T) {
	f := &fakeClient{pods: []Pod{{
		ID: "pod-1", Name: "nebula-claim-a", DesiredStatus: statusRunning,
		LastStartedAt: "t", Interruptible: true, DataCenterID: "EU-RO-1",
		Ports: []string{"8000/http"},
	}}}
	p := newTestProvider(f)

	inst, err := p.Get(context.Background(), "pod-1", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if inst == nil {
		t.Fatal("Get returned nil for a live Pod")
	}
	// The observed instance reports the tier it actually GOT and where RunPod actually put
	// it, both of which can differ from what was asked for.
	if inst.CapacityType != nebulav1alpha1.CapacitySpot || inst.Region != "EU-RO-1" {
		t.Errorf("CapacityType = %q, Region = %q", inst.CapacityType, inst.Region)
	}
	// An /http-only Pod has no public IP or port mapping ever, so the derived proxy URL is
	// its only address.
	if want := "https://pod-1-8000.proxy.runpod.net"; inst.Endpoint != want {
		t.Errorf("Endpoint = %q, want %q", inst.Endpoint, want)
	}

	// A Pod that is gone reports (nil, nil) — absent means terminated, per the interface.
	gone, err := p.Get(context.Background(), "pod-missing", "")
	if err != nil || gone != nil {
		t.Errorf("Get(missing) = %v, %v; want nil, nil", gone, err)
	}

	if err := p.Terminate(context.Background(), "pod-1", "EU-RO-1"); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if len(f.terminated) != 1 || f.terminated[0] != "pod-1" {
		t.Errorf("terminated = %v, want [pod-1]", f.terminated)
	}
	// Nothing was ever provisioned: there is no id to delete and no call to make.
	if err := p.Terminate(context.Background(), "", ""); err != nil {
		t.Errorf("Terminate(\"\") = %v, want nil", err)
	}
	if len(f.terminated) != 1 {
		t.Errorf("Terminate(\"\") called the API: %v", f.terminated)
	}
}

func TestToState(t *testing.T) {
	cases := []struct {
		name string
		pod  Pod
		want provider.InstanceState
	}{{
		// The subtlety of the whole adapter: desiredStatus is DESIRED. RunPod says RUNNING
		// from the moment it accepts the Pod, while the image may still be pulling —
		// reporting Running then would mark a Deployment's replica ready before anything is
		// listening. LastStartedAt is the one field that only appears once the container has
		// actually started.
		name: "RUNNING without LastStartedAt is still Pending",
		pod:  Pod{DesiredStatus: "RUNNING"},
		want: provider.InstancePending,
	}, {
		name: "RUNNING with LastStartedAt is Running",
		pod:  Pod{DesiredStatus: "RUNNING", LastStartedAt: "2026-08-29T00:00:00Z"},
		want: provider.InstanceRunning,
	}, {
		name: "EXITED is Terminated",
		pod:  Pod{DesiredStatus: "EXITED", LastStartedAt: "t"},
		want: provider.InstanceTerminated,
	}, {
		// Our own Terminate, or a spot reclaim — indistinguishable here, and both mean gone.
		name: "TERMINATED is Terminated",
		pod:  Pod{DesiredStatus: "TERMINATED"},
		want: provider.InstanceTerminated,
	}, {
		name: "casing is not load-bearing",
		pod:  Pod{DesiredStatus: "running", LastStartedAt: "t"},
		want: provider.InstanceRunning,
	}, {
		// A status this adapter has never seen keeps the poll loop WATCHING rather than
		// going terminal on a live, billing Pod.
		name: "an unknown status is Pending, not Terminated",
		pod:  Pod{DesiredStatus: "SOMETHING_NEW"},
		want: provider.InstancePending,
	}, {
		name: "an empty status is Pending",
		pod:  Pod{},
		want: provider.InstancePending,
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := toState(tc.pod); got != tc.want {
				t.Errorf("toState = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestEndpointOf_PrefersDirectAddress(t *testing.T) {
	// A public IP with an assigned port reaches the Pod directly, so it wins over the
	// proxy. The mapping is walked in sorted key order because Go randomizes map iteration
	// and this value is written to the Pod: an unsorted pick would rewrite the endpoint on
	// alternating poll ticks, which reads as flapping.
	pd := Pod{
		ID:           "pod-1",
		Ports:        []string{"8000/http"},
		PublicIP:     "1.2.3.4",
		PortMappings: map[string]int{"22": 40022, "8000": 41234},
	}
	for range 8 {
		if got := endpointOf(pd); got != "1.2.3.4:40022" {
			t.Fatalf("endpointOf = %q, want the lowest-keyed mapping 1.2.3.4:40022", got)
		}
	}
	// No port at all: no address to publish, and a guess would advertise something that
	// answers nothing.
	if got := endpointOf(Pod{ID: "pod-2"}); got != "" {
		t.Errorf("endpointOf(no ports) = %q, want empty", got)
	}
}

func TestClassifyProvisionError(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	const accel, region = "H100:8", "EU-RO-1"

	t.Run("nil error blocks nothing", func(t *testing.T) {
		// ClassifyError already returns the zero scope here, but the region decoration
		// below would repopulate it into a scope recordBlock installs.
		if got := p.ClassifyProvisionError(nil, accel, region); got != (provider.BlockScope{}) {
			t.Errorf("scope = %+v, want the zero scope", got)
		}
	})

	t.Run("capacity is scoped to this accelerator, tier and region", func(t *testing.T) {
		err := fmt.Errorf("no instances available: %w", provider.ErrNoCapacity)
		got := p.ClassifyProvisionError(err, accel, region)
		if got.DenyAll {
			t.Error("DenyAll = true for a capacity shortage; only this candidate ran out")
		}
		if got.Accelerator == nil || *got.Accelerator != accel {
			t.Errorf("Accelerator = %v, want %q", got.Accelerator, accel)
		}
		if got.CapacityType != nebulav1alpha1.CapacityOnDemand {
			t.Errorf("CapacityType = %q, want OnDemand", got.CapacityType)
		}
		if got.Region == nil || *got.Region != region {
			t.Errorf("Region = %v, want %q — a shortage in one DC must not disqualify another",
				got.Region, region)
		}
	})

	t.Run("a spot shortage does not block OnDemand", func(t *testing.T) {
		// The failing tier is not on the error's face, so the Client marks an interruptible
		// shortage with ErrSpotCapacity. Blocking it as OnDemand would disable capacity that
		// is still purchasable at the higher price.
		err := fmt.Errorf("spot gone: %w: %w", provider.ErrNoCapacity, ErrSpotCapacity)
		got := p.ClassifyProvisionError(err, accel, region)
		if got.CapacityType != nebulav1alpha1.CapacitySpot {
			t.Errorf("CapacityType = %q, want Spot", got.CapacityType)
		}
	})

	t.Run("auth denies the whole provider and is not narrowed", func(t *testing.T) {
		// A bad API key fails in every region, so narrowing DenyAll to one would contradict
		// the category and keep trying the other candidates against the same dead key.
		err := fmt.Errorf("401: %w", provider.ErrAuth)
		got := p.ClassifyProvisionError(err, accel, region)
		if !got.DenyAll {
			t.Fatal("DenyAll = false for an auth failure")
		}
		if got.Region != nil {
			t.Errorf("Region = %v on a DenyAll scope, want nil", got.Region)
		}
	})

	t.Run("a request rejection is not decorated into a block", func(t *testing.T) {
		// An unusable pull credential says nothing about the CANDIDATE. Stamping a region
		// onto the zero scope would make it non-empty, and recordBlock would then install a
		// region-wide block across every accelerator — excluding that DC for every other Pod
		// until the TTL lapsed.
		err := fmt.Errorf("bad credential: %w", provider.ErrImagePull)
		if got := p.ClassifyProvisionError(err, accel, region); got != (provider.BlockScope{}) {
			t.Errorf("scope = %+v, want the zero scope so nothing is blocklisted", got)
		}
	})

	t.Run("an empty region leaves Region nil", func(t *testing.T) {
		// nil matches only candidates that carry no region either — the unconstrained pool —
		// so the block never leaks onto region-pinned candidates.
		err := fmt.Errorf("no capacity: %w", provider.ErrNoCapacity)
		if got := p.ClassifyProvisionError(err, accel, ""); got.Region != nil {
			t.Errorf("Region = %v, want nil", got.Region)
		}
	})
}

func TestCapabilities(t *testing.T) {
	c := newTestProvider(&fakeClient{}).Capabilities()
	// Spot is real here (`interruptible`), which is what makes the catalog's Spot rows and
	// the ErrSpotCapacity marker meaningful — RunPod is the first adapter where both matter.
	if !c.SupportsSpot {
		t.Error("SupportsSpot = false")
	}
	// A stopped RunPod Pod still bills for its disk and releases its GPU, so it is neither
	// free nor resumable in the sense the capability promises.
	if c.SupportsStop {
		t.Error("SupportsStop = true; a stopped Pod still bills and has lost its GPU")
	}
	// No outbound-allowlist knob exists in the API, so placement must skip RunPod for an
	// egress-restricted pool rather than have Provision refuse it after the fact.
	if c.SupportsEgressPolicy {
		t.Error("SupportsEgressPolicy = true; RunPod exposes no outbound policy")
	}
	// No tags: identity rides the Pod name, which is what List filters on.
	if c.NativeTags {
		t.Error("NativeTags = true; RunPod Pods have no tags")
	}
	// Reclaims arrive with no notice pushed to us, so polling is the only detector — hence
	// a zero notice window and a faster-than-default cadence.
	if c.PreemptionNotice != 0 {
		t.Errorf("PreemptionNotice = %v, want 0 (abrupt)", c.PreemptionNotice)
	}
	if c.PollInterval != spotPollInterval {
		t.Errorf("PollInterval = %v, want %v", c.PollInterval, spotPollInterval)
	}
}

func TestSplitRegion(t *testing.T) {
	// The split is by SHAPE, not by a lookup table: RunPod's data-center ids are compound
	// ("EU-RO-1") while a bare two-letter token is an ISO country code its countryCodes
	// field takes. That is what keeps a static data-center list — which rots silently — out
	// of this package, and why ExpandRegions stays catalog.Base's pass-through.
	cases := []struct {
		region    string
		wantDCs   []string
		wantCodes []string
	}{
		{region: "", wantDCs: nil, wantCodes: nil},
		{region: "  ", wantDCs: nil, wantCodes: nil},
		{region: "us", wantDCs: nil, wantCodes: []string{"US"}},
		{region: "SE", wantDCs: nil, wantCodes: []string{"SE"}},
		{region: "US-KS-2", wantDCs: []string{"US-KS-2"}, wantCodes: nil},
		{region: "EU-RO-1", wantDCs: []string{"EU-RO-1"}, wantCodes: nil},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("%q", tc.region), func(t *testing.T) {
			dcs, codes := splitRegion(tc.region)
			if strings.Join(dcs, ",") != strings.Join(tc.wantDCs, ",") ||
				strings.Join(codes, ",") != strings.Join(tc.wantCodes, ",") {
				t.Errorf("splitRegion(%q) = %v, %v; want %v, %v",
					tc.region, dcs, codes, tc.wantDCs, tc.wantCodes)
			}
		})
	}
}
