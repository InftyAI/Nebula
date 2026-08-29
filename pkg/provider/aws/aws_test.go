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

package aws

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	smithy "github.com/aws/smithy-go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// fakeClient is an in-memory Client for tests. It records the last RunInstance
// spec and lets tests seed existing instances and inject errors.
type fakeClient struct {
	instances  []EC2Instance
	lastSpec   InstanceSpec
	runCnt     int
	runErr     error
	runID      string
	terminated []string
	// available is the set the AvailableInstanceTypes probe returns; availErr
	// injects a probe failure. Both default to zero (empty set / no error), so an
	// Offerings call on a bare fake reports every row unavailable but still present.
	available map[string]bool
	availErr  error
}

func (f *fakeClient) RunInstance(_ context.Context, spec InstanceSpec) (string, error) {
	f.runCnt++
	f.lastSpec = spec
	if f.runErr != nil {
		return "", f.runErr
	}
	id := f.runID
	if id == "" {
		id = "i-new"
	}
	f.instances = append(f.instances, EC2Instance{
		ID:     id,
		Tags:   spec.Tags,
		State:  statePending,
		Region: spec.Region,
		Spot:   spec.Spot,
	})
	return id, nil
}

func (f *fakeClient) TerminateInstance(_ context.Context, id string) error {
	f.terminated = append(f.terminated, id)
	return nil
}

func (f *fakeClient) DescribeInstance(_ context.Context, id string) (*EC2Instance, error) {
	for i := range f.instances {
		if f.instances[i].ID == id {
			inst := f.instances[i]
			return &inst, nil
		}
	}
	return nil, nil
}

func (f *fakeClient) ListInstances(_ context.Context) ([]EC2Instance, error) {
	return f.instances, nil
}

func (f *fakeClient) AvailableInstanceTypes(_ context.Context) (map[string]bool, error) {
	if f.availErr != nil {
		return nil, f.availErr
	}
	return f.available, nil
}

// fakeCatalog is a trivial catalog.Lookup for tests. Its rows carry the
// AcceleratorID (EC2 instance type) so MapAccelerator resolves the way AWS
// requires — by instance type, not accelerator name.
type fakeCatalog struct{ rows []provider.Offering }

func (c fakeCatalog) Offerings(_ string) []provider.Offering { return c.rows }

const testRegion = "us-east-1"

// offering is a compact constructor for a region-aware test catalog row (the
// full struct literal is too wide to read inline). count is the gpu_count, the
// AWS lookup key that (with the accelerator type) selects the instance type.
func offering(accel, id string, count int32, tier nebulav1alpha1.CapacityType, price float64) provider.Offering {
	return provider.Offering{
		AcceleratorType: accel,
		AcceleratorID:   id,
		GPUCount:        count,
		CapacityType:    tier,
		PricePerHour:    price,
		Available:       true,
		Region:          testRegion,
	}
}

// newTestProvider builds a Provider with a fake client and a small region-aware
// catalog whose (accelerator_type, gpu_count) pairs map to EC2 instance types.
// T4 appears at two counts so the count-aware resolution is exercised.
func newTestProvider(f *fakeClient) *Provider {
	return newSingleRegion(f, fakeCatalog{rows: []provider.Offering{
		offering("H100", "p5.48xlarge", 8, nebulav1alpha1.CapacityOnDemand, 98.32),
		offering("H100", "p5.48xlarge", 8, nebulav1alpha1.CapacitySpot, 34.41),
		offering("A100-80GB", "p4de.24xlarge", 8, nebulav1alpha1.CapacityOnDemand, 40.96),
		offering("T4", "g4dn.xlarge", 1, nebulav1alpha1.CapacityOnDemand, 0.526),
		offering("T4", "g4dn.metal", 8, nebulav1alpha1.CapacityOnDemand, 7.824),
	}}, testRegion)
}

// gpuPod builds a Pod whose accelerator type rides on the accelerator-type label
// and whose count rides on the container's nvidia.com/gpu resource; count<=0
// means CPU-only (no label, no GPU resource).
func gpuPod(accel string, count int64) *corev1.Pod {
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

// TestProvision_UsesResolvedEnv pins the env contract: the request's resolved map is the whole
// environment, and the Pod's own env is not read — it holds references this adapter cannot
// follow, so a caller that does not resolve gets nothing rather than half a workload.
func TestProvision_UsesResolvedEnv(t *testing.T) {
	f := &fakeClient{runID: "i-env"}
	p := newTestProvider(f)
	pod := gpuPod("H100", 8) // carries FOO=bar literally

	if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{
		ClaimName: "claim-env",
		Region:    "us-west-2", // EC2 needs one; the test client is not configured with a default
		Env:       map[string]string{"FOO": "bar", "TOKEN": "t0ken"},
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if got := f.lastSpec.Env; got["FOO"] != "bar" || got["TOKEN"] != "t0ken" || len(got) != 2 {
		t.Fatalf("spec env = %v, want FOO=bar TOKEN=t0ken", got)
	}

	// No resolved map: nothing is set, even though the Pod carries FOO=bar literally.
	f = &fakeClient{runID: "i-env-2"}
	p = newTestProvider(f)
	if _, err := p.Provision(context.Background(), pod, provider.ProvisionRequest{
		ClaimName: "claim-env",
		Region:    "us-west-2",
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if got := f.lastSpec.Env; len(got) != 0 {
		t.Fatalf("spec env = %v, want empty: the Pod's env is not a source here", got)
	}
}

func TestProvision_MapsAcceleratorToInstanceType(t *testing.T) {
	f := &fakeClient{runID: "i-1"}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName:    "claim-a",
		CapacityType: nebulav1alpha1.CapacityOnDemand,
		Region:       "us-west-2",
	})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	id, reserved := res.InstanceID, res.Reserved
	// EC2 mints no bearer credential: an instance is reached at the public DNS name or
	// IP that only exists once it boots, so the address comes from the observed
	// endpoint, not from this call.
	if res.ConnectURL != "" || res.ConnectToken != "" {
		t.Fatalf("expected no credential from AWS, got url=%q token set=%t",
			res.ConnectURL, res.ConnectToken != "")
	}
	// The returned id is the raw EC2 id (no region prefix); Terminate/Get re-locate
	// it by sweeping regions.
	if id != "i-1" {
		t.Fatalf("id = %q, want i-1", id)
	}
	// An instant fleet is synchronous, so an id means EC2 already allocated capacity:
	// AWS never hands back an instance that is still queued.
	if !reserved {
		t.Fatal("reserved = false; an instant fleet only returns an id once capacity is allocated")
	}
	// AWS requests by instance type: H100 must resolve to its accelerator_id as the
	// primary (fleet) instance type.
	if got := primaryType(f.lastSpec); got != "p5.48xlarge" {
		t.Fatalf("primary InstanceType = %q, want p5.48xlarge", got)
	}
	if f.lastSpec.Image != "myimg:latest" {
		t.Fatalf("image = %q", f.lastSpec.Image)
	}
	if f.lastSpec.Spot {
		t.Fatalf("Spot = true, want false for an OnDemand request")
	}
	// The request pinned a region; it must ride through verbatim.
	if f.lastSpec.Region != "us-west-2" {
		t.Fatalf("Region = %q, want us-west-2 (from request)", f.lastSpec.Region)
	}
	if got := f.lastSpec.Tags[ClaimTagKey]; got != "claim-a" {
		t.Fatalf("claim tag = %q, want claim-a", got)
	}
	// Command and Args ride through SEPARATELY (no flattening), preserving the
	// Pod's Kubernetes container semantics for the user-data renderer.
	if len(f.lastSpec.Command) != 1 || f.lastSpec.Command[0] != "run" {
		t.Fatalf("command = %v, want [run]", f.lastSpec.Command)
	}
	if len(f.lastSpec.Args) != 1 || f.lastSpec.Args[0] != "--flag" {
		t.Fatalf("args = %v, want [--flag]", f.lastSpec.Args)
	}
}

func TestProvision_LowercaseAcceleratorLabel(t *testing.T) {
	f := &fakeClient{runID: "i-lc"}
	p := newTestProvider(f)

	// A user may write the accelerator-type label in any case; it must resolve to
	// the canonical catalog row (and thus the right instance type).
	if _, err := p.Provision(context.Background(), gpuPod("h100", 8), provider.ProvisionRequest{
		ClaimName: "claim-lc",
		Region:    testRegion,
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if got := primaryType(f.lastSpec); got != "p5.48xlarge" {
		t.Fatalf("primary InstanceType = %q, want p5.48xlarge from lowercase label", got)
	}
}

// primaryType returns the fleet's primary (blocklist-keyed) instance type, or ""
// when the spec carries none.
func primaryType(spec InstanceSpec) string {
	if len(spec.InstanceTypes) == 0 {
		return ""
	}
	return spec.InstanceTypes[0]
}

func TestProvision_CountSelectsInstanceType(t *testing.T) {
	// The GPU count is a lookup key on AWS: the same accelerator at different
	// counts must resolve to DIFFERENT instance types (T4x1 = g4dn.xlarge,
	// T4x8 = g4dn.metal), because the count is baked into the instance type.
	cases := []struct {
		count    int64
		wantType string
	}{
		{1, "g4dn.xlarge"},
		{8, "g4dn.metal"},
	}
	for _, tc := range cases {
		f := &fakeClient{runID: "i-t4"}
		p := newTestProvider(f)
		req := provider.ProvisionRequest{ClaimName: "claim-t4", Region: testRegion}
		if _, err := p.Provision(context.Background(), gpuPod("T4", tc.count), req); err != nil {
			t.Fatalf("Provision(T4 x%d): %v", tc.count, err)
		}
		if got := primaryType(f.lastSpec); got != tc.wantType {
			t.Fatalf("T4 x%d -> primary InstanceType %q, want %q", tc.count, got, tc.wantType)
		}
	}
}

func TestProvision_UnsupportedCountIsError(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)
	// T4 x2 has no instance type (there is no 2-GPU T4 shape): must error rather
	// than silently picking the x1 or x8 row.
	req := provider.ProvisionRequest{ClaimName: "claim-t4x2", Region: testRegion}
	if _, err := p.Provision(context.Background(), gpuPod("T4", 2), req); err == nil {
		t.Fatal("expected an error for an unsupported (accelerator, count) pair")
	}
	if f.runCnt != 0 {
		t.Fatalf("RunInstance called %d times, want 0", f.runCnt)
	}
}

func TestProvision_SpotSetsMarketOption(t *testing.T) {
	f := &fakeClient{runID: "i-spot"}
	p := newTestProvider(f)

	if _, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName:    "claim-spot",
		CapacityType: nebulav1alpha1.CapacitySpot,
		Region:       testRegion,
	}); err != nil {
		t.Fatalf("Provision: %v", err)
	}
	if !f.lastSpec.Spot {
		t.Fatalf("Spot = false, want true for a Spot request")
	}
}

func TestProvision_EmptyRegionIsError(t *testing.T) {
	f := &fakeClient{runID: "i-def"}
	p := newTestProvider(f)

	// There is NO default region: a request that omits one cannot build a client, so
	// Provision errors rather than silently guessing. In production every request
	// carries a region (ExpandRegions never yields an empty one; placement stamps it),
	// so this only guards a malformed request.
	if _, err := p.Provision(context.Background(), gpuPod("H100", 8), provider.ProvisionRequest{
		ClaimName: "claim-def",
	}); err == nil {
		t.Fatal("expected an error for a request with no region")
	}
	if f.runCnt != 0 {
		t.Fatalf("RunInstance called %d times, want 0 (no client built)", f.runCnt)
	}
}

func TestProvision_NoAcceleratorIsError(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)

	// EC2 GPU provisioning is by instance type; a Pod with no accelerator has no
	// instance type to launch, so it must error rather than silently guessing.
	req := provider.ProvisionRequest{ClaimName: "claim-cpu", Region: testRegion}
	if _, err := p.Provision(context.Background(), gpuPod("", 0), req); err == nil {
		t.Fatal("expected an error for a Pod requesting no accelerator")
	}
	if f.runCnt != 0 {
		t.Fatalf("RunInstance called %d times, want 0", f.runCnt)
	}
}

func TestProvision_Idempotent(t *testing.T) {
	f := &fakeClient{
		instances: []EC2Instance{{
			ID:     "i-existing",
			Tags:   map[string]string{ClaimTagKey: "claim-a"},
			State:  stateRunning,
			Region: testRegion,
		}},
	}
	p := newTestProvider(f)

	res, err := p.Provision(context.Background(), gpuPod("H100", 8),
		provider.ProvisionRequest{ClaimName: "claim-a", Region: testRegion})
	if err != nil {
		t.Fatalf("Provision: %v", err)
	}
	id := res.InstanceID
	// Raw EC2 id, since idempotent reuse returns the same clean id a fresh launch would.
	if id != "i-existing" {
		t.Fatalf("id = %q, want i-existing (idempotent reuse)", id)
	}
	if f.runCnt != 0 {
		t.Fatalf("RunInstance called %d times, want 0 (idempotent)", f.runCnt)
	}
}

func TestProvision_UnsupportedAccelerator(t *testing.T) {
	f := &fakeClient{}
	p := newTestProvider(f)
	req := provider.ProvisionRequest{ClaimName: "claim-x", Region: testRegion}
	if _, err := p.Provision(context.Background(), gpuPod("TPU-v4", 1), req); err == nil {
		t.Fatal("expected error for unsupported accelerator")
	}
}

func TestGetAndList_NormalizeInstance(t *testing.T) {
	f := &fakeClient{
		instances: []EC2Instance{{
			ID:                 "i-1",
			Tags:               map[string]string{ClaimTagKey: "claim-a"},
			State:              stateRunning,
			Region:             "eu-west-1",
			PublicEndpoint:     "ec2-1-2-3-4.compute.amazonaws.com",
			Spot:               true,
			StatusChecksPassed: true, // 2/2 checks passed => reported Running
		}},
	}
	p := newTestProvider(f)

	// Get with no region falls back to sweeping for the instance.
	got, err := p.Get(context.Background(), "i-1", "")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got == nil {
		t.Fatal("Get returned nil for an existing instance")
	}
	if got.ClaimName != "claim-a" || got.State != provider.InstanceRunning {
		t.Fatalf("Get = %+v", got)
	}
	if got.Region != "eu-west-1" {
		t.Fatalf("Region = %q, want eu-west-1 (reported by the instance)", got.Region)
	}
	if got.CapacityType != nebulav1alpha1.CapacitySpot {
		t.Fatalf("CapacityType = %q, want Spot", got.CapacityType)
	}
	if got.Endpoint != "ec2-1-2-3-4.compute.amazonaws.com" {
		t.Fatalf("Endpoint = %q", got.Endpoint)
	}

	// A missing instance is (nil, nil): absence == terminated per the contract.
	missing, err := p.Get(context.Background(), "i-gone", testRegion)
	if err != nil {
		t.Fatalf("Get(missing): %v", err)
	}
	if missing != nil {
		t.Fatalf("Get(missing) = %+v, want nil", missing)
	}

	list, err := p.List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	// List reports the raw EC2 id, alongside the Region a downstream Get/Terminate should
	// pass back so neither has to search for it.
	if len(list) != 1 || list[0].ID != "i-1" {
		t.Fatalf("List = %+v", list)
	}
}

func TestToInstance_RegionFromInstance(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	// The region-pinned client always stamps its region onto observed instances, so
	// toInstance passes it through verbatim (no default fallback exists anymore).
	inst := p.toInstance(EC2Instance{ID: "i-x", State: stateRunning, Region: "ap-south-1"})
	if inst.Region != "ap-south-1" {
		t.Fatalf("Region = %q, want ap-south-1 (reported by the instance)", inst.Region)
	}
}

func TestTerminate_Idempotent(t *testing.T) {
	f := &fakeClient{
		instances: []EC2Instance{{
			ID:     "i-1",
			Tags:   map[string]string{ClaimTagKey: "claim-a"},
			State:  stateRunning,
			Region: testRegion,
		}},
	}
	p := newTestProvider(f)

	// Empty id: nothing was provisioned; treat as already gone (no client call).
	if err := p.Terminate(context.Background(), "", testRegion); err != nil {
		t.Fatalf("Terminate(\"\"): %v", err)
	}
	if len(f.terminated) != 0 {
		t.Fatalf("Terminate(\"\") called client, want no-op")
	}
	// With a region: straight to that region's client.
	if err := p.Terminate(context.Background(), "i-1", testRegion); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if len(f.terminated) != 1 || f.terminated[0] != "i-1" {
		t.Fatalf("terminated = %v, want [i-1]", f.terminated)
	}
	// Without one: sweep the regions, confirm the instance lives in one, terminate there.
	if err := p.Terminate(context.Background(), "i-1", ""); err != nil {
		t.Fatalf("Terminate(no region): %v", err)
	}
	if len(f.terminated) != 2 {
		t.Fatalf("terminated = %v, want a second [i-1] from the sweep", f.terminated)
	}
}

// The region is what makes teardown reliable. An instance in a region the sweep does not
// visit — no pool declares it and nothing is in the client cache, the state of a freshly
// restarted process — is invisible to the sweep, which then reports success having
// terminated nothing and leaves the instance billing. The region reaches it regardless.
func TestTerminate_RegionOutsideTheSweep(t *testing.T) {
	f := &fakeClient{
		instances: []EC2Instance{{ID: "i-2", State: stateRunning, Region: "eu-west-1"}},
	}
	p := New(
		func(context.Context, string) (Client, error) { return f, nil },
		fakeCatalog{},
		func() []string { return nil }, // no pool declares a region
	)

	if regions := p.sweepRegions(); len(regions) != 0 {
		t.Fatalf("sweepRegions = %v, want none (the premise of this test)", regions)
	}
	if err := p.Terminate(context.Background(), "i-2", "eu-west-1"); err != nil {
		t.Fatalf("Terminate: %v", err)
	}
	if len(f.terminated) != 1 || f.terminated[0] != "i-2" {
		t.Fatalf("terminated = %v, want [i-2]", f.terminated)
	}
}

func TestClassifyProvisionError(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	const accel = "H100"
	denyAll := provider.BlockScope{DenyAll: true}
	// EC2 capacity/accelerator/quota blocks are confined to the adapter's region (an
	// exact-match Region pointer) AND to the requested accelerator (passed in by the
	// caller — the provider owns the whole scope). Only auth (DenyAll) ignores both.
	region, wantAccel := testRegion, accel
	onDemandRegional := provider.BlockScope{
		Accelerator: &wantAccel, CapacityType: nebulav1alpha1.CapacityOnDemand, Region: &region,
	}
	spotRegional := provider.BlockScope{
		Accelerator: &wantAccel, CapacityType: nebulav1alpha1.CapacitySpot, Region: &region,
	}

	// A Spot capacity failure the Client wraps with both sentinels: no-capacity
	// (category) + spot (tier), so the block confines to Spot in this region.
	spotNoCapacity := fmt.Errorf("%w: %w", provider.ErrNoCapacity, ErrSpotCapacity)
	stringNoCapacity := fmt.Errorf("InsufficientInstanceCapacity: no capacity")

	tests := []struct {
		name string
		err  error
		want provider.BlockScope
	}{
		{"auth is provider-wide (no region)", provider.ErrAuth, denyAll},
		// A vCPU/instance-limit quota is a regional, per-family, per-tier ceiling, so
		// it blocks only this accelerator + tier in this region — never the whole
		// provider. Otherwise one region's quota would strand every other region.
		{"quota is regional OnDemand", provider.ErrQuota, onDemandRegional},
		{"capacity is regional OnDemand", provider.ErrNoCapacity, onDemandRegional},
		{"wrapped capacity is regional", fmt.Errorf("run: %w", provider.ErrNoCapacity), onDemandRegional},
		{"spot capacity blocks only Spot in region", spotNoCapacity, spotRegional},
		{"string no-capacity is regional OnDemand", stringNoCapacity, onDemandRegional},
		// An error we cannot attribute blocks NOTHING, so no region is stamped either: a
		// transport failure or an unknown code is no evidence against this candidate.
		{"unknown blocks nothing", fmt.Errorf("weird transient blip"), provider.BlockScope{}},
		// The RAW code reaches here only if it escaped translation, and unrecognized means
		// unattributable, so it blocks nothing. The real path never presents it this way:
		// classifyEC2Error wraps InvalidFleetConfiguration as no-capacity first (it means
		// "not offered in this subnet's AZ" — see translate.go), which lands on the
		// ErrNoCapacity rows above and blocks this accelerator/tier/region.
		{"raw invalid fleet config blocks nothing",
			&smithy.GenericAPIError{Code: "InvalidFleetConfiguration", Message: "not supported in AZ"},
			provider.BlockScope{}},
		{"nil", nil, provider.BlockScope{}},
		// An image pull credential this adapter cannot honour is a fact about the POD, so it
		// blocks nothing. Region must NOT be stamped on: a scope carrying only a region
		// fences off every accelerator and tier there, on behalf of one Pod.
		{"image pull blocks nothing", provider.ErrImagePull, provider.BlockScope{}},
		{"wrapped image pull blocks nothing",
			fmt.Errorf("aws: unsupported image pull credential: %w", provider.ErrImagePull),
			provider.BlockScope{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := p.ClassifyProvisionError(tt.err, accel, testRegion)
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestCapabilities(t *testing.T) {
	p := newTestProvider(&fakeClient{})
	caps := p.Capabilities()
	if !caps.SupportsStop || !caps.SupportsSpot || !caps.NativeTags {
		t.Fatalf("unexpected caps: %+v", caps)
	}
	if caps.PreemptionNotice != 2*time.Minute {
		t.Fatalf("PreemptionNotice = %v, want 2m", caps.PreemptionNotice)
	}
	if p.Name() != provider.ProviderAWS {
		t.Fatalf("name = %q, want %q", p.Name(), provider.ProviderAWS)
	}
}

func TestExpandRegions(t *testing.T) {
	cases := []struct {
		name     string
		declared []string
		want     []string
	}{{
		name:     "nil is unconstrained: every default-enabled region",
		declared: nil,
		want: []string{
			"ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-southeast-1", "ap-southeast-2",
			"ca-central-1",
			"eu-central-1", "eu-north-1", "eu-west-1", "eu-west-2", "eu-west-3",
			"sa-east-1",
			"us-east-1", "us-east-2", "us-west-1", "us-west-2",
		},
	}, {
		name:     "empty behaves as nil",
		declared: []string{},
		want: []string{
			"ap-northeast-1", "ap-northeast-2", "ap-northeast-3", "ap-south-1", "ap-southeast-1", "ap-southeast-2",
			"ca-central-1",
			"eu-central-1", "eu-north-1", "eu-west-1", "eu-west-2", "eu-west-3",
			"sa-east-1",
			"us-east-1", "us-east-2", "us-west-1", "us-west-2",
		},
	}, {
		name:     "group token expands",
		declared: []string{"us"},
		want:     []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2"},
	}, {
		name:     "group token is case-insensitive",
		declared: []string{"EU"},
		want:     []string{"eu-central-1", "eu-north-1", "eu-west-1", "eu-west-2", "eu-west-3"},
	}, {
		name:     "literal region passes through",
		declared: []string{"us-east-1"},
		want:     []string{"us-east-1"},
	}, {
		// An opt-in region belongs to no group, so naming it explicitly is the only
		// way to reach it — that escape hatch must keep working.
		name:     "opt-in region passes through though no group contains it",
		declared: []string{"me-central-1"},
		want:     []string{"me-central-1"},
	}, {
		// Region names outlive this table, so an unknown value is FORWARDED, not
		// rejected: it fails later with AWS's own error instead of Nebula refusing a
		// region that shipped after this code did.
		name:     "unknown value is forwarded unvalidated",
		declared: []string{"us-east-9", "not-a-region"},
		want:     []string{"us-east-9", "not-a-region"},
	}, {
		// A group and one of its own members must not yield a duplicate: placement
		// walks the result, so a repeat would attempt the same region twice.
		name:     "group plus a member of it dedupes",
		declared: []string{"us", "us-east-1"},
		want:     []string{"us-east-1", "us-east-2", "us-west-1", "us-west-2"},
	}, {
		name:     "declared order is preserved across groups and literals",
		declared: []string{"ca", "us-east-1", "sa"},
		want:     []string{"ca-central-1", "us-east-1", "sa-east-1"},
	}, {
		name:     "whitespace is trimmed and empties dropped",
		declared: []string{" us-east-1 ", "", "  "},
		want:     []string{"us-east-1"},
	}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := ExpandRegions(tc.declared)
			if !slices.Equal(got, tc.want) {
				t.Fatalf("ExpandRegions(%v)\n got %v\nwant %v", tc.declared, got, tc.want)
			}
			// The method must agree with the package function: cmd/main.go's region
			// source calls the function while placement calls the method, and the two
			// diverging is exactly the bug that reports a live fleet as Terminated.
			p := newTestProvider(&fakeClient{})
			if m := p.ExpandRegions(tc.declared); !slices.Equal(m, got) {
				t.Fatalf("method %v != function %v", m, got)
			}
		})
	}
}

func TestRegionGroups_ExcludeOptInRegions(t *testing.T) {
	// Opt-in regions are disabled until an operator enables them. clientFor does not
	// cache build failures, so one in a group would fail its AMI/subnet resolution and
	// retry on EVERY poll tick, forever, for a region nobody asked for. Guard the
	// table against a well-meant future addition.
	optIn := []string{
		"af-south-1", "ap-east-1", "ap-east-2", "ap-south-2",
		"ap-southeast-3", "ap-southeast-4", "ap-southeast-5", "ap-southeast-6", "ap-southeast-7",
		"ca-west-1", "eu-central-2", "eu-south-1", "eu-south-2",
		"il-central-1", "me-central-1", "me-south-1", "mx-central-1",
	}
	all := ExpandRegions(nil)
	for _, r := range optIn {
		if slices.Contains(all, r) {
			t.Errorf("opt-in region %q must not be in any group (it is disabled by default)", r)
		}
	}
	// Separate IAM partitions: one credential set cannot reach them at all.
	for _, r := range all {
		if strings.HasPrefix(r, "us-gov-") || strings.HasPrefix(r, "cn-") {
			t.Errorf("region %q is in another IAM partition and must not be in a group", r)
		}
	}
}

func TestOfferings_StampsRegionAndFiltersByLiveProbe(t *testing.T) {
	// The region offers only g4dn.xlarge and p5.48xlarge (not g4dn.metal or
	// p4de.24xlarge), so those two rows stay available and the rest, though still
	// returned so the optimizer sees their price, are marked unavailable.
	f := &fakeClient{available: map[string]bool{
		"g4dn.xlarge": true,
		"p5.48xlarge": true,
	}}
	p := newTestProvider(f)

	offs, err := p.Offerings(context.Background())
	if err != nil {
		t.Fatalf("Offerings: %v", err)
	}
	// Every catalog row is returned (none dropped), each stamped with the region.
	if len(offs) != 5 {
		t.Fatalf("got %d offerings, want all 5 catalog rows", len(offs))
	}
	availByType := map[string]bool{}
	for _, o := range offs {
		if o.Region != testRegion {
			t.Fatalf("offering %q region = %q, want %q", o.AcceleratorID, o.Region, testRegion)
		}
		availByType[o.AcceleratorID] = o.Available
	}
	if !availByType["g4dn.xlarge"] || !availByType["p5.48xlarge"] {
		t.Fatalf("types the region offers must stay available: %+v", availByType)
	}
	if availByType["g4dn.metal"] || availByType["p4de.24xlarge"] {
		t.Fatalf("types the region does not offer must be unavailable: %+v", availByType)
	}
}

func TestOfferings_ProbeErrorPropagates(t *testing.T) {
	// A probe failure must surface as an error, not silently report the stale seed
	// availability as truth.
	f := &fakeClient{availErr: errors.New("throttled")}
	p := newTestProvider(f)
	if _, err := p.Offerings(context.Background()); err == nil {
		t.Fatal("expected the probe error to propagate")
	}
}

func TestToState(t *testing.T) {
	// The EC2-state → provider-state mapping is load-bearing: an unknown state
	// must map to Pending so the poll loop keeps watching rather than declaring a
	// premature terminal state; stop/stopping fold into Terminated. "running" is
	// gated on the reachability checks — running-but-unchecked stays Pending.
	cases := []struct {
		state        string
		checksPassed bool
		want         provider.InstanceState
	}{
		{stateRunning, true, provider.InstanceRunning},
		// Running but the 2/2 checks have not cleared yet: still Pending.
		{stateRunning, false, provider.InstancePending},
		{statePending, true, provider.InstancePending},
		{statePending, false, provider.InstancePending},
		{stateStopping, false, provider.InstanceTerminated},
		{stateStopped, false, provider.InstanceTerminated},
		{stateShuttingDown, false, provider.InstanceTerminated},
		{stateTerminated, false, provider.InstanceTerminated},
		{"", false, provider.InstancePending},
		{"rebooting", true, provider.InstancePending},
	}
	for _, tc := range cases {
		if got := toState(tc.state, tc.checksPassed); got != tc.want {
			t.Fatalf("toState(%q, checks=%v) = %q, want %q", tc.state, tc.checksPassed, got, tc.want)
		}
	}
}

func TestResolveGPUAMI_PicksNewestAndErrsWhenAbsent(t *testing.T) {
	// The newest CreationDate wins so a driver/runtime refresh is picked up.
	f := &fakeEC2{imagesOut: &ec2.DescribeImagesOutput{Images: []ec2types.Image{
		{ImageId: awssdk.String("ami-old"), CreationDate: awssdk.String("2024-01-01T00:00:00.000Z")},
		{ImageId: awssdk.String("ami-new"), CreationDate: awssdk.String("2026-01-01T00:00:00.000Z")},
	}}}
	c := newSDKClient(f)
	got, err := c.resolveGPUAMI(context.Background())
	if err != nil {
		t.Fatalf("resolveGPUAMI: %v", err)
	}
	if got != "ami-new" {
		t.Fatalf("resolveGPUAMI = %q, want ami-new (newest)", got)
	}

	// No matching image => ErrConfig (AWS unusable in the region, non-fatal skip).
	f2 := &fakeEC2{imagesOut: &ec2.DescribeImagesOutput{}}
	c2 := newSDKClient(f2)
	if _, err := c2.resolveGPUAMI(context.Background()); !errors.Is(err, ErrConfig) {
		t.Fatalf("resolveGPUAMI(no images) err = %v, want ErrConfig", err)
	}
}

func TestDiscoverDefaultSubnets_ReturnsPerAZTargets(t *testing.T) {
	f := &fakeEC2{subnetsOut: &ec2.DescribeSubnetsOutput{Subnets: []ec2types.Subnet{
		{SubnetId: awssdk.String("subnet-a"), AvailabilityZone: awssdk.String("us-east-1a")},
		{SubnetId: awssdk.String("subnet-b"), AvailabilityZone: awssdk.String("us-east-1b")},
	}}}
	c := newSDKClient(f)
	got, err := c.discoverDefaultSubnets(context.Background())
	if err != nil {
		t.Fatalf("discoverDefaultSubnets: %v", err)
	}
	if len(got) != 2 || got[0].id != "subnet-a" || got[1].az != "us-east-1b" {
		t.Fatalf("subnets = %+v, want the two default-VPC subnets", got)
	}

	// No default VPC (empty result) is not an error: launch just skips zone failover.
	f2 := &fakeEC2{subnetsOut: &ec2.DescribeSubnetsOutput{}}
	c2 := newSDKClient(f2)
	if got, err := c2.discoverDefaultSubnets(context.Background()); err != nil || len(got) != 0 {
		t.Fatalf("discoverDefaultSubnets(no default VPC) = (%+v, %v), want (nil, nil)", got, err)
	}
}
