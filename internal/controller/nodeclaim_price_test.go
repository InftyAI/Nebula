/*
Copyright 2026.

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
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// pricedProvider is a fakeProvider that also implements provider.Pricer. A separate type
// on purpose: adding PricePerHour to fakeProvider itself would silently start pricing every
// other test's claims, and the no-Pricer path is one this must keep exercising.
type pricedProvider struct {
	*fakeProvider
	rate float64
	err  error
	got  []provider.PriceRequest // every request, in order
}

func (p *pricedProvider) PricePerHour(req provider.PriceRequest) (float64, error) {
	p.got = append(p.got, req)
	return p.rate, p.err
}

// newPricedReconciler wires a reconciler whose only provider is a Pricer named "fake".
func newPricedReconciler(t *testing.T, objs []client.Object, pp *pricedProvider) (*NodeClaimReconciler, client.Client) {
	t.Helper()
	r, c := newClaimReconciler(t, objs)
	r.Providers = func(name string) (provider.Provider, bool) {
		if name == pp.name {
			return pp, true
		}
		return nil, false
	}
	return r, c
}

// gpuPod is a served Pod shaped like a real GPU workload: the accelerator label, a
// nvidia.com/gpu count, and a CPU/memory reservation.
func gpuPod(accelerator string, gpus int64, cpu, memory string) *corev1.Pod {
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: accelerator}
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse(cpu),
			corev1.ResourceMemory: resource.MustParse(memory),
			"nvidia.com/gpu":      *resource.NewQuantity(gpus, resource.DecimalSI),
		},
	}
	return pod
}

func TestRecordPrice_WritesRateAndRequest(t *testing.T) {
	pod := gpuPod("H100", 2, "4", "8Gi")
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	claim.Spec.CapacityType = nebulav1alpha1.CapacityOnDemand
	pp := &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 7.9}
	r, c := newPricedReconciler(t, []client.Object{pod, claim}, pp)

	reconcileClaim(t, r, "c1")

	// Four decimals, so a sub-cent rate is not rounded into the UNPRICED-looking 0.
	if got := getClaim(t, c, "c1").Status.PriceUSDPerHour; got != "7.9000" {
		t.Fatalf("PriceUSDPerHour = %q, want %q", got, "7.9000")
	}
	if len(pp.got) == 0 {
		t.Fatal("PricePerHour was never called")
	}
	want := provider.PriceRequest{
		AcceleratorType: "H100", Count: 2,
		CapacityType: nebulav1alpha1.CapacityOnDemand,
		// The Pod's own reservation, which only a provider metering CPU/memory apart from
		// the accelerator charges for — but the request carries it either way.
		CPUCores: 4, MemoryMiB: 8192,
	}
	if pp.got[0] != want {
		t.Fatalf("PriceRequest = %+v, want %+v", pp.got[0], want)
	}
}

// The rate is pinned at the first write: re-pricing on every reconcile would let a catalog
// edit retroactively reprice a running instance and rewrite cost history.
func TestRecordPrice_PinnedOnce(t *testing.T) {
	pod := gpuPod("H100", 1, "1", "1Gi")
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	pp := &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 3.95}
	r, c := newPricedReconciler(t, []client.Object{pod, claim}, pp)

	reconcileClaim(t, r, "c1")
	if got := getClaim(t, c, "c1").Status.PriceUSDPerHour; got != "3.9500" {
		t.Fatalf("first pass: PriceUSDPerHour = %q, want 3.9500", got)
	}

	pp.rate = 99.0 // the catalog changes under a running instance
	reconcileClaim(t, r, "c1")

	if got := getClaim(t, c, "c1").Status.PriceUSDPerHour; got != "3.9500" {
		t.Fatalf("second pass: PriceUSDPerHour = %q, want the pinned 3.9500", got)
	}
	if len(pp.got) != 1 {
		t.Fatalf("PricePerHour called %d times, want 1 — a priced claim must not be re-priced", len(pp.got))
	}
}

// Unpriceable is not a failure. Every one of these leaves the field EMPTY (which readers
// treat as UNPRICED, never as free) and must not fail the reconcile — the claim's real job
// is teardown, and a missing cost column may never block it.
func TestRecordPrice_UnpriceableLeavesEmpty(t *testing.T) {
	cases := map[string]struct {
		prov func() provider.Provider
		pod  *corev1.Pod
	}{
		"provider implements no Pricer": {
			prov: func() provider.Provider { return &fakeProvider{name: "fake"} },
			pod:  gpuPod("H100", 1, "1", "1Gi"),
		},
		"no catalog row": {
			prov: func() provider.Provider {
				return &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, err: provider.ErrNoPrice}
			},
			pod: gpuPod("H100", 1, "1", "1Gi"),
		},
		"malformed request is loud but not fatal": {
			prov: func() provider.Provider {
				return &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, err: fmt.Errorf("bad request")}
			},
			pod: gpuPod("H100", 1, "1", "1Gi"),
		},
		"contradictory accelerator request": {
			prov: func() provider.Provider {
				return &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 1}
			},
			// nvidia.com/gpu with no accelerator label: util.AcceleratorRequest rejects it.
			pod: func() *corev1.Pod {
				p := gpuPod("H100", 1, "1", "1Gi")
				p.Labels = nil
				return p
			}(),
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			claim := newClaim("c1", "p1", "default", "uid-1", "fake")
			r, c := newClaimReconciler(t, []client.Object{tc.pod, claim})
			prov := tc.prov()
			r.Providers = func(string) (provider.Provider, bool) { return prov, true }

			reconcileClaim(t, r, "c1") // fails the test on any error

			got := getClaim(t, c, "c1")
			if got.Status.PriceUSDPerHour != "" {
				t.Fatalf("PriceUSDPerHour = %q, want empty (UNPRICED)", got.Status.PriceUSDPerHour)
			}
			// The claim's own job must still have happened.
			if got.Status.Phase != nebulav1alpha1.NodeClaimBound {
				t.Fatalf("phase = %q, want Bound — pricing must not disturb the ledger", got.Status.Phase)
			}
		})
	}
}

// An unregistered provider cannot be priced and must not panic on the nil returned
// alongside ok=false.
func TestRecordPrice_UnknownProvider(t *testing.T) {
	pod := gpuPod("H100", 1, "1", "1Gi")
	claim := newClaim("c1", "p1", "default", "uid-1", "gone")
	r, c := newClaimReconciler(t, []client.Object{pod, claim})
	r.Providers = func(string) (provider.Provider, bool) { return nil, false }

	reconcileClaim(t, r, "c1")

	if got := getClaim(t, c, "c1").Status.PriceUSDPerHour; got != "" {
		t.Fatalf("PriceUSDPerHour = %q, want empty", got)
	}
}

// A CPU-only claim still reaches the Pricer: a provider that can run CPU-only work
// (Modal) prices it, and one that cannot answers ErrNoPrice, which is its call to make
// rather than ours to pre-empt.
func TestRecordPrice_CPUOnlyReachesPricer(t *testing.T) {
	pod := newPod("p1", "default", "uid-1", corev1.PodRunning)
	pod.Spec.Containers[0].Resources = corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:    resource.MustParse("500m"),
			corev1.ResourceMemory: resource.MustParse("512Mi"),
		},
	}
	claim := newClaim("c1", "p1", "default", "uid-1", "fake")
	pp := &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 0.0276}
	r, c := newPricedReconciler(t, []client.Object{pod, claim}, pp)

	reconcileClaim(t, r, "c1")

	if got := getClaim(t, c, "c1").Status.PriceUSDPerHour; got != "0.0276" {
		t.Fatalf("PriceUSDPerHour = %q, want 0.0276", got)
	}
	want := provider.PriceRequest{CPUCores: 0.5, MemoryMiB: 512}
	if pp.got[0] != want {
		t.Fatalf("PriceRequest = %+v, want %+v", pp.got[0], want)
	}
}
