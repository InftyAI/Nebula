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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/failover"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/util"
)

// The collectors are package-level and registered once, so every test in this package
// shares them. Assertions are on the DELTA a reconcile produced, never on an absolute
// value — otherwise they would pass or fail depending on run order.

func counterVal(t *testing.T, vec *prometheus.CounterVec, l prometheus.Labels) float64 {
	t.Helper()
	c, err := vec.GetMetricWith(l)
	if err != nil {
		t.Fatalf("GetMetricWith(%v): %v", l, err)
	}
	return testutil.ToFloat64(c)
}

// histStats reads a histogram series' sample count and sum. GetMetricWith creates the
// series when absent, so an unobserved label set reads as (0, 0) rather than erroring.
func histStats(t *testing.T, vec *prometheus.HistogramVec, l prometheus.Labels) (uint64, float64) {
	t.Helper()
	obs, err := vec.GetMetricWith(l)
	if err != nil {
		t.Fatalf("GetMetricWith(%v): %v", l, err)
	}
	pb := &dto.Metric{}
	if err := obs.(prometheus.Metric).Write(pb); err != nil {
		t.Fatalf("write metric %v: %v", l, err)
	}
	return pb.GetHistogram().GetSampleCount(), pb.GetHistogram().GetSampleSum()
}

// decisionLabels is the candidate label set for a Pod built by gatedPod: the count is 1
// because an accelerator-type label with no explicit nvidia.com/gpu limit means one GPU,
// and the region is the placeholder because poolWith declares none.
func decisionLabels(prov string, tier nebulav1alpha1.CapacityType, region, accel string) prometheus.Labels {
	return prometheus.Labels{
		"provider":          prov,
		"region":            orNoneLabel(region),
		"capacity_type":     orNoneLabel(string(tier)),
		"accelerator":       orNoneLabel(accel),
		"accelerator_count": "1",
	}
}

// skipLabels is the candidate-skip label set. A skip decided before the walk reaches
// the region axis carries no region, which renders as the placeholder.
func skipLabels(prov string, tier nebulav1alpha1.CapacityType, region, reason string) prometheus.Labels {
	return prometheus.Labels{
		"provider":      orNoneLabel(prov),
		"capacity_type": orNoneLabel(string(tier)),
		"region":        orNoneLabel(region),
		"reason":        reason,
	}
}

func orNoneLabel(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

// Every reason a Pod goes unplaced must land on its OWN series, because each points at a
// different owner: no_pool/invalid_request need a human to fix the request, all_blocked
// clears itself, and no_candidate needs an operator to widen the pool. Collapsing any two
// of them would make the metric unactionable.
func TestPlacement_DeferralReasons(t *testing.T) {
	tests := []struct {
		name string
		pool string // the pool label expected on the metric
		want string
		// build returns the objects to seed and the providers to register.
		build func() ([]client.Object, []*fakeProvider, Blocklister)
	}{{
		// The Pod names a pool that does not exist. The pool label is deliberately the
		// placeholder, NOT the unresolved name — that string is a user-controlled Pod
		// label, so filing it would mint a series per typo.
		name: "missing pool",
		pool: "none",
		want: metrics.DeferNoPool,
		build: func() ([]client.Object, []*fakeProvider, Blocklister) {
			return []client.Object{gatedPod("d1", "default", "uid-d1", "ghost-pool", "H100")},
				[]*fakeProvider{{name: "p1"}}, nil
		},
	}, {
		// nvidia.com/gpu with no accelerator-type label: malformed, not CPU-only.
		name: "invalid accelerator request",
		pool: "pool",
		want: metrics.DeferInvalidRequest,
		build: func() ([]client.Object, []*fakeProvider, Blocklister) {
			pod := gatedPod("d1", "default", "uid-d1", "pool", "")
			pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
				util.NvidiaGPUResource: resource.MustParse("1"),
			}
			pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "p1")
			return []client.Object{pod, pool}, []*fakeProvider{{name: "p1"}}, nil
		},
	}, {
		// Servable, but every candidate is blocked: self-clearing, and the caller
		// requeues for the expiry.
		name: "all candidates blocked",
		pool: "pool",
		want: metrics.DeferAllBlocked,
		build: func() ([]client.Object, []*fakeProvider, Blocklister) {
			pod := gatedPod("d1", "default", "uid-d1", "pool", "H100")
			pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "p1")
			return []client.Object{pod, pool}, []*fakeProvider{{name: "p1"}},
				&fakeBlocklist{blocked: []failover.Candidate{{Provider: "p1"}}}
		},
	}, {
		// No provider in the pool offers the accelerator: no TTL will ever fix this.
		name: "no servable candidate",
		pool: "pool",
		want: metrics.DeferNoCandidate,
		build: func() ([]client.Object, []*fakeProvider, Blocklister) {
			pod := gatedPod("d1", "default", "uid-d1", "pool", "H100")
			pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "p1")
			return []client.Object{pod, pool}, []*fakeProvider{{name: "p1", gpus: []string{"A100"}}}, nil
		},
	}, {
		// A claim from a prior same-named Pod has not been reaped yet.
		name: "stale claim",
		pool: "pool",
		want: metrics.DeferStaleClaim,
		build: func() ([]client.Object, []*fakeProvider, Blocklister) {
			pod := gatedPod("d1", "default", "uid-new", "pool", "H100")
			pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "p1")
			stale := &nebulav1alpha1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: "default-d1"},
				Spec: nebulav1alpha1.NodeClaimSpec{
					PodRef: nebulav1alpha1.PodReference{Namespace: "default", Name: "d1", UID: "uid-old"},
				},
			}
			return []client.Object{pod, pool, stale}, []*fakeProvider{{name: "p1"}}, nil
		},
	}}

	// Every reason value, so each case can assert the others did NOT move.
	all := []string{
		metrics.DeferNoPool, metrics.DeferInvalidRequest, metrics.DeferAllBlocked,
		metrics.DeferNoCandidate, metrics.DeferStaleClaim,
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			objs, provs, bl := tt.build()
			before := map[string]float64{}
			for _, reason := range all {
				before[reason] = counterVal(t, metrics.PlacementDeferrals,
					prometheus.Labels{"pool": tt.pool, "reason": reason})
			}

			r, _ := newPlacementReconciler(t, objs, provs...)
			r.Blocklist = bl
			reconcilePod(t, r, "default", "d1")

			for _, reason := range all {
				want := 0.0
				if reason == tt.want {
					want = 1
				}
				got := counterVal(t, metrics.PlacementDeferrals,
					prometheus.Labels{"pool": tt.pool, "reason": reason}) - before[reason]
				if got != want {
					t.Fatalf("deferrals{pool=%q,reason=%q} delta = %v, want %v", tt.pool, reason, got, want)
				}
			}
		})
	}
}

// The skip counter is the only view into WHY the walk passed over a candidate, and each
// reason is a different fix: register the provider, drop the tier from the pool, or pick
// an accelerator the provider offers. One reconcile can file several — the walk visits
// every candidate before giving up.
func TestPlacement_CandidateSkipReasons(t *testing.T) {
	unregistered := skipLabels("ghost", nebulav1alpha1.CapacitySpot, "", metrics.SkipProviderUnregistered)
	tierUnsupported := skipLabels("p1", nebulav1alpha1.CapacitySpot, "", metrics.SkipCapacityUnsupported)
	accelUnsupported := skipLabels("p2", nebulav1alpha1.CapacitySpot, "", metrics.SkipAcceleratorUnsupported)
	before := map[string]float64{
		"unregistered": counterVal(t, metrics.CandidateSkips, unregistered),
		"tier":         counterVal(t, metrics.CandidateSkips, tierUnsupported),
		"accel":        counterVal(t, metrics.CandidateSkips, accelUnsupported),
	}

	// A Spot-only pool over three providers: one unregistered, one with no Spot tier,
	// one that has Spot but not this accelerator. Nothing is placeable, and each
	// candidate is skipped for its own reason.
	pod := gatedPod("s1", "default", "uid-s1", "pool", "H100")
	pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacitySpot}, "ghost", "p1", "p2")
	r, _ := newPlacementReconciler(t, []client.Object{pod, pool},
		&fakeProvider{name: "p1", spot: false},
		&fakeProvider{name: "p2", spot: true, gpus: []string{"A100"}},
	)
	reconcilePod(t, r, "default", "s1")

	if got := counterVal(t, metrics.CandidateSkips, unregistered) - before["unregistered"]; got != 1 {
		t.Fatalf("provider_unregistered skips delta = %v, want 1", got)
	}
	if got := counterVal(t, metrics.CandidateSkips, tierUnsupported) - before["tier"]; got != 1 {
		t.Fatalf("capacity_type_unsupported skips delta = %v, want 1", got)
	}
	if got := counterVal(t, metrics.CandidateSkips, accelUnsupported) - before["accel"]; got != 1 {
		t.Fatalf("accelerator_unsupported skips delta = %v, want 1", got)
	}
}

// The pool a Pod asks for is a Pod LABEL: user-controlled and unbounded. An unresolvable
// one must never reach a metric label, or a mislabeled workload could mint a time series
// per typo and blow up the registry.
func TestPlacement_UnresolvedPoolNameNeverBecomesALabel(t *testing.T) {
	ghost := prometheus.Labels{"pool": "ghost-pool", "reason": metrics.DeferNoPool}
	before := counterVal(t, metrics.PlacementDeferrals, ghost)

	pod := gatedPod("d2", "default", "uid-d2", "ghost-pool", "H100")
	r, _ := newPlacementReconciler(t, []client.Object{pod}, &fakeProvider{name: "p1"})
	reconcilePod(t, r, "default", "d2")

	if got := counterVal(t, metrics.PlacementDeferrals, ghost) - before; got != 0 {
		t.Fatalf("deferrals{pool=%q} delta = %v, want 0 (the name must not be filed)", "ghost-pool", got)
	}
}

// A placed Pod is counted on the candidate it actually landed on, and its wait is
// measured from POD CREATION — not from the reconcile that placed it. The distinction is
// the whole value of the metric: a Pod that sat gated for two minutes waiting out a
// failover block must report two minutes, not the microseconds the final pass took.
func TestPlacement_RecordsDecisionAndWaitFromPodCreation(t *testing.T) {
	landed := decisionLabels("p1", nebulav1alpha1.CapacityOnDemand, "", "H100")
	beforeCount := counterVal(t, metrics.PlacementDecisions, landed)
	beforeSamples, beforeSum := histStats(t, metrics.PlacementWaitDuration, landed)

	pod := gatedPod("m1", "default", "uid-m1", "pool", "H100")
	pod.CreationTimestamp = metav1.NewTime(time.Now().Add(-2 * time.Minute))
	pool := poolWith("pool", []nebulav1alpha1.CapacityType{nebulav1alpha1.CapacityOnDemand}, "p1")
	r, _ := newPlacementReconciler(t, []client.Object{pod, pool}, &fakeProvider{name: "p1"})
	reconcilePod(t, r, "default", "m1")

	if got := counterVal(t, metrics.PlacementDecisions, landed) - beforeCount; got != 1 {
		t.Fatalf("decisions delta = %v, want 1", got)
	}
	samples, sum := histStats(t, metrics.PlacementWaitDuration, landed)
	if got := samples - beforeSamples; got != 1 {
		t.Fatalf("wait observations delta = %d, want 1", got)
	}
	// Two minutes of gated time, minus nothing: anything near zero means the clock
	// started at the reconcile instead of at the Pod.
	if got := sum - beforeSum; got < 100 {
		t.Fatalf("observed wait = %vs, want >= 100s (measured from Pod creation)", got)
	}
}

// The tier a Pod LANDS on is the cost question, so the decision must be filed against
// the fallback actually used — not the tier the pool asked for first. A fleet sliding
// from Spot to OnDemand is a cost regression with no error anywhere; this series and the
// skip counter that explains it are the only signals.
func TestPlacement_DecisionRecordsTheFallbackTierActuallyUsed(t *testing.T) {
	spot := decisionLabels("p1", nebulav1alpha1.CapacitySpot, "", "H100")
	onDemand := decisionLabels("p1", nebulav1alpha1.CapacityOnDemand, "", "H100")
	blockedSkip := skipLabels("p1", nebulav1alpha1.CapacitySpot, "", metrics.SkipBlocked)
	beforeSpot := counterVal(t, metrics.PlacementDecisions, spot)
	beforeOnDemand := counterVal(t, metrics.PlacementDecisions, onDemand)
	beforeSkip := counterVal(t, metrics.CandidateSkips, blockedSkip)

	pod := gatedPod("m2", "default", "uid-m2", "pool", "H100")
	pool := poolWith("pool", []nebulav1alpha1.CapacityType{
		nebulav1alpha1.CapacitySpot, nebulav1alpha1.CapacityOnDemand,
	}, "p1")
	r, _ := newPlacementReconciler(t, []client.Object{pod, pool}, &fakeProvider{name: "p1", spot: true})
	r.Blocklist = &fakeBlocklist{blocked: []failover.Candidate{
		{Provider: "p1", CapacityType: nebulav1alpha1.CapacitySpot},
	}}
	reconcilePod(t, r, "default", "m2")

	if got := counterVal(t, metrics.PlacementDecisions, onDemand) - beforeOnDemand; got != 1 {
		t.Fatalf("OnDemand decisions delta = %v, want 1", got)
	}
	if got := counterVal(t, metrics.PlacementDecisions, spot) - beforeSpot; got != 0 {
		t.Fatalf("Spot decisions delta = %v, want 0 (Spot was blocked)", got)
	}
	// And the skip counter is what explains the fallback after the fact.
	if got := counterVal(t, metrics.CandidateSkips, blockedSkip) - beforeSkip; got != 1 {
		t.Fatalf("blocked Spot skips delta = %v, want 1", got)
	}
}
