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

package metrics

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// Reason values for the placement deferral label: why one reconcile ended without
// placing the Pod. Closed set, and each value points at a DIFFERENT owner — which is
// the whole reason for splitting them:
//
//   - no_pool / invalid_request: the request is wrong. Nobody is retrying their way out
//     of these; a human must edit the Pod (or the workload that generates it).
//   - all_blocked: the request is fine and a candidate exists, but failover is holding
//     it off. Self-clearing, and the Pod is already requeued for the moment it frees.
//   - no_candidate: the pool cannot serve this request at all today. Self-clearing only
//     if an operator adds a provider or a provider registers.
//   - stale_claim: a NodeClaim from a prior same-named Pod has not been reaped yet.
//     Self-clearing in seconds; a sustained rate means the NodeClaim backstop is stuck.
const (
	DeferNoPool         = "no_pool"
	DeferInvalidRequest = "invalid_request"
	DeferAllBlocked     = "all_blocked"
	DeferNoCandidate    = "no_candidate"
	DeferStaleClaim     = "stale_claim"
)

// Reason values for the candidate skip label: why the placement walk passed over one
// (tier, provider, region) candidate. Only "blocked" clears on its own.
const (
	SkipProviderUnregistered   = "provider_unregistered"
	SkipCapacityUnsupported    = "capacity_type_unsupported"
	SkipAcceleratorUnsupported = "accelerator_unsupported"
	SkipBlocked                = "blocked"
)

var (
	// PlacementDecisions counts Pods actually placed, by the candidate they landed on.
	// It carries the same labels as the provisioning metrics on purpose: the two are
	// joinable without label surgery, so "placed on Spot but never provisioned" is one
	// query rather than a correlation exercise. The tier breakdown is the money
	// question — a fleet quietly sliding from Spot to OnDemand is a cost regression
	// with no error anywhere.
	PlacementDecisions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_placement_decisions_total",
		Help: "Pods placed, by provider, region, capacity type, accelerator type and accelerator count.",
	}, candidateLabels)

	// PlacementWaitDuration measures from Pod creation to the gate being removed: the
	// user-visible queue time BEFORE provisioning starts, which
	// nebula_instance_ready_duration_seconds then continues from. Together they cover
	// the whole path from `kubectl apply` to a Running instance.
	//
	// Unlike the ready duration, this one has no restart gap: the start timestamp is
	// the Pod's own creationTimestamp, so a placement that happens after a manager
	// restart still reports the true total wait. Buckets span three orders of
	// magnitude because the honest range does: an unblocked Pod is placed in
	// milliseconds, while one waiting out a failover block waits the blocklist TTL.
	PlacementWaitDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nebula_placement_wait_duration_seconds",
		Help: "Time from Pod creation to placement (scheduling gate removal), by provider, region, " +
			"capacity type, accelerator type and accelerator count.",
		Buckets: []float64{0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800},
	}, candidateLabels)

	// PlacementDeferrals counts reconciles that ended without placing the Pod, by
	// reason.
	//
	// It counts DEFERRALS, not Pods: a gated Pod is reconciled again on every requeue
	// and resync, so one Pod stuck for an hour contributes many increments. That makes
	// the rate a measure of placement pressure, not a population count — for "how many
	// Pods are stuck right now", read the SchedulingGated Pod count from
	// kube-state-metrics and use this series to explain WHY.
	PlacementDeferrals = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_placement_deferrals_total",
		Help: "Reconciles that ended without placing a Pod, by pool and reason " +
			"(no_pool, invalid_request, all_blocked, no_candidate, stale_claim).",
	}, []string{"pool", "reason"})

	// CandidateSkips counts individual (tier, provider, region) candidates passed over
	// during the placement walk. This is the only view into failover actually working:
	// when every Pod lands on OnDemand, a rate on {capacity_type="Spot",
	// reason="blocked"} is the explanation, and one on
	// reason="capacity_type_unsupported" says the pool is misconfigured instead.
	//
	// Cardinality is bounded by pool configuration (providers x tiers x regions x four
	// reasons), not by Pod count.
	CandidateSkips = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_placement_candidate_skips_total",
		Help: "Placement candidates skipped, by provider, capacity type, region and reason.",
	}, []string{"provider", "capacity_type", "region", "reason"})
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		PlacementDecisions,
		PlacementWaitDuration,
		PlacementDeferrals,
		CandidateSkips,
	)
}

// ObservePlacement records one Pod placed onto the candidate l describes, having
// waited waited since it was created.
func ObservePlacement(l Labels, waited time.Duration) {
	PlacementDecisions.WithLabelValues(l.values()...).Inc()
	PlacementWaitDuration.WithLabelValues(l.values()...).Observe(waited.Seconds())
}

// RecordDeferral records one reconcile that placed nothing.
//
// pool MUST be the name of a NodePool that actually exists, or "" for the deferral
// where it does not (DeferNoPool). The pool a Pod asks for is a Pod LABEL — user
// controlled and unbounded — so filing the unresolved string here would let a
// mislabeled workload mint a new time series per typo. Once the pool has been
// resolved to a real object, its name is bounded by cluster resources and safe.
func RecordDeferral(pool, reason string) {
	PlacementDeferrals.WithLabelValues(orNone(pool), reason).Inc()
}

// RecordCandidateSkip records one candidate the placement walk passed over. region is
// empty for the skips decided before the walk reaches the region axis (an
// unregistered provider, an unservable tier, a missing accelerator), which is honest:
// those rule out every region at once.
func RecordCandidateSkip(prov string, tier nebulav1alpha1.CapacityType, region, reason string) {
	CandidateSkips.WithLabelValues(orNone(prov), orNone(string(tier)), orNone(region), reason).Inc()
}
