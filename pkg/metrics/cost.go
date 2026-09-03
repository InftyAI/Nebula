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
	"fmt"
	"math"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

// CostTotal accumulates spend one CLOSED WINDOW at a time, which is what makes it billable:
// increase(...[w]) over any window is a pure function of that window, so a consumer replaying
// an old window re-derives the same dollars and can upsert them idempotently. That holds only for a
// series scraped before its first charge, which is what TouchSeries is for — and not at all for one
// born after that pass has run.
//
// Deliberately carries no claim identity. A per-claim series would churn — one per instance ever
// created, retained until the process exits — and worse, a claim that lived and died between two
// window boundaries could not be billed from it at all: differencing a cumulative per-claim series
// needs a sample at each boundary, and a short-lived instance has neither. Aggregating first does
// not help, since a sum over a changing claim set is not monotonic. Per-claim spend lives in
// NodeClaim.status.estimatedCostUSD (the EST_COST column) instead.
var CostTotal = newCostTotal(nil)

// Deliberately no init here: CostTotal cannot self-register the way every other metric in this
// package does, because its label names are not known until --cost-labels is parsed. See InitCost.

func newCostTotal(podKeys []string) *prometheus.CounterVec {
	// The DERIVED names, not the Pod keys the operator configured: a qualified key is not a legal
	// Prometheus label name. Derived here rather than stored, so the counter's shape and what
	// CostLabelKeys hands the stamper cannot disagree.
	return prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "nebula_cost_usd_total",
			Help: "Cumulative USD billed by external instances, added window by window as it accrues.",
		},
		withExtra(append([]string{"phase"}, metricNames(podKeys)...)...),
	)
}

// InitCost fixes the attribution dimension the cost counter carries, from --cost-labels, and
// registers it. MUST be called once from main after flags are parsed and before the manager
// starts — cost is the one metric here that is not scraped until it is.
//
// Two facts force that. A CounterVec's label names are set at CONSTRUCTION, while the flag is not
// known until main runs; and a Prometheus registry remembers a metric NAME's label dimensions for
// the life of the process even across Unregister, so registering a placeholder first would
// permanently forbid the real shape. Nothing is at risk in the gap: no window can be recorded
// before the manager starts.
//
// Not safe against concurrent recording.
func InitCost(podKeys []string) error {
	configureCost(podKeys)
	if err := ctrlmetrics.Registry.Register(CostTotal); err != nil {
		// The derived names, since those are what the registry objected to — a shadowing key looks
		// innocent until you see what it emits as.
		return fmt.Errorf("registering cost counter with labels %v: %w", metricNames(podKeys), err)
	}
	return nil
}

// configureCost rebuilds the counter for a label set. Split out so tests can swap the dimension
// without touching the process-wide registry, which would refuse the second shape it ever saw.
func configureCost(podKeys []string) {
	CostTotal = newCostTotal(podKeys)
	costLabelKeys = podKeys

	costSeriesMu.Lock()
	defer costSeriesMu.Unlock()
	costSeries = nil
	costSeriesWarned = false
}

// RecordWindow books the dollars one claim ran up over a single accrual window, attributed with
// the labels stamped on the claim (NodeClaimStatus.CostLabels).
//
// Call it only AFTER the window has been persisted: a counter has no idempotency key, so a
// window added twice is charged twice. Nothing is lost by waiting, because a failed write leaves
// the anchor in place and the next tick re-derives the same window.
func RecordWindow(l Labels, phase string, attribution map[string]string, usd float64) {
	// A zero window is the ordinary case for a claim whose anchor was just opened, and a
	// negative one would panic. Neither is worth a series.
	//
	// Non-finite is checked separately because it passes every comparison above: a counter cannot
	// be decremented, so one NaN added here is permanent, and it spreads — any sum() spanning that
	// series is NaN too, taking the whole fleet's cost query with it. Cheaper to refuse here than
	// to trust every caller's own parsing (see controller.finalRate).
	if usd <= 0 || math.IsNaN(usd) || math.IsInf(usd, 0) {
		return
	}
	values := l.values(append([]string{phase}, attributionValues(attribution)...)...)
	noteSeries(values)
	CostTotal.WithLabelValues(values...).Add(usd)
}

// TouchSeries publishes one claim's label set as a zero-valued series under each of phases, so the
// first charge booked there has an earlier sample to be differenced against.
//
// increase() recovers a RISE between two samples, so a series whose very first sample already holds
// money reads as no rise at all: those dollars are in the counter's absolute value but not in any
// increase()/rate() query, which is what a billing consumer runs. Sharing series across claims
// usually hides that — but attribution makes them tenant-scoped, and a tenant whose whole usage is
// one short job would be billed nothing.
//
// This does not contradict RecordWindow's refusal of zeros. A zero WINDOW is a measurement claiming
// something cost nothing; a zero COUNTER only says nothing has been charged here yet, and publishing
// it is the ordinary way to make rate() work over label values not known until runtime.
//
// Series are PROCESS-local, so the baseline has to be republished by whatever process is doing the
// charging — see controller.CostAccrual.seedBaselines, which is the one caller. A scrape still has to
// land between the baseline and the charge, so a claim that becomes chargeable after the seeding pass
// is not covered at all; see docs/metrics.md.
func TouchSeries(l Labels, attribution map[string]string, phases ...string) {
	for _, phase := range phases {
		values := l.values(append([]string{phase}, attributionValues(attribution)...)...)
		noteSeries(values)
		CostTotal.WithLabelValues(values...).Add(0)
	}
}
