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
	"context"
	"math"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/util"
)

// accrualInterval is how often spend is checkpointed. It is a WRITE cadence, not an accounting
// resolution: every window is measured from the persisted anchor, so a longer interval trades
// freshness of the EST_COST column for fewer writes, never accuracy.
//
// What it does bound is the gap a baseline has to survive — a scrape must land between a series'
// zero sample and its first charge, one tick later, or those dollars reach no increase() query at
// all (see seedClaimBaseline). Thirty seconds keeps that reachable for the usual 15s scrape, at
// ~16.7 writes/s across a 500-claim fleet: a third of the client's rate budget (see the QPS in
// cmd/main.go), which is where this stops being free and starts competing with the reconcilers.
const accrualInterval = 30 * time.Second

// accrualTimeout bounds one whole tick, List plus every write. A tick that cannot finish loses
// nothing: the anchors it did not reach are still where they were, so the next tick charges the
// same windows.
//
// Equal to accrualInterval, which is safe rather than tidy: ticks run sequentially and a
// time.Ticker drops the ticks a slow receiver missed instead of queueing them, so the worst case is
// back-to-back ticks, and re-deriving a window from its anchor cannot double-charge it.
const accrualTimeout = accrualInterval

// CostAccrual advances each claim's durable spend ledger on a ticker.
//
// A Runnable rather than a hook on the reconcile path: spend accrues with the CLOCK, not with
// events, and a Bound claim can sit for hours without a reconcile. Leader election is the
// manager's default for a plain Runnable and is load-bearing here — two replicas accruing the
// same fleet would double every dollar.
//
// It lives beside the reconciler rather than in pkg/metrics because it WRITES: the ledger is a
// status field, and instrumentation that patches API objects is no longer instrumentation.
type CostAccrual struct {
	client.Client
	interval time.Duration
	// now is overridable so tests can drive whole windows without sleeping.
	now func() time.Time
}

var _ manager.Runnable = (*CostAccrual)(nil)

// NewCostAccrual builds the accrual loop over the manager's client: cached reads, direct writes.
func NewCostAccrual(c client.Client) *CostAccrual {
	return &CostAccrual{Client: c, interval: accrualInterval, now: time.Now}
}

// Start accrues until ctx is cancelled. Unlike a loop that charges the time between its own
// ticks, this one is anchored per claim in status, so a tick that is late, skipped, or the first
// after a restart all charge exactly the time that really passed. The window still open at
// shutdown is not lost either — the next process charges it.
func (a *CostAccrual) Start(ctx context.Context) error {
	// An idle fleet writes nothing and emits no series at all, which is indistinguishable from a
	// loop that never started. This line is the only evidence either way.
	log := logf.Log.WithName("cost-accrual")
	log.Info("starting cost accrual", "interval", a.interval)

	ticker := time.NewTicker(a.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			a.accrueAll(ctx)
		}
	}
}

// accrueAll checkpoints every billing claim once. Errors are logged per claim and never
// propagated: one unwritable claim must not cost the rest of the fleet its window.
func (a *CostAccrual) accrueAll(ctx context.Context) {
	log := logf.Log.WithName("cost-accrual")

	ctx, cancel := context.WithTimeout(ctx, accrualTimeout)
	defer cancel()

	var claims nebulav1alpha1.NodeClaimList
	if err := a.List(ctx, &claims); err != nil {
		log.Error(err, "listing nodeclaims to accrue")
		return
	}
	for i := range claims.Items {
		nc := &claims.Items[i]
		if err := a.accrue(ctx, nc); err != nil {
			// A conflict is the ordinary case — the reconciler patched the same claim from its
			// own copy. The anchor did not move, so this window is simply charged next tick.
			if apierrors.IsConflict(err) || apierrors.IsNotFound(err) {
				log.V(1).Info("skipping accrual this tick", "claim", nc.Name, "reason", err.Error())
				continue
			}
			log.Error(err, "accruing claim cost", "claim", nc.Name)
		}
	}
}

// accrue persists what the claim has cost as of now and re-anchors there, in one patch.
//
// It decides WHETHER to write; costNow decides what the number is. The pair of fields being one
// patch is what makes it crash-safe: the anchor can never
// advance past money that was recorded, so an interrupted tick re-charges the same window rather
// than skipping it.
//
// Charging is also idempotent per unit time — the amount follows from the anchor, not from how
// many ticks fire — and Update carries a resourceVersion, so a writer working from a stale ledger
// is rejected outright. Together that makes a second, unelected accruer wasteful, not harmful.
func (a *CostAccrual) accrue(ctx context.Context, nc *nebulav1alpha1.NodeClaim) error {
	if _, ok := billingRate(nc); !ok {
		// Not billing. Note this is stricter than costNow, which still reports a Terminated
		// claim's frozen total: nothing more can be charged, so writing it again is pure churn.
		return nil
	}
	// Second precision, because that is all metav1.Time serializes. Reading the anchor back
	// truncated while having charged from the untruncated instant would re-charge the dropped
	// fraction every tick — a small but systematic OVER-count, the one direction this must
	// never drift.
	now := a.now().Truncate(time.Second)

	// An anchor in the future — a wall-clock jump, or a hand-edited field. Charging the negative
	// window would rewind the ledger. Left in place rather than pulled back to now, so billing
	// resumes by itself once the clock passes it, having lost only the bogus window.
	if at := nc.Status.LastAccruedAt; at != nil && !now.After(at.Time) {
		return nil
	}
	// A claim with no anchor at all — one that became billable before this build, or whose Bound
	// patch did not carry the stamp — falls through with nothing added: the window before this
	// point has an unknown length, and guessing it would invent spend. Opening one here is the
	// whole write.
	prev := costSoFar(nc)
	total, _ := costNow(nc, now)
	nc.Status.EstimatedCostUSD = formatCost(total)
	nc.Status.LastAccruedAt = &metav1.Time{Time: now}
	if err := a.Status().Update(ctx, nc); err != nil {
		return err
	}
	// Booked only now that the window is durable — see metrics.RecordWindow. Re-reading the
	// field instead of using total charges the counter the same ROUNDED dollars the ledger took,
	// so the two reconcile exactly rather than drifting a fraction of a cent per tick.
	metrics.RecordWindow(claimLabels(nc), string(nc.Status.Phase), nc.Status.CostLabels, costSoFar(nc)-prev)
	return nil
}

// billingRate returns the claim's hourly rate and whether it should be charged at all.
//
// Two gates, and both are omissions rather than zeros — a zero would be summed and averaged as
// a real "this costs nothing", while absent spend is honestly unknown:
//
//   - Phase. Only Bound and Terminating hold an instance that exists (see NodeClaimPhase).
//     Terminating still bills until teardown finishes. Provisioning is excluded and undercounts
//     by roughly one poll tick, the same lag the phase itself carries.
//   - Price. Empty means UNPRICED, not free (see Status.PriceUSDPerHour): no Pricer, or no
//     catalog row. An unparseable value is a corrupted claim and is treated the same.
func billingRate(nc *nebulav1alpha1.NodeClaim) (float64, bool) {
	switch nc.Status.Phase {
	case nebulav1alpha1.NodeClaimBound, nebulav1alpha1.NodeClaimTerminating:
	default:
		return 0, false
	}
	return parsePrice(nc)
}

// billingPhases is every phase a window can be booked under: the two billingRate admits, plus
// Terminated for the last window settleFinalCost closes. seedClaimBaseline publishes all three up
// front rather than guessing: one always stays empty, since a reclaimed instance passes through
// Terminating and a self-terminated one does not, and which of the two it will be is not knowable
// in advance.
var billingPhases = []string{
	string(nebulav1alpha1.NodeClaimBound),
	string(nebulav1alpha1.NodeClaimTerminating),
	string(nebulav1alpha1.NodeClaimTerminated),
}

// parsePrice is billingRate without the phase gate: whether an instance of this shape costs money,
// not whether this claim is accruing right now. Nothing may use it to decide that a claim ACCRUES.
//
// Both callers want the ungated form because of Terminated — the phase billingRate refuses, since a
// claim sits there until its Pod is deleted and charging those idle hours would be wrong.
// settleFinalCost books the one window that is real, between the last checkpoint and the instance's
// death; seedClaimBaseline only asks whether a window could ever be booked at all.
func parsePrice(nc *nebulav1alpha1.NodeClaim) (float64, bool) {
	rate, err := strconv.ParseFloat(nc.Status.PriceUSDPerHour, 64)
	// ParseFloat accepts "NaN" and "Inf" without an error, and both slip past rate <= 0. Left
	// unchecked, one such value reaches the ledger, and from there it is unrecoverable: the total
	// is re-read every tick, so NaN + x stays NaN even after the price is fixed.
	if err != nil || math.IsNaN(rate) || math.IsInf(rate, 0) || rate <= 0 {
		return 0, false
	}
	return rate, true
}

// costSoFar reads the running total. An unparseable value is treated as zero rather than as a
// reason to stop accruing: losing the history of one corrupted claim beats freezing its ledger
// and under-reporting the fleet forever.
func costSoFar(nc *nebulav1alpha1.NodeClaim) float64 {
	total, err := strconv.ParseFloat(nc.Status.EstimatedCostUSD, 64)
	// Non-finite is what makes the zero here load-bearing rather than tidy: a ledger already
	// holding "NaN" (see parsePrice) would otherwise stay poisoned for the claim's whole life,
	// since every later total is derived from this read.
	if err != nil || math.IsNaN(total) || math.IsInf(total, 0) || total < 0 {
		return 0
	}
	return total
}

// costDecimals is how many fractional digits the LEDGER keeps — more than priceDecimals, because a
// rate is an input that is written once while a total is an accumulator that is re-read and
// re-written every window. Rounding it at each checkpoint would quantize the effective rate onto the
// grid: the residue is discarded rather than carried, so a claim cheaper than half a grid step per
// window freezes forever (at four digits and a 30s tick, anything under $0.006/hr — a Modal CPU-only
// sandbox), and one just above it is charged the rounded-UP step every tick. A shorter interval only
// sharpens that, since it shrinks the charge and not the step. Eight digits holds the drift under
// 0.03% at the cheapest rate a catalog carries and far under it everywhere else, at the cost of a
// wider EST_COST column.
const costDecimals = 8

func formatCost(usd float64) string {
	return strconv.FormatFloat(usd, 'f', costDecimals, 64)
}

// stampAccrualStart opens the billing window the moment a claim first becomes chargeable,
// returning whether it mutated the claim.
//
// Load-bearing rather than an optimisation: it is what makes a crash BEFORE the first
// checkpoint lossless. The anchor is what recovery measures from, so without it a claim that
// billed for 90 seconds and then lost its manager would be charged from whenever the next tick
// happened to find it. Free, because it rides the status patch markPhase is already making.
func stampAccrualStart(nc *nebulav1alpha1.NodeClaim) bool {
	if nc.Status.LastAccruedAt != nil {
		return false
	}
	if _, ok := billingRate(nc); !ok {
		return false
	}
	nc.Status.LastAccruedAt = &metav1.Time{Time: time.Now().Truncate(time.Second)}
	return true
}

// seedClaimBaseline publishes a zero-valued series for each phase this claim could ever book a
// window under, so the first charge has an earlier sample to be differenced against — without one,
// those dollars are in the counter but in no increase() query, which is what a billing consumer
// runs (see metrics.TouchSeries).
//
// LEVEL-TRIGGERED on purpose, which is why one call site covers two problems that look separate.
// A claim born mid-process is seeded when it first becomes chargeable; and because counter series
// are process-local, a restart or leader handoff re-seeds the whole fleet off the initial sync,
// well before the accrual loop's first tick an interval later. A once-at-startup pass covers only
// the second, and only for claims that existed by then.
//
// parsePrice rather than billingRate: a Terminated claim still books a window when its Pod is
// finally deleted (see settleFinalCost), so it needs a baseline as much as a live one. Both gates
// are omissions — no anchor or no price means no window can ever be booked here, and a baseline
// for one would be an empty promise that only costs cardinality.
//
// Worth nothing unless the scrape interval is shorter than accrualInterval: the gap this opens is
// one tick, and a scrape has to land inside it. See docs/metrics.md, Known gaps.
func seedClaimBaseline(nc *nebulav1alpha1.NodeClaim) {
	if _, ok := parsePrice(nc); !ok || nc.Status.LastAccruedAt == nil {
		return
	}
	metrics.TouchSeries(claimLabels(nc), nc.Status.CostLabels, billingPhases...)
}

// settleFinalCost books the window still open at teardown and logs what the instance cost over its
// whole life.
//
// Booking is safe here despite there being no anchor left to move, because the caller has already
// had its finalizer removal ACCEPTED: that Update is a compare-and-swap, so it lands at most once,
// and only for a copy of the claim whose ledger is current — a stale anchor would have conflicted
// there rather than re-charging time the loop already took. Skipping it would lose far more than
// one window: an instance that died before its first accrual tick would be charged NOTHING at all.
//
// The lifetime figure itself only goes to the log; status is deleted with the object.
func settleFinalCost(ctx context.Context, nc *nebulav1alpha1.NodeClaim) {
	var final float64
	if rate, ok := parsePrice(nc); ok && nc.Status.LastAccruedAt != nil {
		open := time.Since(nc.Status.LastAccruedAt.Time)
		// Clamped in Terminated alone: its anchor froze when the instance died and the claim may
		// have sat for days since. A Terminating claim is still running, so its whole open window is
		// real money — including hours recovered after a manager outage.
		if nc.Status.Phase == nebulav1alpha1.NodeClaimTerminated && open > accrualInterval {
			open = accrualInterval
		}
		if open > 0 {
			final = rate * open.Hours()
		}
	}
	// Zero for a claim that was never priced or never anchored, which RecordWindow drops rather
	// than minting a series for.
	metrics.RecordWindow(claimLabels(nc), string(nc.Status.Phase), nc.Status.CostLabels, final)
	logf.FromContext(ctx).Info("claim finalized", "phase", nc.Status.Phase,
		"totalTimeCostUSD", formatCost(costSoFar(nc)+final))
}

// costNow is what a claim has cost as of now: the persisted ledger plus the window still open
// since its anchor. False means the claim has no cost to report AT ALL — neither billable nor
// ever charged, which is absent spend rather than zero spend (see billingRate).
func costNow(nc *nebulav1alpha1.NodeClaim, now time.Time) (float64, bool) {
	rate, billing := billingRate(nc)
	total := costSoFar(nc)
	if !billing && total == 0 {
		return 0, false
	}
	if billing && nc.Status.LastAccruedAt != nil {
		if open := now.Sub(nc.Status.LastAccruedAt.Time); open > 0 {
			total += rate * open.Hours()
		}
	}
	return total, true
}

// claimLabels renders the candidate a claim was provisioned against, so its cost carries the same
// label values as the placement and provisioning attempts that produced it and the three join
// without label surgery.
func claimLabels(nc *nebulav1alpha1.NodeClaim) metrics.Labels {
	accelerator, count := util.SplitAcceleratorPool(nc.Spec.Accelerator)
	return metrics.Labels{
		Provider:         nc.Spec.Provider,
		Region:           nc.Spec.Region,
		CapacityType:     string(nc.Spec.CapacityType),
		Accelerator:      accelerator,
		AcceleratorCount: count,
	}
}

// stampCostLabels copies the configured attribution labels off the served Pod, returning whether
// it mutated the claim.
//
// Written once and never refreshed, for the same reason the price is: spend already reported
// under one tenant must not be re-attributed to another by an edit to a Pod label. It runs
// alongside stampAccrualStart in one patch, so attribution is durable before the first window it
// has to explain.
func stampCostLabels(nc *nebulav1alpha1.NodeClaim, pod *corev1.Pod) bool {
	keys := metrics.CostLabelKeys()
	if nc.Status.CostLabels != nil || pod == nil || len(keys) == 0 {
		return false
	}
	stamped := map[string]string{}
	for _, key := range keys {
		if v := pod.Labels[key]; v != "" {
			stamped[key] = v
		}
	}
	// An empty map, not nil: a Pod carrying none of the configured labels is a settled fact, and
	// nil would make every later reconcile look it up again. Durable only because the field has no
	// omitempty — see NodeClaimStatus.CostLabels.
	nc.Status.CostLabels = stamped
	return true
}
