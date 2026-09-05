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
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// billingClaim is a claim holding a priced instance, anchored ago in the past. A nil ago leaves
// the anchor unset, i.e. never checkpointed.
func billingClaim(name, price string, ago *time.Duration) *nebulav1alpha1.NodeClaim {
	nc := &nebulav1alpha1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       nebulav1alpha1.NodeClaimSpec{Provider: "modal", Accelerator: "H100:1"},
		Status: nebulav1alpha1.NodeClaimStatus{
			Phase:           nebulav1alpha1.NodeClaimBound,
			PriceUSDPerHour: price,
		},
	}
	if ago != nil {
		nc.Status.LastAccruedAt = &metav1.Time{Time: time.Now().Add(-*ago).Truncate(time.Second)}
	}
	return nc
}

// newAccrual wires the loop over a fake client seeded with claims, with the clock pinned so a
// whole window can be charged without sleeping.
func newAccrual(t *testing.T, claims ...*nebulav1alpha1.NodeClaim) (*CostAccrual, client.Client) {
	t.Helper()
	metrics.CostTotal.Reset()

	objs := make([]client.Object, 0, len(claims))
	for _, nc := range claims {
		objs = append(objs, nc)
	}
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(objs...).
		WithStatusSubresource(&nebulav1alpha1.NodeClaim{}).
		Build()
	return NewCostAccrual(c), c
}

// ledger reads back the persisted total and anchor.
func ledger(t *testing.T, c client.Client, name string) (float64, *metav1.Time) {
	t.Helper()
	var nc nebulav1alpha1.NodeClaim
	if err := c.Get(context.Background(), client.ObjectKey{Name: name}, &nc); err != nil {
		t.Fatalf("get claim %q: %v", name, err)
	}
	if nc.Status.EstimatedCostUSD == "" {
		return 0, nc.Status.LastAccruedAt
	}
	total, err := strconv.ParseFloat(nc.Status.EstimatedCostUSD, 64)
	if err != nil {
		t.Fatalf("status.estimatedCostUSD %q is not a number: %v", nc.Status.EstimatedCostUSD, err)
	}
	return total, nc.Status.LastAccruedAt
}

// booked is the dollars the cost counter holds across every series.
func booked(t *testing.T) float64 {
	t.Helper()
	if testutil.CollectAndCount(metrics.CostTotal) == 0 {
		return 0
	}
	return testutil.ToFloat64(metrics.CostTotal)
}

func TestCostAccrual_ChargesFromTheAnchor(t *testing.T) {
	halfHour := 30 * time.Minute
	a, c := newAccrual(t, billingClaim("bound", "98.3200", &halfHour))

	a.accrueAll(context.Background())

	want := 98.32 * 0.5
	total, anchor := ledger(t, c, "bound")
	if math.Abs(total-want) > 1e-3 {
		t.Fatalf("status.estimatedCostUSD %v, want %v", total, want)
	}
	// The same window reaches the counter, so a consumer's increase() and the field agree.
	if got := booked(t); math.Abs(got-total) > 1e-9 {
		t.Fatalf("booked %v, want %v — the counter must take exactly what the ledger did", got, total)
	}
	if time.Since(anchor.Time) > time.Minute {
		t.Fatalf("anchor was not moved forward: %v", anchor)
	}
}

// The whole point of persisting a timestamp: a window that spans a restart is charged in full on
// the first tick back, not clipped to one interval.
func TestCostAccrual_RecoversDowntime(t *testing.T) {
	down := 3 * time.Hour
	a, c := newAccrual(t, billingClaim("bound", "10.0000", &down))

	a.accrueAll(context.Background())

	if total, _ := ledger(t, c, "bound"); math.Abs(total-30) > 1e-2 {
		t.Fatalf("status.estimatedCostUSD %v after a 3h gap, want 30 — the downtime was not recovered", total)
	}
}

// Successive checkpoints accumulate on the field rather than replacing it.
func TestCostAccrual_AccumulatesAcrossTicks(t *testing.T) {
	hour := time.Hour
	a, c := newAccrual(t, billingClaim("bound", "4.0000", &hour))

	// First tick charges the hour; the second, driven by a clock pushed 30 minutes on, charges
	// the half hour since.
	a.accrueAll(context.Background())
	a.now = func() time.Time { return time.Now().Add(30 * time.Minute) }
	a.accrueAll(context.Background())

	if total, _ := ledger(t, c, "bound"); math.Abs(total-6) > 1e-2 {
		t.Fatalf("status.estimatedCostUSD %v, want 6 (1h + 0.5h at $4/hr)", total)
	}
}

// Every checkpoint re-reads the ledger to measure the next window, so the digits it keeps ARE the
// accounting resolution — see costDecimals. Driven at the real cadence, a cheap claim is where that
// shows: at four decimals and the one-minute cadence this ran at then, $0.0028/hr froze at $0.0001
// and never moved again, while $0.01/hr was charged a rounded-up step every tick and ran 20% over.
func TestCostAccrual_ChargesCheapClaimsAtTheirRealRate(t *testing.T) {
	// Modal's CPU-only components: a 20-millicore reservation, a 64MiB one, and one whole core.
	for _, rate := range []string{"0.0028", "0.0100", "0.1419"} {
		t.Run(rate, func(t *testing.T) {
			none := time.Duration(0)
			nc := billingClaim("cheap", rate, &none)
			base := nc.Status.LastAccruedAt.Time
			a, c := newAccrual(t, nc)

			const ticks = 480 // four hours at the 30s write cadence
			for i := 1; i <= ticks; i++ {
				at := base.Add(time.Duration(i) * accrualInterval)
				a.now = func() time.Time { return at }
				a.accrueAll(context.Background())
			}

			hourly, err := strconv.ParseFloat(rate, 64)
			if err != nil {
				t.Fatalf("bad rate %q: %v", rate, err)
			}
			want := hourly * float64(ticks) * accrualInterval.Hours()
			total, _ := ledger(t, c, "cheap")
			// Half a rounding step is discarded per tick and never carried, so the bar is that
			// residue against the SMALLEST per-window charge a catalog produces — $0.0028/hr over
			// 30s, i.e. ~2e-4 relative. Anything worse means costDecimals is too coarse for the
			// interval, which is the failure this test exists to catch.
			if math.Abs(total-want)/want > 3e-4 {
				t.Fatalf("status.estimatedCostUSD %v after %d ticks at $%s/hr, want %v", total, ticks, rate, want)
			}
			if got := booked(t); math.Abs(got-total) > 1e-9 {
				t.Fatalf("booked %v, want %v — the counter must take exactly what the ledger did", got, total)
			}
		})
	}
}

// A claim that became billable before the ledger existed has no anchor. Opening one must not
// invent spend for the window whose length nobody knows.
func TestCostAccrual_StampsMissingAnchorWithoutCharging(t *testing.T) {
	a, c := newAccrual(t, billingClaim("bound", "98.3200", nil))

	a.accrueAll(context.Background())

	total, anchor := ledger(t, c, "bound")
	if total != 0 {
		t.Fatalf("status.estimatedCostUSD %v on an unanchored claim, want 0", total)
	}
	if anchor == nil {
		t.Fatal("no anchor was written, so the next tick will charge nothing either")
	}
	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("collected %d series, want 0 — nothing was charged", n)
	}
}

// A claim holding no instance, or one nobody can price, accrues NOTHING rather than zero: a zero
// is summed and averaged as a real "this costs nothing".
func TestCostAccrual_SkipsNonBilling(t *testing.T) {
	hour := time.Hour
	unbilled := func(name, price string, phase nebulav1alpha1.NodeClaimPhase) *nebulav1alpha1.NodeClaim {
		nc := billingClaim(name, price, &hour)
		nc.Status.Phase = phase
		return nc
	}
	claims := []*nebulav1alpha1.NodeClaim{
		unbilled("provisioning", "3.9500", nebulav1alpha1.NodeClaimProvisioning),
		// The instance is gone; the claim lingers only as a ledger.
		unbilled("terminated", "3.9500", nebulav1alpha1.NodeClaimTerminated),
		unbilled("no-phase", "3.9500", ""),
		// Bound but UNPRICED (no Pricer, or no catalog row) — not free.
		unbilled("unpriced", "", nebulav1alpha1.NodeClaimBound),
		unbilled("malformed", "cheap", nebulav1alpha1.NodeClaimBound),
		unbilled("nonpositive", "0.0000", nebulav1alpha1.NodeClaimBound),
		// ParseFloat reads these as valid floats and they outrank every ordering check, so
		// without an explicit test the guard against them is one refactor from vanishing.
		unbilled("nan", "NaN", nebulav1alpha1.NodeClaimBound),
		unbilled("inf", "+Inf", nebulav1alpha1.NodeClaimBound),
	}
	a, c := newAccrual(t, claims...)

	a.accrueAll(context.Background())

	for _, nc := range claims {
		if total, _ := ledger(t, c, nc.Name); total != 0 {
			t.Fatalf("claim %q accrued %v, want 0", nc.Name, total)
		}
	}
	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("collected %d series, want 0 — no non-billing claim may accrue", n)
	}
}

// A ledger that already holds a non-finite total recovers on the next tick. Without this the claim
// would stay poisoned for the rest of its life even after the bad price was fixed, because every
// total is derived from reading the previous one back.
func TestCostAccrual_RecoversFromPoisonedLedger(t *testing.T) {
	halfHour := 30 * time.Minute
	nc := billingClaim("poisoned", "10.0000", &halfHour)
	nc.Status.EstimatedCostUSD = "NaN"
	a, c := newAccrual(t, nc)

	a.accrueAll(context.Background())

	want := 10 * 0.5
	total, _ := ledger(t, c, "poisoned")
	// Tested for finiteness FIRST: every comparison against NaN is false, so an ordinary
	// tolerance check would pass on the very value this test exists to catch.
	if math.IsNaN(total) || math.IsInf(total, 0) || math.Abs(total-want) > 1e-3 {
		t.Fatalf("status.estimatedCostUSD %v, want %v — the poisoned total must be dropped, not carried", total, want)
	}
	if got := booked(t); math.IsNaN(got) || math.Abs(got-total) > 1e-9 {
		t.Fatalf("booked %v, want %v", got, total)
	}
}

// An anchor in the future (a wall-clock jump, a hand-edited field) must not rewind the ledger.
func TestCostAccrual_DropsBackwardsWindow(t *testing.T) {
	ahead := -time.Hour
	a, c := newAccrual(t, billingClaim("bound", "4.0000", &ahead))

	a.accrueAll(context.Background())

	if total, _ := ledger(t, c, "bound"); total != 0 {
		t.Fatalf("status.estimatedCostUSD %v on a backwards window, want 0", total)
	}
	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("collected %d series on a backwards window, want 0", n)
	}
}

// A write that fails must leave the anchor where it was, so the next tick charges the same window
// once — not twice, and not never.
func TestCostAccrual_FailedWriteChargesNothing(t *testing.T) {
	hour := time.Hour
	nc := billingClaim("bound", "98.3200", &hour)
	c := fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithObjects(nc).
		WithStatusSubresource(&nebulav1alpha1.NodeClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(context.Context, client.Client, string, client.Object, ...client.SubResourceUpdateOption) error {
				return apierrors.NewConflict(schema.GroupResource{Resource: "nodeclaims"}, "bound", errors.New("stale"))
			},
		}).
		Build()
	a := NewCostAccrual(c)

	a.accrueAll(context.Background())

	if total, anchor := ledger(t, c, "bound"); total != 0 || anchor.Time.Before(time.Now().Add(-90*time.Minute)) {
		t.Fatalf("a failed write moved the ledger: estimatedCostUSD=%v anchor=%v", total, anchor)
	}
	// The ordering that keeps a retry from over-charging: a counter has no idempotency key, so a
	// window booked before its patch landed would be charged again next tick.
	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("collected %d series after a failed write, want 0", n)
	}
}

// costNow is what both the checkpoint and the teardown log line read, so the window still open
// since the anchor has to be in it.
func TestCostNow(t *testing.T) {
	hour := time.Hour
	anchored := billingClaim("bound", "10.0000", &hour)
	anchored.Status.EstimatedCostUSD = "5.0000"

	// Terminated: the instance is gone, so the persisted total is final and no window is added.
	settled := billingClaim("settled", "10.0000", &hour)
	settled.Status.Phase = nebulav1alpha1.NodeClaimTerminated
	settled.Status.EstimatedCostUSD = "5.0000"

	cases := map[string]struct {
		claim  *nebulav1alpha1.NodeClaim
		want   float64
		report bool
	}{
		"ledger plus the open window": {claim: anchored, want: 15, report: true},
		"terminated total is frozen":  {claim: settled, want: 5, report: true},
		"unanchored charges nothing":  {claim: billingClaim("fresh", "10.0000", nil), want: 0, report: true},
		// Never billable and never charged: absent cost, which must not be reported as zero.
		"unpriced is absent, not zero": {claim: billingClaim("unpriced", "", &hour), want: 0, report: false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, ok := costNow(tc.claim, time.Now())
			if ok != tc.report {
				t.Fatalf("costNow ok = %v, want %v", ok, tc.report)
			}
			if math.Abs(got-tc.want) > 1e-2 {
				t.Fatalf("costNow = %v, want %v", got, tc.want)
			}
		})
	}
}

// status is deliberately untouched on the way out: writing it would race the deletion.
func TestSettleFinalCost_LeavesStatusAlone(t *testing.T) {
	hour := time.Hour
	nc := billingClaim("bound", "10.0000", &hour)
	nc.Status.EstimatedCostUSD = "5.0000"

	settleFinalCost(context.Background(), nc)

	if nc.Status.EstimatedCostUSD != "5.0000" {
		t.Fatalf("settleFinalCost wrote status.estimatedCostUSD %q; it must not touch a deleting claim",
			nc.Status.EstimatedCostUSD)
	}
}

// The window open at teardown reaches the counter even though no checkpoint can close it — without
// this, an instance that never survived a tick would be billed nothing at all.
func TestSettleFinalCost_BooksTheOpenWindow(t *testing.T) {
	metrics.CostTotal.Reset()

	hour := time.Hour
	// Never checkpointed: the whole of its life is the open window.
	nc := billingClaim("bound", "10.0000", &hour)
	nc.Status.Phase = nebulav1alpha1.NodeClaimTerminating

	settleFinalCost(context.Background(), nc)

	if got := booked(t); math.Abs(got-10) > 1e-2 {
		t.Fatalf("booked %v at teardown, want 10 (1h at $10/hr charged by no tick)", got)
	}
}

// An instance that ended on its own reaches Terminated, which the accrual loop refuses to touch, so
// the time since its last checkpoint is booked here or nowhere. The clamp is what keeps that safe:
// the claim then sits in Terminated until its Pod is deleted, and charging that wait would bill idle
// hours at GPU rates.
func TestSettleFinalCost_BooksTheLastWindowOfADeadInstance(t *testing.T) {
	// Half a window, so the first case measures the real gap rather than tripping the clamp.
	halfWindow, day := accrualInterval/2, 24*time.Hour
	cases := map[string]struct {
		ago  time.Duration
		want float64
	}{
		"the window since the last checkpoint": {ago: halfWindow, want: 10 * halfWindow.Hours()},
		"never longer than one interval":       {ago: day, want: 10 * accrualInterval.Hours()},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			metrics.CostTotal.Reset()

			nc := billingClaim("settled", "10.0000", &tc.ago)
			nc.Status.Phase = nebulav1alpha1.NodeClaimTerminated
			nc.Status.EstimatedCostUSD = "5.0000"

			settleFinalCost(context.Background(), nc)

			// billingClaim truncates the anchor to the second, so the real window is up to a
			// second longer than ago. Two seconds of spend covers that and the test's own time.
			if got := booked(t); math.Abs(got-tc.want) > 10*(2*time.Second).Hours() {
				t.Fatalf("booked %v, want %v", got, tc.want)
			}
		})
	}
}

// Unpriced is absent spend, not zero — a claim that was never chargeable must not mint a series on
// its way out either.
func TestSettleFinalCost_BooksNothingUnpriced(t *testing.T) {
	metrics.CostTotal.Reset()

	hour := time.Hour
	nc := billingClaim("unpriced", "", &hour)
	nc.Status.Phase = nebulav1alpha1.NodeClaimTerminated

	settleFinalCost(context.Background(), nc)

	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("collected %d series for a claim that was never priced, want 0", n)
	}
}

// The window is booked against the candidate the claim was provisioned on, so cost joins the
// placement and provisioning series without label surgery.
func TestCostAccrual_BooksAgainstTheCandidate(t *testing.T) {
	hour := time.Hour
	nc := billingClaim("bound", "10.0000", &hour)
	nc.Spec.Region = "us-east-1"
	nc.Spec.CapacityType = nebulav1alpha1.CapacitySpot
	a, _ := newAccrual(t, nc)

	a.accrueAll(context.Background())

	// The exposition format is line-oriented, so this cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="Spot",phase="Bound",provider="modal",region="us-east-1"} 10
`
	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// Attribution is copied off the Pod once and then frozen: spend already reported under one tenant
// must not move to another because somebody relabelled a Pod.
//
// Configured with QUALIFIED keys, which is what a real deployment uses: the stamp must be keyed by
// the key the Pod carries, prefix and all, not by the name the metric emits under.
func TestStampCostLabels(t *testing.T) {
	t.Cleanup(func() { metrics.ConfigureCostForTest(nil) })
	metrics.ConfigureCostForTest([]string{"example.com/org-id", "example.com/team-id"})

	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{
		"example.com/org-id":  "acme",
		"example.com/team-id": "ml",
		// The derived metric name is not a Pod key, so it must not be read as one.
		"org_id":       "ignored",
		"unconfigured": "ignored",
	}}}

	nc := billingClaim("bound", "10.0000", nil)
	if !stampCostLabels(nc, pod) {
		t.Fatal("stampCostLabels reported no change on an unstamped claim")
	}
	want := map[string]string{"example.com/org-id": "acme", "example.com/team-id": "ml"}
	if !reflect.DeepEqual(nc.Status.CostLabels, want) {
		t.Fatalf("stamped %v, want %v — only configured keys are copied", nc.Status.CostLabels, want)
	}

	// A relabelled Pod must not re-attribute the claim.
	pod.Labels["example.com/org-id"] = "someone-else"
	if stampCostLabels(nc, pod) {
		t.Fatal("stampCostLabels rewrote attribution that was already settled")
	}
	if nc.Status.CostLabels["example.com/org-id"] != "acme" {
		t.Fatalf("org-id moved to %q", nc.Status.CostLabels["example.com/org-id"])
	}

	// A Pod carrying none of them is a settled fact too, or every later reconcile re-checks it.
	bare := billingClaim("bare", "10.0000", nil)
	if !stampCostLabels(bare, &corev1.Pod{}) || bare.Status.CostLabels == nil {
		t.Fatal("a Pod with no configured labels must still stamp an empty map")
	}
}

// With no --cost-labels there is nothing to copy, and the claim must stay untouched rather than
// carrying an empty map that reads as "we looked and found nothing".
func TestStampCostLabels_NotConfigured(t *testing.T) {
	nc := billingClaim("bound", "10.0000", nil)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"org_id": "acme"}}}

	if stampCostLabels(nc, pod) || nc.Status.CostLabels != nil {
		t.Fatalf("stamped %v with no cost labels configured, want nil", nc.Status.CostLabels)
	}
}

// The window booked carries the claim's stamped attribution, which is the whole point of pinning
// it in status: the Pod is long gone by the time a Terminating window is charged.
//
// End to end on the key/name split: the claim is keyed "example.com/org-id" and the series comes out
// as org_id.
func TestCostAccrual_BooksAttribution(t *testing.T) {
	t.Cleanup(func() { metrics.ConfigureCostForTest(nil) })
	metrics.ConfigureCostForTest([]string{"example.com/org-id"})

	hour := time.Hour
	nc := billingClaim("bound", "10.0000", &hour)
	nc.Status.CostLabels = map[string]string{"example.com/org-id": "acme"}
	a, _ := newAccrual(t, nc)

	a.accrueAll(context.Background())

	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",example_com_org_id="acme",phase="Bound",provider="modal",region="none"} 10
`
	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// The whole chain --cost-labels exists for, through a real reconcile: a label on the served Pod
// reaches the claim's status, and the window charged later comes out under it. Nothing is stamped
// by hand here, so the two halves are only connected if markPhase picks the attribution up and
// opens the accrual anchor in the SAME write it records the phase and price with.
func TestMarkPhase_StampsAttributionThatIsThenBooked(t *testing.T) {
	t.Cleanup(func() { metrics.ConfigureCostForTest(nil) })
	metrics.ConfigureCostForTest([]string{"example.com/org-id"})
	metrics.CostTotal.Reset()

	pod := gpuPod("H100", 1, "1", "1Gi")
	pod.Labels["example.com/org-id"] = "acme"
	withInstanceID(pod, "sb-1")

	nc := newClaim("c1", "p1", "default", "uid-1", "fake")
	nc.Spec.Accelerator = "H100:1"
	nc.Spec.Region = "us-east-1"
	nc.Spec.CapacityType = nebulav1alpha1.CapacityOnDemand

	// Status writes are counted, not merely observed: a window charged before the attribution
	// explaining it is durable is spend nobody can bill, so the four fields must land together.
	writes := 0
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pod, nc).
		WithStatusSubresource(&nebulav1alpha1.NodeClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				writes++
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()
	pp := &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 10}
	r := &NodeClaimReconciler{Client: c, Scheme: s, Providers: func(name string) (provider.Provider, bool) {
		return pp, name == pp.name
	}}

	reconcileClaim(t, r, "c1")

	if writes != 1 {
		t.Fatalf("%d status writes, want 1 — the attribution and the anchor must ride one patch", writes)
	}
	got := getClaim(t, c, "c1")
	if got.Status.Phase != nebulav1alpha1.NodeClaimBound || got.Status.PriceUSDPerHour != "10.0000" {
		t.Fatalf("phase %q price %q, want Bound / 10.0000", got.Status.Phase, got.Status.PriceUSDPerHour)
	}
	want := map[string]string{"example.com/org-id": "acme"}
	if !reflect.DeepEqual(got.Status.CostLabels, want) {
		t.Fatalf("status.costLabels = %v, want %v", got.Status.CostLabels, want)
	}
	// The anchor opens only once the claim is Bound AND priced, so its presence is also the
	// evidence that stampAccrualStart ran after the two calls it reads — not before them.
	if got.Status.LastAccruedAt == nil {
		t.Fatal("no accrual anchor was opened; the window before the first checkpoint is lost")
	}
	// Everything is settled, so a second pass over the same Pod must write nothing.
	reconcileClaim(t, r, "c1")
	if writes != 1 {
		t.Fatalf("%d status writes after a second reconcile, want 1", writes)
	}

	// Rewind the anchor an hour (truncated, as the accrual's own clock is) so a whole window is
	// chargeable without sleeping, then let the loop charge it.
	got.Status.LastAccruedAt = &metav1.Time{Time: time.Now().Add(-time.Hour).Truncate(time.Second)}
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatalf("rewind the anchor: %v", err)
	}
	NewCostAccrual(c).accrueAll(context.Background())

	// The two zeroes are the point, not noise. They are what stands between this claim's first
	// window and a series whose first sample already holds money: with them, the 10 below is a RISE
	// that increase() can difference; without them it is invisible to every billing query.
	//
	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	wantSeries := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",example_com_org_id="acme",phase="Bound",provider="fake",region="us-east-1"} 10
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",example_com_org_id="acme",phase="Terminated",provider="fake",region="us-east-1"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",example_com_org_id="acme",phase="Terminating",provider="fake",region="us-east-1"} 0
`
	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(wantSeries)); err != nil {
		t.Fatal(err)
	}
}

// Seeding follows the patch, never the stamp. An anchor that failed to persist opens no window, so a
// series minted alongside it advertises spend that cannot arrive — and for a claim whose writes keep
// failing it is pure cardinality on a counter that never releases a series.
//
// The last pass is the restart: series are process-local, so a claim that is already anchored — one
// this process never stamped — has to be re-seeded off the initial sync, without a status write.
// That is why the seed is level-triggered rather than riding the stamp's once-per-claim gate.
func TestMarkPhase_SeedsBaseline(t *testing.T) {
	metrics.CostTotal.Reset()

	pod := gpuPod("H100", 1, "1", "1Gi")
	withInstanceID(pod, "sb-1")

	nc := newClaim("c1", "p1", "default", "uid-1", "fake")
	nc.Spec.Accelerator = "H100:1"
	nc.Spec.Region = "us-east-1"
	nc.Spec.CapacityType = nebulav1alpha1.CapacityOnDemand

	refuse := true
	writes := 0
	s := testScheme(t)
	c := fake.NewClientBuilder().
		WithScheme(s).
		WithObjects(pod, nc).
		WithStatusSubresource(&nebulav1alpha1.NodeClaim{}).
		WithInterceptorFuncs(interceptor.Funcs{
			SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string,
				obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if refuse {
					return apierrors.NewInternalError(errors.New("status write refused"))
				}
				writes++
				return cl.SubResource(sub).Update(ctx, obj, opts...)
			},
		}).
		Build()
	pp := &pricedProvider{fakeProvider: &fakeProvider{name: "fake"}, rate: 10}
	r := &NodeClaimReconciler{Client: c, Scheme: s, Providers: func(name string) (provider.Provider, bool) {
		return pp, name == pp.name
	}}

	if _, err := r.Reconcile(context.Background(), reconcile.Request{
		NamespacedName: types.NamespacedName{Name: "c1"},
	}); err == nil {
		t.Fatal("reconcile succeeded with the status write refused; the error must reach the caller")
	}
	if n := testutil.CollectAndCount(metrics.CostTotal); n != 0 {
		t.Fatalf("%d series after a failed patch, want 0 — the seed must not outrun the anchor", n)
	}

	refuse = false
	reconcileClaim(t, r, "c1")

	if got := getClaim(t, c, "c1"); got.Status.LastAccruedAt == nil {
		t.Fatal("no accrual anchor persisted; the seed below would have nothing to baseline")
	}
	// Zero, not absent: the dollars come later, and a baseline's whole job is to be the sample
	// before them. See metrics.TouchSeries for why RecordWindow still refuses a zero WINDOW.
	//
	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",phase="Bound",provider="fake",region="us-east-1"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",phase="Terminated",provider="fake",region="us-east-1"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="OnDemand",phase="Terminating",provider="fake",region="us-east-1"} 0
`
	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	// Reset stands in for a restart or a leader handoff: the claim is untouched and still anchored,
	// but every series this process published is gone. Nothing here needs a write — the anchor is
	// already durable — so a seed gated on the stamp would leave the whole fleet's first
	// post-restart window landing on newborn series, an interval before the accrual loop's first tick.
	metrics.CostTotal.Reset()
	reconcileClaim(t, r, "c1")

	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("%d status writes, want 1 — re-seeding a settled claim must not patch it", writes)
	}
}

// Which claims get a baseline at all, over the shapes one reconcile pass cannot cover.
func TestSeedClaimBaseline(t *testing.T) {
	metrics.CostTotal.Reset()
	hour := time.Hour

	// Every claim gets a shape of its own, or a claim wrongly skipped would hide behind the series
	// another one seeded — only "same-shape" shares deliberately.
	shape := billingClaim("same-shape", "10.0000", &hour)
	// Anchored and priced, so its last window is still booked at teardown: it needs a baseline as
	// much as a running claim does, which is why seeding asks parsePrice rather than billingRate.
	terminated := billingClaim("terminated", "10.0000", &hour)
	terminated.Status.Phase = nebulav1alpha1.NodeClaimTerminated
	terminated.Spec.Accelerator = "A100:2"
	unanchored := billingClaim("unanchored", "10.0000", nil)
	unanchored.Spec.Accelerator = "L4:1"
	unpriced := billingClaim("unpriced", "", &hour)
	unpriced.Spec.Accelerator = "T4:1"
	cpu := billingClaim("cpu", "0.1000", &hour)
	cpu.Spec.Accelerator = ""

	for _, nc := range []*nebulav1alpha1.NodeClaim{
		billingClaim("bound", "10.0000", &hour), shape, terminated, unanchored, unpriced, cpu,
	} {
		seedClaimBaseline(nc)
	}

	// Three shapes, three phases each. The two claims sharing H100:1 collapse onto one set of series
	// — a baseline is per SERIES, not per claim — while L4 and T4 are absent entirely: no window can
	// ever be booked for a claim with no anchor or no price, so a baseline would be an empty promise.
	//
	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="A100",accelerator_count="2",capacity_type="none",phase="Bound",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="A100",accelerator_count="2",capacity_type="none",phase="Terminated",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="A100",accelerator_count="2",capacity_type="none",phase="Terminating",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Bound",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Terminated",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Terminating",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="none",accelerator_count="none",capacity_type="none",phase="Bound",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="none",accelerator_count="none",capacity_type="none",phase="Terminated",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="none",accelerator_count="none",capacity_type="none",phase="Terminating",provider="modal",region="none"} 0
`
	if err := testutil.CollectAndCompare(metrics.CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// The stamp is what makes a crash before the first checkpoint lossless, so it must land exactly
// when the claim becomes chargeable — and never move once set.
func TestStampAccrualStart(t *testing.T) {
	hour := time.Hour
	cases := map[string]struct {
		claim *nebulav1alpha1.NodeClaim
		want  bool
	}{
		"billable and unanchored": {claim: billingClaim("a", "3.9500", nil), want: true},
		"already anchored":        {claim: billingClaim("b", "3.9500", &hour), want: false},
		"unpriced":                {claim: billingClaim("c", "", nil), want: false},
	}
	provisioning := billingClaim("d", "3.9500", nil)
	provisioning.Status.Phase = nebulav1alpha1.NodeClaimProvisioning
	cases["not holding an instance"] = struct {
		claim *nebulav1alpha1.NodeClaim
		want  bool
	}{claim: provisioning, want: false}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			before := tc.claim.Status.LastAccruedAt
			if got := stampAccrualStart(tc.claim); got != tc.want {
				t.Fatalf("stampAccrualStart = %v, want %v", got, tc.want)
			}
			if !tc.want && tc.claim.Status.LastAccruedAt != before {
				t.Fatal("the anchor was rewritten")
			}
		})
	}
}

// A cost checkpoint must not re-enqueue the claim, but anything alongside it must.
func TestIgnoreCostAccrual(t *testing.T) {
	p := ignoreCostAccrual()
	hour := time.Hour

	base := billingClaim("bound", "3.9500", &hour)
	costOnly := base.DeepCopy()
	costOnly.Status.EstimatedCostUSD = "12.0000"
	costOnly.Status.LastAccruedAt = &metav1.Time{Time: time.Now()}
	costOnly.ResourceVersion = "999"
	if p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: costOnly}) {
		t.Fatal("a cost-only checkpoint was enqueued")
	}

	alsoPhase := costOnly.DeepCopy()
	alsoPhase.Status.Phase = nebulav1alpha1.NodeClaimTerminating
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: alsoPhase}) {
		t.Fatal("a phase change riding along with a checkpoint was dropped")
	}

	withFinalizer := base.DeepCopy()
	withFinalizer.Finalizers = append(withFinalizer.Finalizers, nebulav1alpha1.TerminateInstanceFinalizer)
	if !p.Update(event.UpdateEvent{ObjectOld: base, ObjectNew: withFinalizer}) {
		t.Fatal("a finalizer update was dropped; the reconciler requeues off that event")
	}
}

func TestCostAccrual_StartStopsOnContextCancel(t *testing.T) {
	a, _ := newAccrual(t)
	a.interval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Start(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
}

// Start must actually checkpoint on a tick. Only that something is persisted is asserted: a test
// cannot pin the scheduler to a known window.
func TestCostAccrual_StartAccrues(t *testing.T) {
	hour := time.Hour
	a, c := newAccrual(t, billingClaim("bound", "3.9500", &hour))
	a.interval = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Start(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if total, _ := ledger(t, c, "bound"); total > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("Start persisted nothing after 5s of 1ms ticks")
}
