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
	"math"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Windows accumulate on one series, which is what makes increase() over any range the dollars
// charged in that range.
func TestRecordWindow_Accumulates(t *testing.T) {
	CostTotal.Reset()

	l := Labels{Provider: "aws", Region: "us-east-1", CapacityType: "Spot", Accelerator: "H100", AcceleratorCount: 8}
	RecordWindow(l, "Bound", nil, 8.1933)
	RecordWindow(l, "Bound", nil, 8.1933)

	got := testutil.ToFloat64(CostTotal.WithLabelValues(l.values("Bound")...))
	if want := 2 * 8.1933; math.Abs(got-want) > 1e-9 {
		t.Fatalf("booked %v, want %v", got, want)
	}
}

// Claims on the same candidate share a series — this is spend by shape, not per instance, so their
// windows must sum. A CPU-only claim renders both accelerator labels "none", and Terminating spend
// splits off so a stuck teardown is visible.
func TestRecordWindow_Series(t *testing.T) {
	CostTotal.Reset()

	gpu := Labels{Provider: "modal", Accelerator: "H100", AcceleratorCount: 1}
	RecordWindow(gpu, "Bound", nil, 3.95)
	RecordWindow(gpu, "Bound", nil, 3.95)
	RecordWindow(gpu, "Terminating", nil, 0.33)
	RecordWindow(Labels{Provider: "modal"}, "Bound", nil, 0.0946)

	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Bound",provider="modal",region="none"} 7.9
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Terminating",provider="modal",region="none"} 0.33
nebula_cost_usd_total{accelerator="none",accelerator_count="none",capacity_type="none",phase="Bound",provider="modal",region="none"} 0.0946
`
	if err := testutil.CollectAndCompare(CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// A counter panics on a negative delta, and a zero window — the ordinary case for a claim whose
// anchor was just opened — would mint an empty series that reads as a real "this costs nothing".
func TestRecordWindow_DropsNonPositive(t *testing.T) {
	CostTotal.Reset()

	for _, usd := range []float64{0, -1} {
		RecordWindow(Labels{Provider: "modal"}, "Bound", nil, usd)
	}
	if n := testutil.CollectAndCount(CostTotal); n != 0 {
		t.Fatalf("collected %d series on a non-positive window, want 0", n)
	}
}

// The baseline exists so increase() has an earlier sample to difference against, which means it must
// be a series that COLLECTS at zero — not merely a child object the counter knows about.
func TestTouchSeries_PublishesAZeroBaseline(t *testing.T) {
	CostTotal.Reset()

	l := Labels{Provider: "modal", Accelerator: "H100", AcceleratorCount: 1}
	TouchSeries(l, nil, "Bound", "Terminating", "Terminated")

	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Bound",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Terminated",provider="modal",region="none"} 0
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",phase="Terminating",provider="modal",region="none"} 0
`
	if err := testutil.CollectAndCompare(CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// A baseline must not displace the charge that follows it, on the series it opened or on a repeat
// touch — the whole point is that the first real window still reads as a rise from zero.
func TestTouchSeries_DoesNotDisplaceTheFirstCharge(t *testing.T) {
	CostTotal.Reset()

	l := Labels{Provider: "modal"}
	TouchSeries(l, nil, "Bound")
	RecordWindow(l, "Bound", nil, 3.95)
	TouchSeries(l, nil, "Bound")

	if got := testutil.ToFloat64(CostTotal.WithLabelValues(l.values("Bound")...)); got != 3.95 {
		t.Fatalf("series reads %v, want 3.95", got)
	}
}

// The baseline is what makes a tenant-scoped series billable, so it has to land on the SAME series
// the charge will — attribution and all, or it buys nothing.
func TestTouchSeries_Attribution(t *testing.T) {
	withAttribution(t, "org_id")

	l := Labels{Provider: "modal"}
	TouchSeries(l, map[string]string{"org_id": "acme"}, "Bound")
	RecordWindow(l, "Bound", map[string]string{"org_id": "acme"}, 1)

	if n := testutil.CollectAndCount(CostTotal); n != 1 {
		t.Fatalf("collected %d series, want 1 — the baseline and the charge must share one", n)
	}
}

// NaN and Inf pass every ordering comparison, so the non-positive guard does not stop them. A
// counter cannot be decremented: one of these lands permanently, and every sum() over the fleet
// that spans the series reads NaN with it.
func TestRecordWindow_DropsNonFinite(t *testing.T) {
	CostTotal.Reset()

	RecordWindow(Labels{Provider: "modal"}, "Bound", nil, 1)
	for _, usd := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		RecordWindow(Labels{Provider: "modal"}, "Bound", nil, usd)
	}
	if got := testutil.ToFloat64(CostTotal); got != 1 {
		t.Fatalf("counter reads %v after non-finite windows, want the 1 it legitimately took", got)
	}
}
