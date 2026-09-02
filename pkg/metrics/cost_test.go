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
