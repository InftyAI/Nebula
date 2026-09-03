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
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// withAttribution swaps in a label set for one test and restores the default afterwards. Keys go
// through the real ParseCostLabels so a test cannot configure a shape the flag could not, then
// through configureCost rather than InitCost: a registry refuses the second set of label dimensions
// it ever sees for a name, and testutil collects straight from the collector anyway.
func withAttribution(t *testing.T, podKeys ...string) {
	t.Helper()

	labels, err := ParseCostLabels(strings.Join(podKeys, ","))
	if err != nil {
		t.Fatalf("ParseCostLabels(%v): %v", podKeys, err)
	}
	configureCost(labels)
	t.Cleanup(func() { configureCost(nil) })
}

func TestParseCostLabels(t *testing.T) {
	cases := map[string]struct {
		spec string
		want []string
		// names is the Prometheus dimension those keys emit under, checked wherever it differs from
		// the keys themselves — the parse and the derivation are one contract for a caller.
		names []string
		err   bool
	}{
		"empty is no attribution": {spec: ""},
		"bare keys pass through": {
			spec: "org_id,team_id",
			want: []string{"org_id", "team_id"},
		},
		"whitespace and blanks": {
			spec: " org_id , , team_id ",
			want: []string{"org_id", "team_id"},
		},
		// The point of the whole exercise: a qualified key is configured as the Pod carries it and
		// emitted as PromQL can express it — prefix and all, as kube-state-metrics does it.
		"qualified key keeps its prefix": {
			spec:  "example.com/org-id",
			want:  []string{"example.com/org-id"},
			names: []string{"example_com_org_id"},
		},
		"a set of qualified keys sharing one prefix": {
			spec: "example.com/experiment-id,example.com/org-id,example.com/team-id,example.com/user-id",
			want: []string{
				"example.com/experiment-id", "example.com/org-id", "example.com/team-id", "example.com/user-id",
			},
			names: []string{
				"example_com_experiment_id", "example_com_org_id", "example_com_team_id", "example_com_user_id",
			},
		},
		// Same name part under two prefixes: distinct series, which is what keeping the prefix buys.
		"same name part under two domains": {
			spec:  "a.com/org-id,b.com/org-id",
			want:  []string{"a.com/org-id", "b.com/org-id"},
			names: []string{"a_com_org_id", "b_com_org_id"},
		},
		"hyphen and dot are folded": {
			spec:  "org-id,team.id",
			want:  []string{"org-id", "team.id"},
			names: []string{"org_id", "team_id"},
		},
		// Legal Pod keys no folding can rescue: a Prometheus label name cannot start with a digit,
		// and the prefix being part of the name puts a digit-leading DOMAIN in the same boat.
		"leading digit":        {spec: "2team", err: true},
		"digit-leading domain": {spec: "4paradigm.com/org-id", err: true},
		"trailing underscore":  {spec: "org_", err: true},
		"spaces inside a key":  {spec: "org id", err: true},
		"empty name part":      {spec: "example.com/", err: true},
		"duplicate key":        {spec: "org_id,org_id", err: true},
		// Folding still lets two distinct keys meet, rarely — and a merge of two tenants' spend is
		// worth refusing even so.
		"distinct keys colliding on the derived name": {spec: "org-id,org.id", err: true},
		"bare key colliding with a qualified one":     {spec: "com_org_id,com/org-id", err: true},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := ParseCostLabels(tc.spec)
			if tc.err {
				if err == nil {
					t.Fatalf("ParseCostLabels(%q) = %v, want an error", tc.spec, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ParseCostLabels(%q): %v", tc.spec, err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ParseCostLabels(%q) = %v, want %v", tc.spec, got, tc.want)
			}
			// Unset means the keys are already legal names and emit unchanged.
			wantNames := tc.names
			if wantNames == nil {
				wantNames = tc.want
			}
			if names := metricNames(got); len(wantNames) > 0 && !reflect.DeepEqual(names, wantNames) {
				t.Fatalf("metricNames(%q) = %v, want %v", tc.spec, names, wantNames)
			}
		})
	}
}

// A DERIVED name that shadows one of the counter's own dimensions is not rejected by the parser —
// it is caught one step later, when InitCost registers a descriptor with a duplicate label name.
// Only a BARE key can reach it now that the prefix is kept: "example.com/provider" emits as
// example_com_provider and collides with nothing.
func TestInitCost_RejectsAShadowingName(t *testing.T) {
	t.Cleanup(func() { configureCost(nil) })

	labels, err := ParseCostLabels("provider")
	if err != nil {
		t.Fatalf("ParseCostLabels: %v", err)
	}
	if err := InitCost(labels); err == nil {
		t.Fatal("InitCost accepted a key emitting a label name the counter already carries")
	}
}

// Values are read by POD KEY and emitted under the DERIVED name — the whole point of splitting the
// two. A Pod that carried none of the keys is reported as "none" rather than dropping the
// dimension, which would silently split the series.
func TestRecordWindow_Attribution(t *testing.T) {
	withAttribution(t, "example.com/org-id", "example.com/team-id")

	l := Labels{Provider: "modal", Accelerator: "H100", AcceleratorCount: 1}
	RecordWindow(l, "Bound", map[string]string{"example.com/org-id": "acme", "example.com/team-id": "ml"}, 3.95)
	RecordWindow(l, "Bound", map[string]string{"example.com/org-id": "acme"}, 1.05)
	RecordWindow(l, "Bound", nil, 0.5)

	// The exposition format is line-oriented, so these cannot be wrapped.
	//nolint:lll
	want := `
# HELP nebula_cost_usd_total Cumulative USD billed by external instances, added window by window as it accrues.
# TYPE nebula_cost_usd_total counter
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",example_com_org_id="acme",example_com_team_id="ml",phase="Bound",provider="modal",region="none"} 3.95
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",example_com_org_id="acme",example_com_team_id="none",phase="Bound",provider="modal",region="none"} 1.05
nebula_cost_usd_total{accelerator="H100",accelerator_count="1",capacity_type="none",example_com_org_id="none",example_com_team_id="none",phase="Bound",provider="modal",region="none"} 0.5
`
	if err := testutil.CollectAndCompare(CostTotal, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}
}

// Values are looked up BY POD KEY, so the order they were stamped in cannot slide a team_id into
// the org_id column — the property that makes a --cost-labels change safe.
func TestRecordWindow_AttributionIsKeyedNotPositional(t *testing.T) {
	withAttribution(t, "example.com/org-id", "example.com/team-id")

	l := Labels{Provider: "modal"}
	RecordWindow(l, "Bound", map[string]string{"example.com/team-id": "ml", "example.com/org-id": "acme"}, 1)
	RecordWindow(l, "Bound", map[string]string{"example.com/org-id": "acme", "example.com/team-id": "ml"}, 1)

	if n := testutil.CollectAndCount(CostTotal); n != 1 {
		t.Fatalf("collected %d series, want 1 — the same pair rendered two ways", n)
	}
}

// Cardinality is WATCHED, never enforced: past the threshold the warning fires once and every
// window is still booked under its own real attribution. A billing metric that merged or dropped
// samples to defend itself would be wrong in a way its consumer could not detect.
func TestNoteSeries_WarnsWithoutCapping(t *testing.T) {
	withAttribution(t, "org_id")

	l := Labels{Provider: "modal"}
	for i := range costSeriesWarnThreshold + 10 {
		RecordWindow(l, "Bound", map[string]string{"org_id": strconv.Itoa(i)}, 1)
	}

	if n := testutil.CollectAndCount(CostTotal); n != costSeriesWarnThreshold+10 {
		t.Fatalf("collected %d series, want %d — nothing may be merged away", n, costSeriesWarnThreshold+10)
	}
	if got := testutil.ToFloat64(CostTotal.WithLabelValues(
		"modal", "none", "none", "none", "none", "Bound", "0")); got != 1 {
		t.Fatalf("the first tenant's series holds %v, want 1 — its attribution was rewritten", got)
	}

	// Having warned, the detector drops its bookkeeping rather than tracking a leak it has already
	// reported, and stays quiet from then on.
	costSeriesMu.Lock()
	defer costSeriesMu.Unlock()
	if !costSeriesWarned || costSeries != nil {
		t.Fatalf("warned = %v, tracking %d keys; want warned with the tracking released",
			costSeriesWarned, len(costSeries))
	}
}

// The detector must not run at all without attribution, where every dimension is bounded by
// configuration — otherwise a long-lived fleet would eventually warn about its own shapes.
func TestNoteSeries_QuietWithoutAttribution(t *testing.T) {
	configureCost(nil)

	RecordWindow(Labels{Provider: "modal"}, "Bound", map[string]string{"org_id": "acme"}, 1)

	costSeriesMu.Lock()
	defer costSeriesMu.Unlock()
	if costSeries != nil {
		t.Fatalf("tracking %d keys with attribution off, want none", len(costSeries))
	}
}

// With attribution off, a stamped claim's labels are ignored rather than joined onto the series:
// the counter's shape follows --cost-labels alone.
func TestRecordWindow_IgnoresStampsWhenUnconfigured(t *testing.T) {
	CostTotal.Reset()

	for _, p := range []string{"modal", "aws", "runpod"} {
		RecordWindow(Labels{Provider: p}, "Bound", map[string]string{"org_id": "ignored"}, 1)
	}

	if n := testutil.CollectAndCount(CostTotal); n != 3 {
		t.Fatalf("collected %d series, want 3 — one per provider, with no attribution dimension", n)
	}
}
