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

import "strconv"

// none is the placeholder for a label the request genuinely did not carry: no
// region (unconstrained), no capacity tier (the provider's default), or no
// accelerator (a CPU-only Pod). An explicit token beats an empty string, which in
// PromQL is indistinguishable from a label that was never set and silently matches
// `{region=""}` selectors an operator did not mean to write.
const none = "none"

// candidateLabels is the label set that identifies a PLACEMENT CANDIDATE — the
// (provider, region, capacity type, accelerator pool) tuple that placement selects and
// provisioning then acts on. Both legs carry it, in this order, which is what lets a
// placement and the provisioning attempt it led to be joined in PromQL without label
// surgery. Extending it therefore touches both legs at once; that is intended.
//
// It is deliberately the same dimensions as failover.Candidate, minus the joined
// accelerator (see below), so a counted failure and an excluded candidate line up.
//
// Region is included because a capacity shortfall is region-local and comparing regions
// is the whole point of collecting this; note that for a provider which collapses
// several declared regions into one candidate (Modal) the value is that provider's
// joined token, not a single region name — see NodeClaimSpec.Region.
//
// The accelerator TYPE and COUNT are two labels, not the joined "H100:8" pool identity
// used as the blocklist key. A metric label set is meant to be aggregated over, and a
// joined string cannot be: `sum by (accelerator)` over every size of H100 requires
// splitting the value in PromQL, and selecting all 8-GPU requests across types is not
// expressible at all. Two labels give both for free, and the pool key is still
// recoverable as accelerator + ":" + accelerator_count when correlating with a
// blocklist entry.
var candidateLabels = []string{"provider", "region", "capacity_type", "accelerator", "accelerator_count"}

// withExtra returns candidateLabels plus one trailing dimension (result, reason), for
// the collectors that carry an outcome. It copies, because appending to a package-level
// slice would let two collectors share and overwrite one backing array.
func withExtra(name string) []string {
	return append(append([]string{}, candidateLabels...), name)
}

// Labels identifies the candidate one placement or provisioning attempt was made
// against. The zero value is valid: every field normalizes to "none".
type Labels struct {
	Provider     string
	Region       string
	CapacityType string
	// Accelerator is the accelerator TYPE alone (e.g. "H100"), and AcceleratorCount how
	// many were requested. They are kept apart so both aggregations work — see
	// candidateLabels. Empty/zero for a CPU-only Pod.
	Accelerator      string
	AcceleratorCount int32
}

// values renders the label set in candidateLabels order, with extra appended for
// the metrics that carry a result/reason dimension.
func (l Labels) values(extra ...string) []string {
	return append([]string{
		orNone(l.Provider),
		orNone(l.Region),
		orNone(l.CapacityType),
		orNone(l.Accelerator),
		countOrNone(l.AcceleratorCount),
	}, extra...)
}

func orNone(s string) string {
	if s == "" {
		return none
	}
	return s
}

// countOrNone renders an accelerator count, or the placeholder when there is no
// accelerator to count. Zero is deliberately NOT rendered as "0": a CPU-only Pod did not
// request zero GPUs, it requested none at all, and "0" would put it in the same numeric
// series an operator reads as a real count. util.AcceleratorRequest never returns a
// positive count without a type, so this cannot mask a real request.
func countOrNone(n int32) string {
	if n <= 0 {
		return none
	}
	return strconv.FormatInt(int64(n), 10)
}
