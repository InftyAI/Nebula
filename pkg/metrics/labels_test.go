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

import "testing"

// An unset field renders as the explicit "none" placeholder, never "" — which in PromQL
// is indistinguishable from a label that was never set and silently matches {region=""}
// selectors an operator did not mean to write. The accelerator COUNT is held to the same
// rule: a CPU-only Pod did not request zero GPUs, it requested none, and rendering "0"
// would drop it into the numeric series an operator reads as real counts.
func TestLabels_RendersInCandidateLabelOrder(t *testing.T) {
	tests := []struct {
		name string
		in   Labels
		want []string
	}{
		{"zero value is all placeholders", Labels{}, []string{none, none, none, none, none}},
		{
			"cpu-only pod has no count, not a zero count",
			Labels{Provider: "p", Region: "r", CapacityType: "OnDemand"},
			[]string{"p", "r", "OnDemand", none, none},
		},
		{
			"accelerator type and count are separate values",
			Labels{Provider: "p", Region: "r", CapacityType: "Spot", Accelerator: "H100", AcceleratorCount: 8},
			[]string{"p", "r", "Spot", "H100", "8"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.in.values()
			if len(got) != len(candidateLabels) {
				t.Fatalf("values() = %v (%d values), want %d to match candidateLabels %v",
					got, len(got), len(candidateLabels), candidateLabels)
			}
			for i := range tt.want {
				if got[i] != tt.want[i] {
					t.Fatalf("values()[%d] (%s) = %q, want %q", i, candidateLabels[i], got[i], tt.want[i])
				}
			}
		})
	}
}
