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

import "strings"

// ConfigureCostForTest points the cost counter at a set of Pod label keys, for tests in other
// packages that exercise attribution. Not for production use: InitCost is the real entry point.
//
// It exists because InitCost can only ever succeed ONCE per process — a registry remembers a
// metric name's label dimensions for the life of the process — so a test binary cannot try two
// label sets through it. This skips registration; testutil collects from the collector directly.
//
// Keys go through the real ParseCostLabels, so a test cannot configure a shape the flag could not,
// and an unparseable key panics rather than silently configuring nothing.
//
// Pass nil to restore the unconfigured default, and do so in a Cleanup: the counter is a package
// variable, so a test that leaves it swapped changes what every later test measures.
func ConfigureCostForTest(podKeys []string) {
	labels, err := ParseCostLabels(strings.Join(podKeys, ","))
	if err != nil {
		panic("metrics: ConfigureCostForTest: " + err.Error())
	}
	configureCost(labels)
}
