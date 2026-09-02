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
	"regexp"
	"strings"
	"sync"

	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

// costSeriesWarnThreshold is where cost cardinality stops looking like a fleet and starts looking
// like a leak. Attribution values come from Pod labels, which are TENANT-controlled, and a counter
// never releases a series, so a workload minting a fresh value per Pod grows process memory and
// every scrape payload until a restart.
//
// A budget rather than a guess: ~1KB per series in client_golang, so 5000 is a few MB of memory and
// roughly a megabyte of exposition text — far more than any fleet reaches by candidate shape alone.
const costSeriesWarnThreshold = 5000

// costLabelName is the intersection of two grammars, because one token is used as both a
// Kubernetes label key and a Prometheus label name: Prometheus forbids '-' and '.' and a
// leading digit, Kubernetes forbids a leading or trailing '_'. Anything outside the overlap is
// rejected at startup rather than mangled, since a silently normalised name that collides with
// another is a mis-attribution nobody would notice.
var costLabelName = regexp.MustCompile(`^[a-zA-Z]([a-zA-Z0-9_]*[a-zA-Z0-9])?$`)

// ParseCostLabels turns the --cost-labels value ("org_id,team_id") into the attribution
// dimension, in the order given. Empty input means no attribution, which is the default.
//
// Each entry names BOTH the Pod label to read and the metric label to emit — one token, so what
// the manifest configures is exactly what appears in PromQL. It fails rather than skipping a bad
// entry: silently dropping one would be discovered at invoicing time, when every series has
// already been recorded as "none".
//
// Only the NAME is checked here. An entry that shadows a dimension the counter already carries
// ("provider", "phase") parses, then fails at InitCost, where the registry rejects a duplicate
// label name — still at startup, just with a less pointed message.
//
// Setting this at all is what makes cost the only metric here whose CARDINALITY is not bounded by
// configuration: the values come from Pod labels. Nothing caps them — noteSeries only warns — so
// pick names whose value set the cluster's admission policy actually constrains.
func ParseCostLabels(spec string) ([]string, error) {
	fields := strings.Split(spec, ",")
	names := make([]string, 0, len(fields))
	seen := map[string]struct{}{}
	for _, raw := range fields {
		name := strings.TrimSpace(raw)
		if name == "" {
			continue
		}
		if len(name) > 63 {
			return nil, fmt.Errorf("cost label %q is longer than the 63-character Kubernetes label limit", name)
		}
		if !costLabelName.MatchString(name) {
			return nil, fmt.Errorf("cost label %q must match %s — it is used as both a Pod label key "+
				"and a Prometheus label name, so '-', '.' and a leading digit are all out", name, costLabelName)
		}
		if _, dup := seen[name]; dup {
			return nil, fmt.Errorf("cost label %q is listed twice", name)
		}
		seen[name] = struct{}{}
		names = append(names, name)
	}
	if len(names) == 0 {
		// nil rather than an empty slice: "unconfigured" is a state callers compare against.
		return nil, nil
	}
	return names, nil
}

// costLabelNames is the configured attribution dimension, in emit order. Read on every
// recording and written only by InitCost, before the manager starts.
//
// The ORDER lives here rather than in the stamped NodeClaim, which is what makes attribution
// safe against a flag change: values are looked up BY NAME at emit time, so adding, removing or
// reordering --cost-labels can never slide a team_id into the org_id column.
var costLabelNames []string

// CostLabelNames is the configured attribution dimension. Exported so the controller that stamps
// the values onto a claim reads the SAME list the metric emits, rather than keeping a second copy
// that could disagree with it.
func CostLabelNames() []string {
	return costLabelNames
}

// attributionValues renders the configured dimension from a claim's stamped labels, in
// costLabelNames order. A name the claim never carried reports "none" rather than being
// omitted, which would move the sample to a different series.
func attributionValues(stamped map[string]string) []string {
	if len(costLabelNames) == 0 {
		return nil
	}
	values := make([]string, 0, len(costLabelNames))
	for _, name := range costLabelNames {
		values = append(values, orNone(stamped[name]))
	}
	return values
}

// costSeries holds the label combinations emitted so far, and only until the warning fires: it is a
// leak DETECTOR, not a guard, so it is dropped once it has done its one job rather than growing
// alongside the leak it reports. Guarded because there are two recorders — the accrual loop and the
// reconciler workers that settle a teardown.
var (
	costSeriesMu     sync.Mutex
	costSeries       map[string]struct{}
	costSeriesWarned bool
)

// noteSeries warns once when the cost counter passes costSeriesWarnThreshold distinct series.
//
// It deliberately caps NOTHING. Merging tenants into an "overflow" bucket, or dropping the window,
// would each corrupt a metric an external billing service reads as truth; a metric that is too big
// is an operator's problem to fix at admission, while one that quietly rewrote its own labels is
// nobody's problem until invoicing. Skipped entirely without attribution, where every dimension is
// bounded by NodePools and provider catalogs.
func noteSeries(values []string) {
	if len(costLabelNames) == 0 {
		return
	}

	costSeriesMu.Lock()
	defer costSeriesMu.Unlock()

	if costSeriesWarned {
		return
	}
	if costSeries == nil {
		costSeries = map[string]struct{}{}
	}
	// NUL, because it cannot appear in a Kubernetes label value: joining on any legal character
	// would let two different combinations collide into one key.
	costSeries[strings.Join(values, "\x00")] = struct{}{}
	if len(costSeries) <= costSeriesWarnThreshold {
		return
	}
	costSeriesWarned = true
	logf.Log.WithName("metrics").Info("WARNING: the cost metric has passed its expected series "+
		"budget, which usually means an attribution label is carrying a per-Pod value. Nothing is "+
		"dropped or merged, so the dollars stay correct, but memory and every scrape grow until this "+
		"process restarts. Constrain these label values at admission.",
		"series", len(costSeries), "threshold", costSeriesWarnThreshold, "costLabels", costLabelNames)
	costSeries = nil
}
