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

	"k8s.io/apimachinery/pkg/util/validation"
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

// promLabelName is Prometheus's own label-name grammar, applied to the DERIVED name rather than to
// the Pod key. The legal Kubernetes keys it still rejects are the ones STARTING with a digit — a
// bare "2team", or a digit-leading domain like "4paradigm.com/org-id" — which nothing here can
// repair without inventing a prefix of its own.
var promLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// metricNameFor derives the Prometheus label name a Pod label key is emitted under: the WHOLE key,
// with '/', '-' and '.' folded to '_' — so "example.com/org-id" is queried as example_com_org_id.
//
// Keeping the domain prefix is what every Kubernetes exporter does (kube-state-metrics emits
// label_app_kubernetes_io_name, Prometheus service discovery __meta_kubernetes_pod_label_…), and
// for the reason that the prefix is part of the key's identity: two tenants' keys stay distinct,
// and the name in a dashboard is one that appears verbatim in `kubectl get pod --show-labels`.
func metricNameFor(podKey string) (string, error) {
	name := strings.NewReplacer("/", "_", "-", "_", ".", "_").Replace(podKey)
	if !promLabelName.MatchString(name) {
		return "", fmt.Errorf("cost label %q emits as %q, which Prometheus will not accept as a label "+
			"name (it must start with a letter or underscore)", podKey, name)
	}
	return name, nil
}

// ParseCostLabels turns the --cost-labels value ("example.com/org-id,team_id") into the attribution
// dimension, in the order given. Empty input means no attribution, which is the default.
//
// Each entry is a POD LABEL KEY, qualified or not; the metric label it emits under is derived from
// it, so a key is configured as the Pod carries it and queried as PromQL can express it. It fails
// rather than skipping a bad entry: silently dropping one would be discovered at invoicing time,
// when every series has already been recorded as "none".
//
// Four rejections, all at startup: a key Kubernetes would not accept, one whose derived name
// Prometheus would not, the same key twice, and two keys folding to the SAME name. That last one is
// a corner case now that the prefix is kept ("org-id" and "org.id" still meet), but it would merge
// two tenants' spend into one series, so it fails rather than being tolerated. A derived name that
// shadows a dimension the counter already carries ("provider", "phase") gets through here and fails
// at InitCost, where the registry rejects a duplicate label name; still at startup, just with a
// less pointed message.
//
// Setting this at all is what makes cost the only metric here whose CARDINALITY is not bounded by
// configuration: the values come from Pod labels. Nothing caps them — noteSeries only warns — so
// pick keys whose value set the cluster's admission policy actually constrains.
func ParseCostLabels(spec string) ([]string, error) {
	fields := strings.Split(spec, ",")
	keys := make([]string, 0, len(fields))
	seenKey := map[string]struct{}{}
	claimedBy := map[string]string{} // derived name -> the key that got there first
	for _, raw := range fields {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		// IsQualifiedName carries the whole Kubernetes key grammar — the 253-character prefix, the
		// 63-character name, and the alphanumeric-at-both-ends rule — so none of it is restated here.
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("cost label %q is not a valid Pod label key: %s", key, strings.Join(errs, "; "))
		}
		name, err := metricNameFor(key)
		if err != nil {
			return nil, err
		}
		if _, dup := seenKey[key]; dup {
			return nil, fmt.Errorf("cost label %q is listed twice", key)
		}
		if first, clash := claimedBy[name]; clash {
			return nil, fmt.Errorf("cost labels %q and %q both emit as %q, which would merge two "+
				"tenants' spend into one series; drop one or rename it", first, key, name)
		}
		seenKey[key] = struct{}{}
		claimedBy[name] = key
		keys = append(keys, key)
	}
	if len(keys) == 0 {
		// nil rather than an empty slice: "unconfigured" is a state callers compare against.
		return nil, nil
	}
	return keys, nil
}

// costLabelKeys is the configured attribution dimension, as POD LABEL KEYS in emit order. Read on
// every recording and written only by configureCost, before the manager starts.
//
// Only the keys are kept. The Prometheus names are a pure function of them (metricNameFor) and are
// needed in exactly one place, when the counter is constructed, so storing them here would be a
// second copy of a derivable fact — one that could go stale against this one.
//
// The ORDER lives here rather than in the stamped NodeClaim, which is what makes attribution safe
// against a flag change: values are looked up BY KEY at emit time, so adding, removing or
// reordering --cost-labels can never slide a team_id into the org_id column.
var costLabelKeys []string

// CostLabelKeys is the Pod label keys attribution reads, in emit order. Exported so the controller
// that stamps the values onto a claim reads the SAME list the metric emits, rather than keeping a
// second copy that could disagree with it.
//
// POD KEYS, not the metric's label names — the two differ whenever a key is qualified. Stamping a
// claim needs the former; building the counter needs metricNames.
func CostLabelKeys() []string {
	return costLabelKeys
}

// metricNames is the Prometheus label names a set of Pod keys is emitted under, in the same order.
//
// The error metricNameFor can return is dropped: ParseCostLabels has already rejected any key that
// fails, and if a caller ever bypassed it, prometheus.Register refuses an illegal label name — so
// the invariant is enforced twice over without an unreachable error path here.
func metricNames(podKeys []string) []string {
	names := make([]string, 0, len(podKeys))
	for _, key := range podKeys {
		name, _ := metricNameFor(key)
		names = append(names, name)
	}
	return names
}

// attributionValues renders the configured dimension from a claim's stamped labels, in
// costLabelKeys order. A key the claim never carried reports "none" rather than being omitted,
// which would move the sample to a different series.
func attributionValues(stamped map[string]string) []string {
	if len(costLabelKeys) == 0 {
		return nil
	}
	values := make([]string, 0, len(costLabelKeys))
	for _, key := range costLabelKeys {
		values = append(values, orNone(stamped[key]))
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
	if len(costLabelKeys) == 0 {
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
		"series", len(costSeries), "threshold", costSeriesWarnThreshold, "costLabels", costLabelKeys)
	costSeries = nil
}
