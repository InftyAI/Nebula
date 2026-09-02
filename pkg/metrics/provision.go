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
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/InftyAI/Nebula/pkg/provider"
)

// Result values for the result label. A provisioning attempt either returned an
// instance id or an error; there is no third outcome.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// Reason values for the failure label. This is a deliberately COARSE, closed set:
// it is a metric label, so it must stay bounded no matter what text a provider API
// returns. The fine-grained detail stays where it is already available (the Pod's
// Failed status message and the vnode-handler error log); this exists to answer
// "are we losing capacity, or are our credentials broken?" at a glance.
const (
	ReasonCapacity    = "capacity"
	ReasonQuota       = "quota"
	ReasonAuth        = "auth"
	ReasonUnsupported = "unsupported_accelerator"
	ReasonTimeout     = "timeout"
	// ReasonOther: the failure carried no sentinel, so its category is unavailable — either
	// the adapter returned a raw API error without wrapping it, or the provider never told
	// us what it decided at all (a transport failure, a 503, an unparseable response). A
	// sustained rate here is a to-do rather than an incident: wrap the condition in the
	// adapter (see docs/add-a-provider.md) and the failure moves onto its real category.
	// Which of the two it was is in the vnode-handler error log.
	ReasonOther = "other"
)

var (
	// ProvisionAttempts counts provisioning attempts by outcome. Rate of the
	// result="failure" series over the total is the provisioning error rate; the
	// per-region/accelerator breakdown is what tells you WHERE it is failing.
	ProvisionAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_provision_attempts_total",
		Help: "Total external instance provisioning attempts, by provider, region, capacity type, " +
			"accelerator type, accelerator count and outcome.",
	}, withExtra("result"))

	// ProvisionFailures breaks failures down by coarse cause. It deliberately
	// overlaps ProvisionAttempts{result="failure"} rather than adding a reason label
	// there: reason is only meaningful on failure, and carrying it on the attempts
	// counter would multiply the success series by a label that is constant for
	// them.
	ProvisionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_provision_failures_total",
		Help: "Failed provisioning attempts by coarse cause " +
			"(capacity, quota, auth, unsupported_accelerator, timeout, other).",
	}, withExtra("reason"))

	// ProvisionDuration measures the provider's Provision call alone — not the
	// wait for the instance to become usable. What lands here is provider-specific
	// and worth knowing per provider: AWS sweeps a region's availability zones
	// inside the call (so a capacity shortage shows up as latency HERE), Modal
	// builds the image inside it (so a cache miss does).
	//
	// The buckets run past the largest Capabilities.ProvisionTimeout (Modal's 5
	// minutes), rather than stopping at it: a ceiling AT the deadline would bury every
	// slow build in +Inf, while this way the 300s bucket is where a call killed by its
	// own deadline lands and anything above it is overshoot.
	ProvisionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nebula_provision_duration_seconds",
		Help: "Latency of the provider's Provision call, by provider, region, capacity type, " +
			"accelerator type, accelerator count and outcome.",
		Buckets: []float64{0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 420, 600},
	}, withExtra("result"))

	// InstanceReadyDuration measures the whole user-visible wait: from the moment
	// CreatePod starts provisioning to the first poll tick that reports the instance
	// Running. It therefore includes the Provision call, any provider-side queueing
	// for capacity, image pull, GPU attach, container boot, and up to one poll
	// interval of detection lag — which is the honest number, because that is what a
	// user waits.
	//
	// Only observed ONCE per Pod, on the first transition to Running, and never for
	// an instance re-adopted after a restart (the original start time is gone, and a
	// duration measured from re-adoption would understate it wildly). Buckets run to
	// 30min because a queueing provider on a large GPU shape genuinely takes that
	// long.
	InstanceReadyDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "nebula_instance_ready_duration_seconds",
		Help: "Time from the start of provisioning to the instance first reporting Running, " +
			"by provider, region, capacity type, accelerator type and accelerator count.",
		Buckets: []float64{5, 10, 20, 30, 45, 60, 90, 120, 180, 300, 600, 900, 1800},
	}, candidateLabels)
)

func init() {
	ctrlmetrics.Registry.MustRegister(
		ProvisionAttempts,
		ProvisionFailures,
		ProvisionDuration,
		InstanceReadyDuration,
	)
}

// ObserveProvision records one completed provisioning attempt: its outcome, its
// latency, and — when it failed — the coarse cause. It is the single call the
// virtual kubelet makes on both the success and failure paths, so the attempt and
// failure counters cannot drift out of step.
//
// err nil means success. d is the duration of the Provision call itself.
func ObserveProvision(l Labels, d time.Duration, err error) {
	result := ResultSuccess
	if err != nil {
		result = ResultFailure
	}
	ProvisionAttempts.WithLabelValues(l.values(result)...).Inc()
	ProvisionDuration.WithLabelValues(l.values(result)...).Observe(d.Seconds())
	if err != nil {
		ProvisionFailures.WithLabelValues(l.values(FailureReason(err))...).Inc()
	}
}

// ObserveReady records the end-to-end wait for an instance to reach Running.
func ObserveReady(l Labels, d time.Duration) {
	InstanceReadyDuration.WithLabelValues(l.values()...).Observe(d.Seconds())
}

// FailureReason maps a Provision error onto the closed reason set.
//
// It matches on the shared sentinels in pkg/provider rather than on message text,
// which is what keeps the label bounded: an adapter that wraps its API errors with
// ErrNoCapacity/ErrQuota/ErrAuth gets an accurate reason, and one that does not
// falls through to "other" instead of inventing a new series per distinct provider
// message. This is intentionally NOT provider.ClassifyError: that answers a
// different question (what to blocklist, and how widely), and its "unrecognized
// errors are scoped like capacity" default would be an outright lie as a metric —
// it would report unknown failures as capacity shortfalls.
func FailureReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, provider.ErrAuth):
		return ReasonAuth
	case errors.Is(err, provider.ErrQuota):
		return ReasonQuota
	case errors.Is(err, provider.ErrUnsupportedAccelerator):
		return ReasonUnsupported
	case errors.Is(err, provider.ErrNoCapacity):
		return ReasonCapacity
	case provider.IsDeadline(err):
		return ReasonTimeout
	default:
		return ReasonOther
	}
}
