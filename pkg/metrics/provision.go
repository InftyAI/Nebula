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
	"context"
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
	// ReasonUnreachable: the provider never told us what it decided — a transport
	// failure, a 503, an unparseable response. Kept separate from every other reason
	// because it is the one that is NOT about capacity, credentials or the request: it
	// says the integration itself is unhealthy, and it is the only reason for which
	// Nebula deliberately does not fail the Pod or blocklist the candidate (see
	// provider.IsRejection). A spike here alongside flat capacity/auth series is a
	// network or provider-outage signal, not a placement one.
	ReasonUnreachable = "unreachable"
	// ReasonOther: the provider DID reject the request, but not through a sentinel, so
	// the category is unavailable. In practice that means the adapter returned a raw API
	// error without wrapping it, which makes a sustained rate on this series a to-do
	// rather than an incident: wrap the condition in the adapter (see
	// docs/add-a-provider.md) and the failure moves onto its real category.
	ReasonOther = "other"
)

var (
	// ProvisionAttempts counts provisioning attempts by outcome. Rate of the
	// result="failure" series over the total is the provisioning error rate; the
	// per-region/accelerator breakdown is what tells you WHERE it is failing.
	ProvisionAttempts = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_provision_attempts_total",
		Help: "Total external instance provisioning attempts, by provider, region, capacity type, accelerator type, accelerator count and outcome.",
	}, withExtra("result"))

	// ProvisionFailures breaks failures down by coarse cause. It deliberately
	// overlaps ProvisionAttempts{result="failure"} rather than adding a reason label
	// there: reason is only meaningful on failure, and carrying it on the attempts
	// counter would multiply the success series by a label that is constant for
	// them.
	ProvisionFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "nebula_provision_failures_total",
		Help: "Failed provisioning attempts by coarse cause (capacity, quota, auth, unsupported_accelerator, timeout, other).",
	}, withExtra("reason"))

	// ProvisionDuration measures the provider's Provision call alone — not the
	// wait for the instance to become usable. The two differ enormously and for
	// different reasons: AWS sweeps a region's availability zones inside this call
	// (so a capacity shortage shows up as latency HERE), while Modal returns as soon
	// as the sandbox is accepted and the wait moves to InstanceReadyDuration.
	// Bucketed out to 300s because the call is bounded by
	// Capabilities.ProvisionTimeout, which AWS raises above the 90s default.
	ProvisionDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "nebula_provision_duration_seconds",
		Help:    "Latency of the provider's Provision call, by provider, region, capacity type, accelerator type, accelerator count and outcome.",
		Buckets: []float64{0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120, 180, 300},
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
		Name:    "nebula_instance_ready_duration_seconds",
		Help:    "Time from the start of provisioning to the instance first reporting Running, by provider, region, capacity type, accelerator type and accelerator count.",
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
	// Checked after the sentinels: a provider that hits its own deadline while
	// sweeping for capacity may wrap both, and the capacity cause is the more useful
	// of the two.
	case errors.Is(err, context.DeadlineExceeded):
		return ReasonTimeout
	// Everything left is either a rejection whose category we could not name, or a
	// failure to reach the provider at all. Splitting them is the whole point of having
	// this label: the first is a placement problem, the second an integration one, and
	// they are fixed by completely different people.
	case !provider.IsRejection(err):
		return ReasonUnreachable
	default:
		return ReasonOther
	}
}
