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

package vnode

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// The collectors are package-level and registered once, so every test in this package
// shares them. Assertions are therefore on the DELTA a call produced, never on an
// absolute value — otherwise the specs would pass or fail depending on run order.

// histCount reads a histogram series' sample count. GetMetricWith creates the series
// when absent, so an unobserved label set reads as 0 rather than erroring.
func histCount(t *testing.T, h *prometheus.HistogramVec, l prometheus.Labels) uint64 {
	t.Helper()
	obs, err := h.GetMetricWith(l)
	if err != nil {
		t.Fatalf("GetMetricWith(%v): %v", l, err)
	}
	m, ok := obs.(prometheus.Metric)
	if !ok {
		t.Fatalf("observer for %v is not a prometheus.Metric", l)
	}
	pb := &dto.Metric{}
	if err := m.Write(pb); err != nil {
		t.Fatalf("write metric %v: %v", l, err)
	}
	return pb.GetHistogram().GetSampleCount()
}

// metricPod carries the half of the label set that comes off the Pod: the accelerator label
// the pool identity is derived from. Pair it with metricCluster, which supplies the other
// half — the tier and region, which are read from the NodeClaim, not the Pod.
func metricPod(ns, name string) *corev1.Pod {
	pod := testPod(ns, name)
	pod.Labels[nebulav1alpha1.AcceleratorTypeLabel] = "H100"
	return pod
}

// metricCluster records the placement the labels below expect.
func metricCluster() *fakeCluster {
	return clusterWithPlacement(nebulav1alpha1.CapacitySpot, "us-east-1")
}

// labelsFor is the label set metricPod produces. The accelerator TYPE and COUNT are
// separate labels so either aggregation works; the count is 1 because a type with no
// explicit nvidia.com/gpu limit means one GPU (see util.AcceleratorRequest).
func labelsFor(extraKey, extraVal string) prometheus.Labels {
	l := prometheus.Labels{
		"provider":          "fake",
		"region":            "us-east-1",
		"capacity_type":     string(nebulav1alpha1.CapacitySpot),
		"accelerator":       "H100",
		"accelerator_count": "1",
	}
	if extraKey != "" {
		l[extraKey] = extraVal
	}
	return l
}

func TestCreatePod_RecordsSuccessfulProvisionAttempt(t *testing.T) {
	success := labelsFor("result", metrics.ResultSuccess)
	beforeAttempts := testutil.ToFloat64(metrics.ProvisionAttempts.With(success))
	beforeDuration := histCount(t, metrics.ProvisionDuration, success)

	h := NewHandler(&fakeProvider{provisionID: "inst-1"}, nil, nil, metricCluster())
	if err := h.CreatePod(context.Background(), metricPod("default", "m1")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	if got := testutil.ToFloat64(metrics.ProvisionAttempts.With(success)) - beforeAttempts; got != 1 {
		t.Fatalf("success attempts delta = %v, want 1", got)
	}
	if got := histCount(t, metrics.ProvisionDuration, success) - beforeDuration; got != 1 {
		t.Fatalf("duration observations delta = %d, want 1", got)
	}
}

// A rejection is counted with the reason the sentinel names, so the failure series can
// answer "are we out of capacity, or are our credentials broken?" without log grepping.
func TestCreatePod_RecordsRejectionReason(t *testing.T) {
	failure := labelsFor("result", metrics.ResultFailure)
	capacity := labelsFor("reason", metrics.ReasonCapacity)
	beforeAttempts := testutil.ToFloat64(metrics.ProvisionAttempts.With(failure))
	beforeFailures := testutil.ToFloat64(metrics.ProvisionFailures.With(capacity))

	fp := &fakeProvider{provisionErr: provider.ErrNoCapacity}
	h := NewHandler(fp, nil, nil, metricCluster())
	if err := h.CreatePod(context.Background(), metricPod("default", "m2")); err == nil {
		t.Fatal("expected the provision error")
	}

	if got := testutil.ToFloat64(metrics.ProvisionAttempts.With(failure)) - beforeAttempts; got != 1 {
		t.Fatalf("failure attempts delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ProvisionFailures.With(capacity)) - beforeFailures; got != 1 {
		t.Fatalf("capacity failures delta = %v, want 1", got)
	}
}

// An unreachable provider is still a counted ATTEMPT — the call happened and failed —
// even though the handler deliberately does not fail the Pod or blocklist anything for
// it. reason="unreachable" is what distinguishes an integration outage from a capacity
// shortfall, which is the whole reason the two are not both "other".
func TestCreatePod_UnreachableProviderCountedSeparately(t *testing.T) {
	failure := labelsFor("result", metrics.ResultFailure)
	unreachable := labelsFor("reason", metrics.ReasonUnreachable)
	capacity := labelsFor("reason", metrics.ReasonCapacity)
	beforeAttempts := testutil.ToFloat64(metrics.ProvisionAttempts.With(failure))
	beforeUnreachable := testutil.ToFloat64(metrics.ProvisionFailures.With(unreachable))
	beforeCapacity := testutil.ToFloat64(metrics.ProvisionFailures.With(capacity))

	fp := &fakeProvider{provisionErr: errors.New("rpc error: code = Unavailable desc = transport is closing")}
	h := NewHandler(fp, nil, nil, metricCluster())
	if err := h.CreatePod(context.Background(), metricPod("default", "m3")); err == nil {
		t.Fatal("expected the provision error")
	}

	if got := testutil.ToFloat64(metrics.ProvisionAttempts.With(failure)) - beforeAttempts; got != 1 {
		t.Fatalf("failure attempts delta = %v, want 1", got)
	}
	if got := testutil.ToFloat64(metrics.ProvisionFailures.With(unreachable)) - beforeUnreachable; got != 1 {
		t.Fatalf("unreachable failures delta = %v, want 1", got)
	}
	// "Unavailable" in the gRPC status text must not be read as a capacity shortfall.
	if got := testutil.ToFloat64(metrics.ProvisionFailures.With(capacity)) - beforeCapacity; got != 0 {
		t.Fatalf("capacity failures delta = %v, want 0 for a transport error", got)
	}
}

// The ready duration is observed on the FIRST tick that reports Running and never
// again, because provisionStart is consumed. Without that, every subsequent tick would
// add a sample with an ever-growing value — measuring the pod's age, not its boot.
func TestReconcileOnce_ObservesReadyDurationExactlyOnce(t *testing.T) {
	ready := labelsFor("", "")
	before := histCount(t, metrics.InstanceReadyDuration, ready)

	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, metricCluster())
	if err := h.CreatePod(context.Background(), metricPod("default", "m4")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}

	// Still initializing: nothing to observe yet.
	fp.list = []provider.Instance{{ID: "inst-1", ClaimName: "default-m4", State: provider.InstancePending}}
	h.reconcileOnce(context.Background())
	if got := histCount(t, metrics.InstanceReadyDuration, ready) - before; got != 0 {
		t.Fatalf("observations while Pending = %d, want 0", got)
	}

	// First Running tick observes.
	fp.list = []provider.Instance{{ID: "inst-1", ClaimName: "default-m4", State: provider.InstanceRunning}}
	h.reconcileOnce(context.Background())
	if got := histCount(t, metrics.InstanceReadyDuration, ready) - before; got != 1 {
		t.Fatalf("observations after the first Running tick = %d, want 1", got)
	}

	// Every later tick must add nothing, however long the pod runs.
	h.reconcileOnce(context.Background())
	h.reconcileOnce(context.Background())
	if got := histCount(t, metrics.InstanceReadyDuration, ready) - before; got != 1 {
		t.Fatalf("observations after three Running ticks = %d, want 1 (the token is one-shot)", got)
	}
}

// A pod re-adopted after a restart has no recoverable start time, so its ready duration
// is SKIPPED rather than measured from re-adoption — which would report a wait of
// minutes as microseconds and bias the histogram fast. A missing sample beats a wrong
// one.
func TestGetPod_ReAdoptedPodIsNotReadyObserved(t *testing.T) {
	// A re-adopted pod is tracked with no placement and a synthesized Pod carrying no
	// accelerator request, so a wrongly taken observation would land on the series below:
	// the provider is known, everything the lost provision knew reads "none". That, not the
	// fully-labelled one, is the assertion carrying this test. Both are deltas because other
	// specs in this package write to both series.
	ready := labelsFor("", "")
	none := prometheus.Labels{
		"provider": "fake", "region": "none", "capacity_type": "none",
		"accelerator": "none", "accelerator_count": "none",
	}
	beforeReady := histCount(t, metrics.InstanceReadyDuration, ready)
	beforeNone := histCount(t, metrics.InstanceReadyDuration, none)

	// Cold map, live instance: the re-adoption path. The instance is already Running,
	// so a naive implementation would observe on the very next tick.
	fp := &fakeProvider{list: []provider.Instance{{
		ID: "inst-9", ClaimName: "default-m5", State: provider.InstanceRunning,
	}}}
	h := NewHandler(fp, nil, nil, metricCluster())
	if _, err := h.GetPod(context.Background(), "default", "m5"); err != nil {
		t.Fatalf("GetPod: %v", err)
	}
	h.reconcileOnce(context.Background())
	h.reconcileOnce(context.Background())

	if got := histCount(t, metrics.InstanceReadyDuration, none) - beforeNone; got != 0 {
		t.Fatalf("observations for a re-adopted pod = %d, want 0", got)
	}
	if got := histCount(t, metrics.InstanceReadyDuration, ready) - beforeReady; got != 0 {
		t.Fatalf("observations on the labelled series = %d, want 0", got)
	}
}

// The provisioning clock is independent of nowFn, the STATUS clock a test may pin. A
// pinned status clock must not produce a nonsense (or negative) duration.
func TestObserveReady_IndependentOfPinnedStatusClock(t *testing.T) {
	ready := labelsFor("", "")
	before := histCount(t, metrics.InstanceReadyDuration, ready)

	fp := &fakeProvider{provisionID: "inst-1"}
	h := NewHandler(fp, nil, nil, metricCluster())
	// A status clock pinned far in the PAST: reusing it to measure would go negative.
	pinned := metav1.NewTime(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC))
	h.nowFn = func() metav1.Time { return pinned }

	if err := h.CreatePod(context.Background(), metricPod("default", "m6")); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	fp.list = []provider.Instance{{ID: "inst-1", ClaimName: "default-m6", State: provider.InstanceRunning}}
	h.reconcileOnce(context.Background())

	if got := histCount(t, metrics.InstanceReadyDuration, ready) - before; got != 1 {
		t.Fatalf("observations = %d, want 1", got)
	}
	// A negative duration lands in no bucket, so a positive count in the smallest one is
	// the proof the measurement used the real clock.
	obs, err := metrics.InstanceReadyDuration.GetMetricWith(ready)
	if err != nil {
		t.Fatalf("GetMetricWith: %v", err)
	}
	pb := &dto.Metric{}
	if err := obs.(prometheus.Metric).Write(pb); err != nil {
		t.Fatalf("write: %v", err)
	}
	if sum := pb.GetHistogram().GetSampleSum(); sum < 0 {
		t.Fatalf("sample sum = %v, want a non-negative duration (the status clock leaked in)", sum)
	}
}
