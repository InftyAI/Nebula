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
	"net"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// Pod status reasons the virtual node stamps on the Pods it reports. This package is
// the only writer, but the strings are a shared contract (the NodeClaim controller reads
// them, operators match on them), so they live in api/v1alpha1 — see that const block
// for what each means.
const (
	reasonProvisioning    = nebulav1alpha1.PodReasonProvisioning
	reasonInitializing    = nebulav1alpha1.PodReasonInitializing
	reasonRunning         = nebulav1alpha1.PodReasonRunning
	reasonProvisionFailed = nebulav1alpha1.PodReasonProvisionFailed
	reasonFailed          = nebulav1alpha1.PodReasonFailed
	reasonTerminated      = nebulav1alpha1.PodReasonTerminated
)

// applyState projects a provider Instance state onto the Pod status, since the Pod is
// what the scheduler and user see:
//
//	Running    -> PodRunning, Ready=True
//	Pending    -> PodPending, Ready=False (starting)
//	Failed     -> PodFailed
//	Terminated -> PodFailed (instance gone: torn down or reclaimed out-of-band)
//
// Running already means "reachable": a provider reports InstanceRunning only once the
// instance passed its readiness bar (AWS's 2/2 status checks, Modal's readiness probe).
// That matters because Ready is what Kubernetes counts toward a Deployment's ready
// replicas.
//
// The bar is only as good as the provider's signal — a Modal sandbox created WITHOUT a
// probe has no observable readiness, so it reaches Running as soon as its process is live.
func applyState(pod *corev1.Pod, state provider.InstanceState, endpoint string, now metav1.Time) {
	switch state {
	case provider.InstanceRunning:
		setPhase(pod, corev1.PodRunning, reasonRunning, "external instance is running", now)
		setReady(pod, corev1.ConditionTrue, now)
		setContainerStatuses(pod, corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}, true)
		// The API server validates PodIP as a literal IP, so only set it when the
		// endpoint is one: a DNS name (AWS's common case) would fail the whole
		// UpdateStatus with a 422 and strand the Pod on its prior status. The address is
		// published on EndpointAnnotation either way, so nothing is lost.
		if endpoint != "" && net.ParseIP(endpoint) != nil {
			pod.Status.PodIP = endpoint
		}
	case provider.InstancePending:
		setPhase(pod, corev1.PodPending, reasonInitializing, "external instance is initializing", now)
		setReady(pod, corev1.ConditionFalse, now)
		setContainerStatuses(pod, corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: reasonInitializing, Message: "external instance is initializing"},
		}, false)
	case provider.InstanceFailed:
		setPhase(pod, corev1.PodFailed, reasonFailed, "external instance entered a failed state", now)
		setReady(pod, corev1.ConditionFalse, now)
		setContainerStatuses(pod, corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				Reason:     reasonFailed,
				Message:    "external instance entered a failed state",
				FinishedAt: now,
			},
		}, false)
	case provider.InstanceTerminated:
		// Gone from the provider's List — torn down by us, deleted out-of-band, or
		// reclaimed. Disappearance alone cannot say WHY, so report the neutral
		// "Terminated" rather than "Preempted", which would assert a reclaim we
		// did not observe.
		setPhase(pod, corev1.PodFailed, reasonTerminated, "external instance is gone", now)
		setReady(pod, corev1.ConditionFalse, now)
		setContainerStatuses(pod, corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{
				Reason:     reasonTerminated,
				Message:    "external instance is gone",
				FinishedAt: now,
			},
		}, false)
	}
}

// setContainerStatuses mirrors the one instance's state onto every container in the spec.
// The READY column, `kubectl wait`, and anything keying off container readiness read
// Status.ContainerStatuses, so an empty array reads as 0/N even with Ready=True. There is
// one real workload behind the Pod, so every container reports the same thing. Image is
// echoed back (required field); RestartCount stays 0, since the provider owns restarts.
func setContainerStatuses(pod *corev1.Pod, state corev1.ContainerState, ready bool) {
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		statuses = append(statuses, corev1.ContainerStatus{
			Name:    c.Name,
			Image:   c.Image,
			State:   state,
			Ready:   ready,
			Started: &ready,
		})
	}
	pod.Status.ContainerStatuses = statuses
}

// setPhase sets the coarse Pod phase plus a human-readable reason/message. The
// start time is stamped once so repeated updates keep a stable value.
func setPhase(pod *corev1.Pod, phase corev1.PodPhase, reason, msg string, now metav1.Time) {
	pod.Status.Phase = phase
	pod.Status.Reason = reason
	pod.Status.Message = msg
	if pod.Status.StartTime == nil {
		pod.Status.StartTime = &now
	}
}

// setReady sets the PodReady condition, preserving the transition time when the
// status is unchanged so watchers don't see spurious flips.
func setReady(pod *corev1.Pod, status corev1.ConditionStatus, now metav1.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type != corev1.PodReady {
			continue
		}
		if pod.Status.Conditions[i].Status != status {
			pod.Status.Conditions[i].Status = status
			pod.Status.Conditions[i].LastTransitionTime = now
		}
		return
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               corev1.PodReady,
		Status:             status,
		LastTransitionTime: now,
	})
}
