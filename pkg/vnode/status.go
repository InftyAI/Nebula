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

	"github.com/InftyAI/Nebula/pkg/provider"
)

// Pod status reasons the virtual node stamps on the Pod it reports. The Pod is
// the source of truth for the external instance's runtime state (the NodeClaim
// is a passive ledger and does not mirror this), so these live here in the
// vnode, not on the API type.
const (
	// reasonProvisioning: a provider Provision call has been issued but the
	// instance does not yet exist — we are still allocating it (e.g. EC2
	// RunInstances in flight). Set on CreatePod, before the first poll observes
	// the instance.
	reasonProvisioning = "Provisioning"
	// reasonInitializing: the instance EXISTS at the provider but is not yet
	// reachable — it is booting (EC2 "pending") or running-but-not-yet-passing its
	// reachability checks (running, <2/2, EC2's own "Initializing" status). It
	// mirrors that EC2 status-check term. Provisioning is done; the instance is
	// coming up. Distinct from Provisioning so a Pod stuck here points at a slow
	// boot / failing status checks, not a stuck allocation.
	reasonInitializing = "Initializing"
	// reasonRunning: the provider reports the instance running.
	reasonRunning = "Running"
	// reasonProvisionFailed: the provider rejected or failed the Provision call.
	reasonProvisionFailed = "ProvisionFailed"
	// reasonFailed: the provider reports the instance in a failed state.
	reasonFailed = "Failed"
	// reasonTerminated: the instance is gone from the provider (torn down,
	// reclaimed, or exited). Disappearance alone does not say WHY, so this is the
	// neutral term rather than "Preempted".
	reasonTerminated = "Terminated"
)

// applyState maps a provider Instance state onto the Pod status the virtual node
// reports. The Pod is the object the scheduler and user see, so the external
// instance's lifecycle is projected onto standard Pod phases/conditions:
//
//	Running    -> PodRunning, Ready=True
//	Pending    -> PodPending, Ready=False (starting)
//	Failed     -> PodFailed
//	Terminated -> PodFailed (instance gone: torn down or reclaimed out-of-band)
//
// Running already means "reachable": a provider only reports InstanceRunning once
// the instance has passed its readiness bar (for AWS, the 2/2 EC2 status checks —
// see toState), so reaching Running is the point at which the Pod is both Running
// and Ready. Ready is the condition Kubernetes counts toward a Deployment's ready
// replicas.
func applyState(pod *corev1.Pod, state provider.InstanceState, endpoint string, now metav1.Time) {
	switch state {
	case provider.InstanceRunning:
		setPhase(pod, corev1.PodRunning, reasonRunning, "external instance is running", now)
		setReady(pod, corev1.ConditionTrue, now)
		setContainerStatuses(pod, corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: now}}, true)
		// PodIP is validated by the API server as a literal IP, so only populate it
		// when the endpoint actually is one — an AWS public DNS name (the common
		// case) would make the whole status UpdateStatus fail with a 422 and strand
		// the Pod on its prior (Initializing) status forever. The endpoint is always
		// surfaced on EndpointAnnotation regardless of form (see Handler.applyEndpoint),
		// so a DNS-only instance still exposes its reachable address; PodIP just stays
		// empty in that case.
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
		// The instance is simply gone — absent from the provider's List, whether
		// torn down by us, deleted out-of-band, or reclaimed. We cannot tell WHY
		// from disappearance alone, so report the neutral, accurate "Terminated"
		// rather than "Preempted", which would falsely assert a provider reclaim.
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

// setContainerStatuses projects the single external-instance lifecycle onto a
// per-container status for every container in the Pod spec. Kubernetes has no
// notion of the external instance — the READY column, `kubectl wait`, and any
// controller keying off container readiness all read Status.ContainerStatuses,
// so a Pod with an empty array reads as 0/N even when its Ready condition is
// True. There is one real workload behind the whole Pod, so every container
// mirrors the same state. The container's Image is echoed back (required field);
// RestartCount stays 0 (the provider owns restarts, not the kubelet).
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
