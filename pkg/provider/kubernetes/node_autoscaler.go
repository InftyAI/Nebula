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

package kubernetes

import (
	"context"
	"time"

	corev1 "k8s.io/api/core/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// NodeAutoscalerKarpenter is the default, and today the only, node autoscaler.
const NodeAutoscalerKarpenter = "karpenter"

// nodeAutoscaler is everything that depends on what adds nodes to a cluster: the labels a pod
// selects to get the right GPU and tier, how it says it gave up, and how long a healthy launch
// takes. Supporting another one is one more implementation in nodeAutoscalers.
type nodeAutoscaler interface {
	// selection steers the pod onto nodes with the GPU families and tier. No families is a
	// CPU-only pod; an empty tier leaves it to the cluster.
	selection(families []string, tier nebulav1alpha1.CapacityType) nodeSelection
	// refused looks once at a pending, unbound pod for the node autoscaler's verdict that it
	// will not get it a node, returning an error wrapping provider.ErrNoCapacity, or nil. It
	// only speeds up failure: success is the pod binding, and silence ends at bindTimeout.
	refused(ctx context.Context, cc clusterClients, pod *corev1.Pod) error
	// provisionTimeout is how long Provision watches a new pod for a binding or refused: long
	// enough for the node autoscaler to decide, short enough not to hold a worker through
	// node boot. Capabilities.ProvisionTimeout adds provisionSlack for the create calls.
	provisionTimeout() time.Duration
	// bindTimeout is how long a healthy launch may take to bind the pod, node boot included,
	// counted from the pod's creation. Past it, List reports no capacity.
	bindTimeout() time.Duration
	// verify fails when the node autoscaler is not installed in the cluster, so a mismatch shows
	// up at startup instead of as pods that never schedule.
	verify(ctx context.Context, cc clusterClients) error
}

// nodeSelection is what a node autoscaler adds to the real pod.
type nodeSelection struct {
	affinity    *corev1.Affinity
	tolerations []corev1.Toleration
	annotations map[string]string
}

var nodeAutoscalers = map[string]nodeAutoscaler{
	NodeAutoscalerKarpenter: karpenter{},
}
