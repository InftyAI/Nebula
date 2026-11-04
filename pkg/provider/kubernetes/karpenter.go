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
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

// Karpenter v1 (sigs.k8s.io/karpenter): its well-known labels, the annotation that keeps
// consolidation off a node, and the pod events it emits. FailedScheduling is also the
// kube-scheduler's reason, expected on every pod that needs a new node, so only Karpenter's
// counts.
const (
	instanceFamilyLabel    = "karpenter.k8s.aws/instance-family"
	capacityTypeLabel      = "karpenter.sh/capacity-type"
	doNotDisruptAnnotation = "karpenter.sh/do-not-disrupt"
	capacityTypeSpot       = "spot"
	capacityTypeOnDemand   = "on-demand"

	reasonFailedScheduling = "FailedScheduling"
	karpenterComponent     = "karpenter"

	// karpenterBindTimeout covers a healthy launch up to binding: Karpenter's decision, the
	// EC2 launch and node boot until the GPUs are advertised. Measured p-family boots may need
	// more; image pull comes after binding and is the readiness deadline's.
	karpenterBindTimeout = 5 * time.Minute
)

// nodeClaimGV is Karpenter v1's API, only checked for presence (see verify).
var nodeClaimGV = schema.GroupVersion{Group: "karpenter.sh", Version: "v1"}

type karpenter struct{}

func (karpenter) bindTimeout() time.Duration { return karpenterBindTimeout }

// selection pins the GPU model through the instance family (one model per NVIDIA family on
// AWS) and the tier through the capacity type, and keeps consolidation and drift from
// evicting a running sandbox to repack.
func (karpenter) selection(families []string, tier nebulav1alpha1.CapacityType) nodeSelection {
	var reqs []corev1.NodeSelectorRequirement
	if len(families) > 0 {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key: instanceFamilyLabel, Operator: corev1.NodeSelectorOpIn, Values: families,
		})
	}
	if ct := karpenterCapacityType(tier); ct != "" {
		reqs = append(reqs, corev1.NodeSelectorRequirement{
			Key: capacityTypeLabel, Operator: corev1.NodeSelectorOpIn, Values: []string{ct},
		})
	}
	sel := nodeSelection{annotations: map[string]string{doNotDisruptAnnotation: labelTrue}}
	if len(reqs) > 0 {
		// Reschedule takes time, let's be conservative here.
		sel.affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: reqs}},
			},
		}}
	}
	return sel
}

func karpenterCapacityType(tier nebulav1alpha1.CapacityType) string {
	switch tier {
	case nebulav1alpha1.CapacitySpot:
		return capacityTypeSpot
	case nebulav1alpha1.CapacityOnDemand:
		return capacityTypeOnDemand
	default:
		return ""
	}
}

// refusals reads Karpenter's FailedScheduling for every pod in the cluster at once: one per pod
// UID, so each attempt gets its own. The selector leaves out the kube-scheduler's, which every
// pod needing a new node gets. A lost event only costs the bind timeout.
//
// Every refusal fails the pod; only a capacity one (see capacityRefusal) blocks the candidate.
// EC2's InsufficientCapacityError is on the NodeClaim, which names no pod, so the pod's own
// message is the only per-pod signal of a shortage.
func (karpenter) refusals(ctx context.Context, cc clusterClients) (map[types.UID]provider.InstanceState, error) {
	evs, err := cc.kube.CoreV1().Events(metav1.NamespaceAll).List(ctx, metav1.ListOptions{
		FieldSelector: fields.Set{
			"involvedObject.kind": "Pod", "source": karpenterComponent, "type": corev1.EventTypeWarning,
		}.String(),
		ResourceVersion: "0", // served from the API server's watch cache
	})
	if err != nil {
		return nil, err
	}
	out := map[types.UID]provider.InstanceState{}
	for _, ev := range evs.Items {
		// Checked again here: the selector is the server's job, and not every client honours it.
		if !fromKarpenter(ev) {
			continue
		}
		if ev.Reason != reasonFailedScheduling {
			continue
		}
		uid := ev.InvolvedObject.UID
		switch {
		case capacityRefusal(ev.Message):
			out[uid] = provider.InstanceUnschedulable
		case out[uid] != provider.InstanceUnschedulable: // a later spec refusal does not undo a shortage
			out[uid] = provider.InstanceFailed
		}
	}
	return out, nil
}

// capacityPhrases are the parts of Karpenter v1's FailedScheduling message that mean no capacity
// rather than the pod's own spec. A sold-out offering is filtered out as unavailable, so the
// instance-type filter reports a missing "offering"; the other two are NodePool limits.
var capacityPhrases = []string{"offering", "exceed limits", "limits have been exhausted"}

// capacityRefusal reports whether a Karpenter refusal is about capacity. The wording is
// Karpenter's, not an API, so TestCapacityRefusal pins real messages.
//
// Known gap, kept on purpose: when every family in a NodePool is unavailable at once,
// Karpenter drops the pool before scheduling, so its clause, and the "offering", is missing.
// Such a refusal fails the pod without a block, until Karpenter's unavailability marks lapse.
func capacityRefusal(message string) bool {
	return slices.ContainsFunc(capacityPhrases, func(p string) bool { return strings.Contains(message, p) })
}

func (karpenter) verify(_ context.Context, cc clusterClients) error {
	res, err := cc.kube.Discovery().ServerResourcesForGroupVersion(nodeClaimGV.String())
	if err == nil && slices.ContainsFunc(res.APIResources, func(r metav1.APIResource) bool {
		return r.Name == "nodeclaims"
	}) {
		return nil
	}
	return fmt.Errorf("kubernetes: Karpenter %s NodeClaims not found (%v); is Karpenter v1 installed?",
		nodeClaimGV, err)
}

func fromKarpenter(ev corev1.Event) bool {
	return strings.Contains(ev.Source.Component, karpenterComponent) ||
		strings.Contains(ev.ReportingController, karpenterComponent)
}
