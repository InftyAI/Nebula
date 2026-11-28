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
	"fmt"
	"maps"
	"slices"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
)

const (
	// reflectedLabel marks a copy of a home-cluster Secret or ConfigMap (see reflect.go). Reflection
	// only ever updates or deletes objects that carry it.
	reflectedLabel = "nebula.inftyai.com/reflected"

	labelTrue = "true"
	podKind   = "Pod"
)

// ownerLabels mark what this provider created. Placeholders carry neither (placement puts the
// provider in their nodeSelector, not their labels), so in the host cluster List never
// mistakes one for an instance; both are required in case a user sets one of them.
func ownerLabels() map[string]string {
	return map[string]string{
		nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
		nebulav1alpha1.ProviderLabel:  provider.ProviderKubernetes,
	}
}

// ParsePropagateLabels turns the --kubernetes-propagate-labels value into label keys. It fails
// rather than dropping a bad entry, so nobody believes a label propagates when it does not: a key
// Kubernetes would not accept, one listed twice, and one of Nebula's own, which buildPod would
// overwrite or which would send the real pod back through Nebula's webhook in the host cluster.
func ParsePropagateLabels(spec string) ([]string, error) {
	var keys []string //nolint:prealloc // nil, not empty, when nothing is listed
	for _, raw := range strings.Split(spec, ",") {
		key := strings.TrimSpace(raw)
		switch {
		case key == "":
			continue
		case slices.Contains(keys, key):
			return nil, fmt.Errorf("propagated label %q is listed twice", key)
		case strings.HasPrefix(key, nebulav1alpha1.GroupVersion.Group+"/") || key == nebulav1alpha1.ManagedByLabel:
			return nil, fmt.Errorf("propagated label %q is Nebula's own and cannot be copied", key)
		}
		if errs := validation.IsQualifiedName(key); len(errs) > 0 {
			return nil, fmt.Errorf("propagated label %q is not a valid label key: %s", key, strings.Join(errs, "; "))
		}
		keys = append(keys, key)
	}
	return keys, nil
}

func ownedByUs(obj metav1.Object) bool {
	for k, v := range ownerLabels() {
		if obj.GetLabels()[k] != v {
			return false
		}
	}
	return true
}

// podSpec is everything buildPod needs that is not on the placeholder.
type podSpec struct {
	claim     string
	selection nodeSelection
	// labels are the placeholder's label keys the real pod carries: --kubernetes-propagate-labels
	// and --cost-labels. An allowlist on purpose: in the host cluster the real pod shares the
	// placeholder's namespace, where a controller label (pod-template-hash,
	// batch.kubernetes.io/controller-uid) would let the placeholder's owner adopt it, and anything
	// selecting on labels (a Service, a PDB, an operator) would count the sandbox twice.
	labels []string
}

// buildPod renders the real pod from the placeholder: a copy of its spec, so the sandbox runs
// as the user wrote it (probes, hooks, security context, tolerations), minus what cannot
// carry over and with Nebula's own scheduling replaced. Metadata is not copied: labels are
// chosen below, since the opt-in label alone would send the pod back through Nebula's
// webhook in the host cluster, and a controller label would let the placeholder's Job adopt it.
func buildPod(local *corev1.Pod, s podSpec) *corev1.Pod {
	// Named after the claim like every provider's instance (util.ClaimName: "<namespace>-<pod>"),
	// which also keeps it off the placeholder's own name in the host cluster.
	name := s.claim

	labels := map[string]string{}
	for _, k := range s.labels {
		// Nebula's own labels drive placement: the opt-in one alone would send the real pod back
		// through Nebula when the host is the target. ParsePropagateLabels refuses them, but
		// --cost-labels is not checked, so they are skipped here too.
		if strings.HasPrefix(k, nebulav1alpha1.GroupVersion.Group+"/") {
			continue
		}
		if v, ok := local.Labels[k]; ok {
			labels[k] = v
		}
	}
	maps.Copy(labels, ownerLabels()) // last, so a copied key cannot overwrite them

	spec := *local.Spec.DeepCopy()

	// Nebula's scheduling: the placeholder is bound to the virtual node through these.
	spec.NodeName = ""
	spec.SchedulingGates = nil
	delete(spec.NodeSelector, nebulav1alpha1.ProviderLabel)
	spec.Tolerations = slices.DeleteFunc(spec.Tolerations, func(t corev1.Toleration) bool {
		return t.Key == nebulav1alpha1.ProviderLabel
	})
	spec.Tolerations = append(spec.Tolerations, acceleratorTolerations(&spec.Containers[0])...)
	spec.Tolerations = append(spec.Tolerations, s.selection.tolerations...)
	spec.Affinity = withNodeRequirements(spec.Affinity, s.selection.affinity)

	// The webhook admits no volume except the service-account token, which is not mounted here.
	// imagePullSecrets are kept as written: reflected ones are usable in the target, and the
	// kubelet skips any that are missing or aren't registry credentials.
	spec.ServiceAccountName, spec.DeprecatedServiceAccount = "", ""
	spec.AutomountServiceAccountToken = ptrTo(false)
	spec.Volumes = nil
	spec.ResourceClaims = nil
	spec.SchedulerName = ""
	spec.Overhead = nil

	c := &spec.Containers[0]
	c.VolumeMounts = nil
	// env and envFrom stay as written (see Capabilities.KeepsEnvReferences): the target's kubelet
	// resolves them from the reflected copies, and a reference that was never reflected fails
	// the pod there with CreateContainerConfigError, as on any node.

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   local.Namespace,
			Labels:      labels,
			Annotations: s.selection.annotations,
		},
		Spec: spec,
	}
}

// withNodeRequirements adds the node autoscaler's required node affinity to the user's own.
// Required terms are ORed, so the requirements go into every one of them, except an empty
// term, which matches no node and must stay that way; preferences and pod (anti-)affinity are
// left as the user wrote them.
func withNodeRequirements(user, ours *corev1.Affinity) *corev1.Affinity {
	if ours == nil || ours.NodeAffinity == nil || ours.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return user
	}
	ourRequired := ours.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	reqs := ourRequired.NodeSelectorTerms[0].MatchExpressions
	if user == nil {
		return ours
	}
	out := user.DeepCopy()
	if out.NodeAffinity == nil {
		out.NodeAffinity = &corev1.NodeAffinity{}
	}
	required := out.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil {
		out.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = ourRequired.DeepCopy()
		return out
	}
	for i := range required.NodeSelectorTerms {
		t := &required.NodeSelectorTerms[i]
		if len(t.MatchExpressions) == 0 && len(t.MatchFields) == 0 {
			continue
		}
		t.MatchExpressions = append(t.MatchExpressions, reqs...)
	}
	return out
}

// acceleratorTolerations tolerates, for each extended resource the container asks for
// (nvidia.com/gpu, amd.com/gpu, aws.amazon.com/neuron, ...), the NoSchedule taint of the same
// key, which is how accelerator nodes are conventionally kept for the pods that use them.
func acceleratorTolerations(c *corev1.Container) []corev1.Toleration {
	var out []corev1.Toleration
	seen := map[corev1.ResourceName]bool{}
	for _, list := range []corev1.ResourceList{c.Resources.Requests, c.Resources.Limits} {
		for name := range list {
			if !isExtendedResource(name) || seen[name] {
				continue
			}
			seen[name] = true
			out = append(out, corev1.Toleration{
				Key: string(name), Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule,
			})
		}
	}
	slices.SortFunc(out, func(a, b corev1.Toleration) int { return strings.Compare(a.Key, b.Key) })
	return out
}

// isExtendedResource mirrors Kubernetes' IsExtendedResourceName (pkg/apis/core/v1/helper in
// k8s.io/kubernetes, not importable): a domain-qualified name outside the reserved
// kubernetes.io domain, not a quota name, and valid once given the quota prefix.
func isExtendedResource(name corev1.ResourceName) bool {
	n := string(name)
	if !strings.Contains(n, "/") || strings.Contains(n, corev1.ResourceDefaultNamespacePrefix) ||
		strings.HasPrefix(n, corev1.DefaultResourceRequestsPrefix) {
		return false
	}
	return len(validation.IsQualifiedName(corev1.DefaultResourceRequestsPrefix+n)) == 0
}

// reflectedLabels mark a copy (see reflect.go). Copies have no owner reference, so this label is
// what tells a copy from a target's own object of the same name, and what the sweep lists by.
// It is Nebula's own key on purpose: a shared one like app.kubernetes.io/managed-by could be set
// by something else.
func reflectedLabels() map[string]string {
	return map[string]string{reflectedLabel: labelTrue}
}

func isReflected(obj metav1.Object) bool {
	return obj.GetLabels()[reflectedLabel] == labelTrue
}

func ptrTo[T any](v T) *T { return &v }
