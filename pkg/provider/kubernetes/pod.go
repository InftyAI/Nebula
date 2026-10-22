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
	"crypto/sha256"
	"encoding/hex"
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
	// maxNameLen is the object-name cap. The real pod is named after the claim, which
	// util.ClaimName keeps within it; only the Secret's suffix can push past it.
	maxNameLen      = 253
	nameHashLen     = 10
	envSecretSuffix = "-env"

	labelTrue = "true"
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

func ownedByUs(obj metav1.Object) bool {
	for k, v := range ownerLabels() {
		if obj.GetLabels()[k] != v {
			return false
		}
	}
	return true
}

// envSecretName is the env Secret's name for the real pod podName, with a hash of podName in
// place of its tail when the suffix would pass maxNameLen, so two long names never meet.
func envSecretName(podName string) string {
	if len(podName)+len(envSecretSuffix) <= maxNameLen {
		return podName + envSecretSuffix
	}
	sum := sha256.Sum256([]byte(podName))
	keep := maxNameLen - len(envSecretSuffix) - nameHashLen - 1
	return podName[:keep] + "-" + hex.EncodeToString(sum[:])[:nameHashLen] + envSecretSuffix
}

// podSpec is everything buildPod needs that is not on the placeholder.
type podSpec struct {
	claim     string
	selection nodeSelection
	// env is ProvisionRequest.Env.Config, set inline; hasSecrets says Env.Secrets is
	// non-empty, so the pod loads the env Secret holding it.
	env        map[string]string
	hasSecrets bool
	// costLabels are the placeholder's --cost-labels keys, copied so cost attribution follows
	// the sandbox to the real pod. Nothing else is: a controller label such as
	// batch.kubernetes.io/controller-uid would let the placeholder's Job adopt the real pod,
	// since both share a namespace.
	costLabels []string
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
	for _, k := range s.costLabels {
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

	// References to objects that live in the home cluster. The webhook admits no volume
	// but the service-account token, which is simply not mounted here.
	spec.ServiceAccountName, spec.DeprecatedServiceAccount = "", ""
	spec.AutomountServiceAccountToken = ptrTo(false)
	spec.Volumes = nil
	spec.ImagePullSecrets = nil
	spec.ResourceClaims = nil
	spec.SchedulerName = ""
	spec.Overhead = nil

	c := &spec.Containers[0]
	c.VolumeMounts = nil
	// The caller resolved every reference, so nothing here points back at the home cluster.
	// Plain values go inline; Secret-sourced ones go in a Secret, since a pod spec is
	// readable with `get pod`. The two never share a key, so precedence is moot.
	c.Env = envVars(s.env)
	c.EnvFrom = nil
	if s.hasSecrets {
		c.EnvFrom = []corev1.EnvFromSource{{
			SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: envSecretName(name)}},
		}}
	}

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
// Required terms are ORed, so the requirements go into every one of them; preferences and
// pod (anti-)affinity are left as the user wrote them.
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
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		out.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution = ourRequired.DeepCopy()
		return out
	}
	for i := range required.NodeSelectorTerms {
		required.NodeSelectorTerms[i].MatchExpressions = append(required.NodeSelectorTerms[i].MatchExpressions, reqs...)
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

// envVars renders env in sorted order, so the same request always yields the same spec. Each
// "$" is doubled because the kubelet expands $(VAR) in env values (and folds "$$" to "$"):
// the values are already resolved and must reach the container verbatim, as resolveEnv
// promises, not pick up another variable such as one from the env Secret.
func envVars(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	out := make([]corev1.EnvVar, 0, len(env))
	for _, k := range slices.Sorted(maps.Keys(env)) {
		out = append(out, corev1.EnvVar{Name: k, Value: strings.ReplaceAll(env[k], "$", "$$")})
	}
	return out
}

// envSecret holds the Secret-sourced env (provider.Env.Secrets) for the real pod named podName.
func envSecret(namespace, podName string, env map[string]string) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      envSecretName(podName),
			Namespace: namespace,
			Labels:    ownerLabels(),
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: env,
	}
}

func ptrTo[T any](v T) *T { return &v }
