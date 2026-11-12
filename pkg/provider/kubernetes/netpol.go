/*
Copyright 2026.

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

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// Namespace isolation: a host pod in namespace X reaches only X's remote pods, and a remote
// pod in X admits only X. The targets see a host address but not its namespace, so each end
// filters what it can tell apart:
//
//   - on the host, the chart's Admin ClusterNetworkPolicy denies every namespace the targets'
//     ranges, after an Admin one per namespace (hostAccess, at hostAccessPriority) passes X's
//     traffic to X's own Nebula pods on to X's NetworkPolicies, as for its local pods. Their
//     placeholders carry the remote pods' IPs, so the CNI resolves the selector to exactly
//     X's remote pods. The deny takes the ranges, not the pods, so it fails closed: a remote
//     pod has its IP before its placeholder shows it, keeps it after the placeholder is gone,
//     and is covered even when Nebula cannot publish it. Pass, not Accept: an Admin Accept is
//     final, so a pod X's own policy denies egress would reach them anyway. Not a
//     NetworkPolicy either: allows add up, so it would widen every pod X's own policies
//     confine. Admin both, as Baseline yields to any NetworkPolicy allow and would let a
//     namespace's own allow-all egress cross the boundary.
//   - in the target, the host included, an Admin ClusterNetworkPolicy per namespace
//     (targetFence) passes X's own traffic to X's Nebula pods on to X's NetworkPolicies and
//     denies every other namespace's, which no NetworkPolicy can reopen: allows add up, so a
//     namespaced fence alone gives way to any broader policy there. Its ingress peers cannot
//     be CIDRs, so traffic from outside the cluster, the host's included, is left to the
//     namespaced policy (targetIngress), which admits only the host's range unless such a
//     broader policy widens it.
//
// It is on whenever the table gives the clusters cidrs. Every policy is written before the
// real pod exists, and a failure fails the Provision: a pod must never run unfenced then.
const (
	targetIngressPolicy = "nebula-isolation-target"
	// hostAccessPriority and targetFencePriority are shared by every namespace's policy: their
	// subjects never overlap.
	hostAccessPriority  = 500
	targetFencePriority = 500
)

// clusterNetworkPolicies is AWS VPC CNI's cluster-scoped, tiered policy.
var clusterNetworkPolicies = schema.GroupVersionResource{
	Group: "networking.k8s.aws", Version: "v1alpha1", Resource: "clusternetworkpolicies",
}

// isolationConfig is Config.isolation resolved for one table; isolation adds the host's clients.
type isolationConfig struct {
	host ClusterConfig
}

type isolation struct {
	isolationConfig
	hostKube    kubeClient
	hostDynamic dynamic.Interface
}

// fence writes both ends' policies for a pod placed in namespace of target. The host as target
// gets its target-side fence too: it selects only Nebula's real pods, so the host's own
// workloads in the namespace are unaffected, and without it nothing stops another cluster's
// pods reaching a sandbox placed on the host.
func (iso *isolation) fence(ctx context.Context, target cluster, namespace string) error {
	ns, err := iso.hostKube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("kubernetes: get host namespace %q: %w", namespace, classifyAPI(err))
	}
	if err := ensureClusterNetworkPolicy(ctx, iso.hostDynamic, hostAccess(ns)); err != nil {
		return err
	}
	tns, err := target.clients.kube.CoreV1().Namespaces().Get(ctx, namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("kubernetes: get namespace %q in %s: %w", namespace, target.cfg.Name, classifyAPI(err))
	}
	if err := ensureClusterNetworkPolicy(ctx, target.clients.dynamic, targetFence(tns)); err != nil {
		return err
	}
	return ensureNetworkPolicy(ctx, target.clients.kube, targetIngress(namespace, iso.host.CIDRs))
}

func targetFenceName(namespace string) string { return "nebula-isolation-target-" + namespace }

// targetFence is owned by the target namespace, so it goes when the namespace does. Its rules
// are matched in order: the namespace's own pods first, then every other namespace.
func targetFence(ns *corev1.Namespace) *unstructured.Unstructured {
	inNamespace := selector(corev1.LabelMetadataName, ns.Name)
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": clusterNetworkPolicies.GroupVersion().String(),
		"kind":       "ClusterNetworkPolicy",
		"spec": map[string]any{
			"tier":     "Admin",
			"priority": int64(targetFencePriority),
			"subject": map[string]any{"pods": map[string]any{
				"namespaceSelector": inNamespace,
				"podSelector":       selector(nebulav1alpha1.ManagedByLabel, nebulav1alpha1.ManagedByValue),
			}},
			"ingress": []any{
				rule("own-namespace", "Pass", "from", namespaces(inNamespace)),
				rule("other-namespaces", "Deny", "from", namespaces(map[string]any{})),
			},
		},
	}}
	u.SetName(targetFenceName(ns.Name))
	u.SetLabels(map[string]string{nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue})
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: ns.Name, UID: ns.UID}})
	return u
}

func hostAccessName(namespace string) string { return "nebula-isolation-host-" + namespace }

// hostAccess is owned by the namespace, so it goes when the namespace does.
func hostAccess(ns *corev1.Namespace) *unstructured.Unstructured {
	inNamespace := selector(corev1.LabelMetadataName, ns.Name)
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": clusterNetworkPolicies.GroupVersion().String(),
		"kind":       "ClusterNetworkPolicy",
		"spec": map[string]any{
			"tier":     "Admin",
			"priority": int64(hostAccessPriority),
			"subject":  namespaces(inNamespace),
			"egress": []any{rule("own-nebula-pods", "Pass", "to", map[string]any{"pods": map[string]any{
				"namespaceSelector": inNamespace,
				"podSelector":       selector(nebulav1alpha1.EnabledLabel, nebulav1alpha1.EnabledValue),
			}})},
		},
	}}
	u.SetName(hostAccessName(ns.Name))
	u.SetLabels(map[string]string{nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue})
	u.SetOwnerReferences([]metav1.OwnerReference{{APIVersion: "v1", Kind: "Namespace", Name: ns.Name, UID: ns.UID}})
	return u
}

// selector, namespaces and rule build the pieces of a ClusterNetworkPolicy, which has no Go types.
func selector(key, value string) map[string]any {
	return map[string]any{"matchLabels": map[string]any{key: value}}
}

func namespaces(sel map[string]any) map[string]any { return map[string]any{"namespaces": sel} }

// rule is one ingress (peersKey "from") or egress ("to") rule.
func rule(name, action, peersKey string, peers ...any) map[string]any {
	return map[string]any{"name": name, "action": action, peersKey: peers}
}

// targetIngress selects only Nebula's pods, so a namespace that already existed in the target
// keeps whatever it had.
func targetIngress(namespace string, hostCIDRs []string) *networkingv1.NetworkPolicy {
	from := make([]networkingv1.NetworkPolicyPeer, 0, len(hostCIDRs))
	for _, s := range hostCIDRs {
		from = append(from, networkingv1.NetworkPolicyPeer{IPBlock: &networkingv1.IPBlock{CIDR: s}})
	}
	return &networkingv1.NetworkPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: targetIngressPolicy, Namespace: namespace,
			Labels: map[string]string{nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue},
		},
		Spec: networkingv1.NetworkPolicySpec{
			PodSelector: metav1.LabelSelector{MatchLabels: map[string]string{
				nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
			}},
			PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress},
			Ingress: []networkingv1.NetworkPolicyIngressRule{
				{From: []networkingv1.NetworkPolicyPeer{{PodSelector: &metav1.LabelSelector{}}}},
				{From: from},
			},
		},
	}
}

// ensureNetworkPolicy and ensureClusterNetworkPolicy create want, or put its spec back if it
// drifted: the cluster table, not a hand edit, is what a policy Nebula owns says.
func ensureNetworkPolicy(ctx context.Context, kube kubeClient, want *networkingv1.NetworkPolicy) error {
	api := kube.NetworkingV1().NetworkPolicies(want.Namespace)
	got, err := api.Get(ctx, want.Name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := api.Create(ctx, want, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("kubernetes: create NetworkPolicy %s/%s: %w", want.Namespace, want.Name, classifyAPI(err))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("kubernetes: get NetworkPolicy %s/%s: %w", want.Namespace, want.Name, classifyAPI(err))
	}
	if equality.Semantic.DeepEqual(got.Spec, want.Spec) {
		return nil
	}
	got.Spec = want.Spec
	if _, err := api.Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("kubernetes: update NetworkPolicy %s/%s: %w", want.Namespace, want.Name, classifyAPI(err))
	}
	return nil
}

func ensureClusterNetworkPolicy(ctx context.Context, dyn dynamic.Interface, want *unstructured.Unstructured) error {
	api := dyn.Resource(clusterNetworkPolicies)
	got, err := api.Get(ctx, want.GetName(), metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		if _, err := api.Create(ctx, want, metav1.CreateOptions{}); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("kubernetes: create ClusterNetworkPolicy %s: %w", want.GetName(), classifyAPI(err))
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("kubernetes: get ClusterNetworkPolicy %s: %w", want.GetName(), classifyAPI(err))
	}
	if equality.Semantic.DeepEqual(got.Object["spec"], want.Object["spec"]) {
		return nil
	}
	got.Object["spec"] = want.Object["spec"]
	if _, err := api.Update(ctx, got, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("kubernetes: update ClusterNetworkPolicy %s: %w", want.GetName(), classifyAPI(err))
	}
	return nil
}
