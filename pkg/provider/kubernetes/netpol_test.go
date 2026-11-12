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
	"errors"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

var hostCluster = ClusterConfig{
	Name: "eks-eun1", Provider: ClusterProviderEKS, Region: "eu-north-1", Geography: "eu",
	CIDRs: []string{"10.20.0.0/16"}, Host: true,
}

const hostNamespaceUID = types.UID("org-1-uid")

func newHostKube() *kubefake.Clientset {
	return kubefake.NewClientset(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-1", UID: hostNamespaceUID}})
}

func newHostDynamic(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{clusterNetworkPolicies: "ClusterNetworkPolicyList"}, objs...)
}

// isolatedProvider provisions into eks-use1 with the fence on, the host being host.
func isolatedProvider(
	target testCluster, hostKube *kubefake.Clientset, hostDyn *dynamicfake.FakeDynamicClient, host ClusterConfig,
) *Provider {
	p := newTestProvider(map[string]clusterClients{"eks-use1": target.clients()})
	p.isolation = &isolation{isolationConfig: isolationConfig{host: host}, hostKube: hostKube, hostDynamic: hostDyn}
	return p
}

func getPolicy(t *testing.T, kube *kubefake.Clientset, name string) (*networkingv1.NetworkPolicy, bool) {
	t.Helper()
	np, err := kube.NetworkingV1().NetworkPolicies("org-1").Get(context.Background(), name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, false
	}
	if err != nil {
		t.Fatal(err)
	}
	return np, true
}

func getHostAccess(t *testing.T, dyn *dynamicfake.FakeDynamicClient) *unstructured.Unstructured {
	t.Helper()
	got, err := dyn.Resource(clusterNetworkPolicies).Get(context.Background(), hostAccessName("org-1"),
		metav1.GetOptions{})
	if err != nil {
		t.Fatalf("host ClusterNetworkPolicy: %v", err)
	}
	return got
}

func TestProvisionFencesNamespace(t *testing.T) {
	target, hostKube, hostDyn := newTestCluster(t, true), newHostKube(), newHostDynamic()
	p := isolatedProvider(target, hostKube, hostDyn, hostCluster)
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}

	cnp := getHostAccess(t, hostDyn)
	if tier, _, _ := unstructured.NestedString(cnp.Object, "spec", "tier"); tier != "Admin" {
		t.Errorf("tier = %q, want Admin: a namespaced policy would merge with the namespace's own", tier)
	}
	subject, _, _ := unstructured.NestedStringMap(cnp.Object, "spec", "subject", "namespaces", "matchLabels")
	if subject[corev1.LabelMetadataName] != "org-1" {
		t.Errorf("subject = %v, want namespace org-1", subject)
	}
	if prio, _, _ := unstructured.NestedInt64(cnp.Object, "spec", "priority"); prio != hostAccessPriority {
		t.Errorf("priority = %d, want %d, before the chart's default deny", prio, hostAccessPriority)
	}
	rules, _, _ := unstructured.NestedSlice(cnp.Object, "spec", "egress")
	// Pass, so org-1's own NetworkPolicies decide these packets as for its local pods: an
	// Accept is final and would let a pod they deny egress reach its remote pods.
	if action := rules[0].(map[string]any)["action"]; action != "Pass" {
		t.Errorf("action = %v, want Pass", action)
	}
	to := rules[0].(map[string]any)["to"].([]any)[0].(map[string]any)["pods"].(map[string]any)
	toNS, _, _ := unstructured.NestedStringMap(to, "namespaceSelector", "matchLabels")
	toPods, _, _ := unstructured.NestedStringMap(to, "podSelector", "matchLabels")
	if toNS[corev1.LabelMetadataName] != "org-1" || toPods[nebulav1alpha1.EnabledLabel] != nebulav1alpha1.EnabledValue {
		t.Errorf("passed peers = %v / %v, want org-1's own Nebula pods", toNS, toPods)
	}
	if owners := cnp.GetOwnerReferences(); len(owners) != 1 || owners[0].UID != hostNamespaceUID {
		t.Errorf("owners = %+v, want the host namespace, so it goes with it", owners)
	}
	list, err := hostKube.NetworkingV1().NetworkPolicies("org-1").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 0 {
		t.Errorf("host namespace policies = %v, %v; want none: they would merge with the namespace's own", list, err)
	}

	fence, err := target.dynamic.Resource(clusterNetworkPolicies).Get(context.Background(),
		targetFenceName("org-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("target ClusterNetworkPolicy: %v", err)
	}
	// Admin, so a broader NetworkPolicy in the target namespace cannot reopen it to another one.
	if tier, _, _ := unstructured.NestedString(fence.Object, "spec", "tier"); tier != "Admin" {
		t.Errorf("target fence tier = %q, want Admin", tier)
	}
	fenced, _, _ := unstructured.NestedStringMap(fence.Object, "spec", "subject", "pods", "podSelector", "matchLabels")
	if fenced[nebulav1alpha1.ManagedByLabel] != nebulav1alpha1.ManagedByValue {
		t.Errorf("target fence selects %v, want only Nebula's pods", fenced)
	}
	in, _, _ := unstructured.NestedSlice(fence.Object, "spec", "ingress")
	if len(in) != 2 || in[0].(map[string]any)["action"] != "Pass" || in[1].(map[string]any)["action"] != "Deny" {
		t.Fatalf("target fence ingress = %v, want Pass for org-1, then Deny for every other namespace", in)
	}
	own, _, _ := unstructured.NestedStringMap(in[0].(map[string]any)["from"].([]any)[0].(map[string]any),
		"namespaces", "matchLabels")
	others, _, _ := unstructured.NestedMap(in[1].(map[string]any)["from"].([]any)[0].(map[string]any), "namespaces")
	if own[corev1.LabelMetadataName] != "org-1" || len(others) != 0 {
		t.Errorf("target fence peers = %v then %v, want org-1 then every namespace", own, others)
	}
	owners := fence.GetOwnerReferences()
	if len(owners) != 1 || owners[0].Kind != "Namespace" || owners[0].Name != "org-1" {
		t.Errorf("target fence owners = %+v, want the target namespace", owners)
	}

	ingress, ok := getPolicy(t, target.kube, targetIngressPolicy)
	if !ok {
		t.Fatal("no ingress policy in the target namespace")
	}
	if ingress.Spec.PodSelector.MatchLabels[nebulav1alpha1.ManagedByLabel] != nebulav1alpha1.ManagedByValue {
		t.Errorf("ingress selects %v, want only Nebula's pods", ingress.Spec.PodSelector)
	}
	if from := ingress.Spec.Ingress[1].From; len(from) != 1 || from[0].IPBlock.CIDR != "10.20.0.0/16" {
		t.Errorf("ingress from = %+v, want the host's cidrs", from)
	}
}

// The host is a target too, and its real pods get the target-side fence like any other, or a pod
// in another cluster could reach a sandbox placed on the host. Both select only Nebula's pods,
// so the host's own workloads in the namespace are left alone.
func TestProvisionOnHostFencesItsRealPods(t *testing.T) {
	target, hostDyn := newTestCluster(t, true), newHostDynamic()
	host := ClusterConfig{Name: "eks-use1", Region: "us-east-1", CIDRs: []string{"10.20.0.0/16"}, Host: true}
	p := isolatedProvider(target, target.kube, hostDyn, host)
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}
	getHostAccess(t, hostDyn)
	ingress, ok := getPolicy(t, target.kube, targetIngressPolicy)
	if !ok {
		t.Fatal("no ingress policy for the host's real pods")
	}
	if ingress.Spec.PodSelector.MatchLabels[nebulav1alpha1.ManagedByLabel] != nebulav1alpha1.ManagedByValue {
		t.Errorf("ingress selects %v, want only Nebula's pods, not the host's own workloads", ingress.Spec.PodSelector)
	}
	fence, err := target.dynamic.Resource(clusterNetworkPolicies).Get(context.Background(),
		targetFenceName("org-1"), metav1.GetOptions{})
	if err != nil {
		t.Fatalf("target fence on the host: %v", err)
	}
	fenced, _, _ := unstructured.NestedStringMap(fence.Object, "spec", "subject", "pods", "podSelector", "matchLabels")
	if fenced[nebulav1alpha1.ManagedByLabel] != nebulav1alpha1.ManagedByValue {
		t.Errorf("target fence selects %v, want only Nebula's pods", fenced)
	}
}

func TestProvisionRepairsDriftedPolicy(t *testing.T) {
	drifted := hostAccess(&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "org-1", UID: hostNamespaceUID}})
	if err := unstructured.SetNestedField(drifted.Object, "Baseline", "spec", "tier"); err != nil { // a hand edit
		t.Fatal(err)
	}
	target, hostDyn := newTestCluster(t, true), newHostDynamic(drifted)
	p := isolatedProvider(target, newHostKube(), hostDyn, hostCluster)
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}
	if tier, _, _ := unstructured.NestedString(getHostAccess(t, hostDyn).Object, "spec", "tier"); tier != "Admin" {
		t.Fatalf("tier = %q, want the fence put back", tier)
	}
}

func TestProvisionWithoutIsolationWritesNoPolicy(t *testing.T) {
	target := newTestCluster(t, true)
	p := newTestProvider(map[string]clusterClients{"eks-use1": target.clients()})
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err != nil {
		t.Fatal(err)
	}
	list, err := target.kube.NetworkingV1().NetworkPolicies("org-1").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(list.Items) != 0 {
		t.Fatalf("policies = %v, %v; want none", list, err)
	}
}

// A pod must never run unfenced under a table that asks for isolation.
func TestProvisionFailsWhenFenceCannotBeWritten(t *testing.T) {
	target, hostDyn := newTestCluster(t, true), newHostDynamic()
	forbidden := apierrors.NewForbidden(clusterNetworkPolicies.GroupResource(), hostAccessName("org-1"),
		errors.New("rbac"))
	hostDyn.PrependReactor("create", "clusternetworkpolicies", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, forbidden
	})
	p := isolatedProvider(target, newHostKube(), hostDyn, hostCluster)
	if _, err := p.Provision(context.Background(), placeholder(), provisionRequest()); err == nil {
		t.Fatal("Provision succeeded without its host policy")
	}
	pods, err := target.kube.CoreV1().Pods("org-1").List(context.Background(), metav1.ListOptions{})
	if err != nil || len(pods.Items) != 0 {
		t.Fatalf("pods = %v, %v; want none created", pods, err)
	}
}

func TestConfigIsolation(t *testing.T) {
	target := ClusterConfig{
		Name: "t", Provider: ClusterProviderEKS, Region: "us-east-1", Geography: "us", CIDRs: []string{"10.21.0.0/16"},
	}
	cases := map[string]struct {
		cfg     Config
		wantErr string
	}{
		"on":             {cfg: Config{Clusters: []ClusterConfig{hostCluster, target}}},
		"off":            {cfg: Config{Clusters: []ClusterConfig{withCIDRs(withHost(target))}}},
		"cidrs, no host": {cfg: Config{Clusters: []ClusterConfig{target}}, wantErr: "marked host"},
		"two hosts":      {cfg: Config{Clusters: []ClusterConfig{hostCluster, withHost(target)}}, wantErr: "at most one"},
		"half the cidrs": {
			cfg:     Config{Clusters: []ClusterConfig{hostCluster, withCIDRs(target)}},
			wantErr: "on every cluster",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}

	if iso := (&Config{Clusters: []ClusterConfig{withCIDRs(target)}}).isolation(); iso != nil {
		t.Errorf("isolation without cidrs = %+v, want off", iso)
	}
	iso := (&Config{Clusters: []ClusterConfig{hostCluster, target}}).isolation()
	if iso == nil || iso.host.Name != "eks-eun1" {
		t.Fatalf("isolation = %+v, want the host", iso)
	}
	for ip, ok := range map[string]bool{"10.20.7.201": true, "10.21.0.9": false, "": true} {
		if err := iso.checkHost(ip); (err == nil) != ok {
			t.Errorf("checkHost(%q) = %v, want ok=%v", ip, err, ok)
		}
	}
}

func withHost(c ClusterConfig) ClusterConfig {
	c.Host = true
	return c
}
