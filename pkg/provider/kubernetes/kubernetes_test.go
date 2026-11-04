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
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubefake "k8s.io/client-go/kubernetes/fake"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

type fakeCatalog struct{ rows []provider.Offering }

func (c fakeCatalog) Offerings(string) []provider.Offering { return c.rows }

var testCatalog = fakeCatalog{rows: []provider.Offering{
	{AcceleratorType: "H100", AcceleratorID: "p5.4xlarge", GPUCount: 1},
	{AcceleratorType: "H100", AcceleratorID: "p5.48xlarge", GPUCount: 8},
	{AcceleratorType: "L4", AcceleratorID: "g6.xlarge", GPUCount: 1},
	{AcceleratorType: "L4", AcceleratorID: "gr6.4xlarge", GPUCount: 1},
}}

var testClusters = []ClusterConfig{
	{Name: "eks-use1", Provider: ClusterProviderEKS, Region: "us-east-1", Geography: "us"},
	{Name: "eks-eun1", Provider: ClusterProviderEKS, Region: "eu-north-1", Geography: "eu"},
	{Name: "eks-usw2", Provider: ClusterProviderEKS, Region: "us-west-2", Geography: "us"},
}

// newTestProvider fills any cluster clients leaves out with an empty fake one.
func newTestProvider(clients map[string]clusterClients) *Provider {
	all := make(map[string]clusterClients, len(testClusters))
	for _, cl := range testClusters {
		cc, ok := clients[cl.Name]
		if !ok {
			cc = clusterClients{kube: kubefake.NewClientset()}
		}
		all[cl.Name] = cc
	}
	return New(context.Background(), testClusters, all, testCatalog, []string{"org-id"})
}

// placeholder is a Pod as Nebula's webhook and placement leave it.
func placeholder() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "work-1", Namespace: "org-1",
			Labels: map[string]string{
				nebulav1alpha1.EnabledLabel:          nebulav1alpha1.EnabledValue,
				nebulav1alpha1.AcceleratorTypeLabel:  "h100",
				"org-id":                             "o1",
				"batch.kubernetes.io/controller-uid": "job-uid",
			},
		},
		Spec: corev1.PodSpec{
			NodeName:      "nebula-kubernetes",
			NodeSelector:  map[string]string{nebulav1alpha1.ProviderLabel: provider.ProviderKubernetes},
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{{
				Name: "main", Image: "img:1", Command: []string{"run"},
				Resources: corev1.ResourceRequirements{
					Requests: corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse("1")},
					Limits:   corev1.ResourceList{util.NvidiaGPUResource: resource.MustParse("1")},
				},
			}},
			Volumes: []corev1.Volume{{
				Name:         "kube-api-access-abcde",
				VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}},
			}},
		},
	}
}

func TestConfigValidate(t *testing.T) {
	ok := ClusterConfig{Name: "a", Provider: "eks", Region: "us-east-1", Geography: "us"}
	cases := map[string]struct {
		clusters []ClusterConfig
		wantErr  string
	}{
		"valid":     {clusters: []ClusterConfig{ok}},
		"empty":     {wantErr: "no clusters"},
		"duplicate": {clusters: []ClusterConfig{ok, ok}, wantErr: "duplicate"},
		"unsupported": {
			clusters: []ClusterConfig{{Name: "a", Provider: "gke", Region: "r"}},
			wantErr:  "unsupported provider",
		},
		"missing": {clusters: []ClusterConfig{{Name: "a", Provider: "eks"}}, wantErr: "required"},
		"region twice": {
			clusters: []ClusterConfig{ok, {Name: "b", Provider: "eks", Region: "us-east-1", Geography: "us"}},
			wantErr:  "a region holds one cluster",
		},
		"geography": {
			clusters: []ClusterConfig{{Name: "a", Provider: "eks", Region: "us-east-1", Geography: "usa"}},
			wantErr:  "geography",
		},
		"node autoscaler": {
			clusters: []ClusterConfig{{
				Name: "a", Provider: "eks", Region: "r", Geography: "us", NodeAutoscaler: "ca",
			}},
			wantErr: "unsupported node autoscaler",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := (&Config{Clusters: tc.clusters}).Validate()
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.wantErr)
			}
		})
	}
}

func TestEnvSecretName(t *testing.T) {
	if got := envSecretName("org-1-work-1"); got != "org-1-work-1-env" {
		t.Fatalf("envSecretName = %q", got)
	}
	long := strings.Repeat("a", maxNameLen)
	a, b := envSecretName(long), envSecretName(long[:maxNameLen-1]+"b")
	if len(a) > maxNameLen || !strings.HasSuffix(a, envSecretSuffix) {
		t.Fatalf("envSecretName = %q (%d chars)", a, len(a))
	}
	if a == b {
		t.Fatal("two long names collided")
	}
}

func TestBuildPod(t *testing.T) {
	got := buildPod(placeholder(), podSpec{
		claim:      "org-1-work-1",
		selection:  karpenter{}.selection([]string{"p5"}, nebulav1alpha1.CapacitySpot),
		costLabels: []string{"org-id"},
		env:        map[string]string{"B": "2", "A": "1"},
		hasSecrets: true,
	})
	if got.Name != "org-1-work-1" || got.Namespace != "org-1" {
		t.Fatalf("got %s/%s", got.Namespace, got.Name)
	}
	if got.Labels["org-id"] != "o1" || !ownedByUs(got) {
		t.Fatalf("labels = %v", got.Labels)
	}
	notCopied := []string{
		nebulav1alpha1.EnabledLabel, "batch.kubernetes.io/controller-uid", nebulav1alpha1.AcceleratorTypeLabel,
	}
	for _, k := range notCopied {
		if _, ok := got.Labels[k]; ok {
			t.Errorf("label %q was copied", k)
		}
	}
	if got.Spec.NodeName != "" || len(got.Spec.NodeSelector) > 0 || len(got.Spec.Volumes) > 0 {
		t.Errorf("Nebula scheduling or volumes leaked: %+v", got.Spec)
	}
	if got.Annotations[doNotDisruptAnnotation] != "true" {
		t.Errorf("annotations = %v", got.Annotations)
	}
	c := got.Spec.Containers[0]
	if len(c.EnvFrom) != 1 || c.EnvFrom[0].SecretRef.Name != "org-1-work-1-env" {
		t.Errorf("envFrom = %+v, want the env Secret", c.EnvFrom)
	}
	if want := []corev1.EnvVar{{Name: "A", Value: "1"}, {Name: "B", Value: "2"}}; !slices.Equal(c.Env, want) {
		t.Errorf("env = %+v, want plain values inline and sorted", c.Env)
	}
	noSecrets := buildPod(placeholder(), podSpec{claim: "org-1-work-1"})
	if len(noSecrets.Spec.Containers[0].EnvFrom) != 0 {
		t.Error("with no Secrets there is no env Secret to load")
	}
	terms := got.Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	reqs := terms[0].MatchExpressions
	want := map[string]string{instanceFamilyLabel: "p5", capacityTypeLabel: capacityTypeSpot}
	for _, r := range reqs {
		if want[r.Key] != strings.Join(r.Values, ",") {
			t.Errorf("affinity %s = %v", r.Key, r.Values)
		}
		delete(want, r.Key)
	}
	if len(want) > 0 {
		t.Errorf("missing affinity %v", want)
	}
}

func TestMapAccelerator(t *testing.T) {
	p := newTestProvider(nil)
	if got, ok := p.MapAccelerator("h100", 8); !ok || !slices.Equal(got, []string{"p5"}) {
		t.Errorf("H100 = %v, %v", got, ok)
	}
	if got, ok := p.MapAccelerator("L4", 1); !ok || !slices.Equal(got, []string{"g6", "gr6"}) {
		t.Errorf("L4 = %v, %v", got, ok)
	}
	if _, ok := p.MapAccelerator("TPU", 1); ok {
		t.Error("TPU should be unsupported")
	}
}

func TestResolveRegions(t *testing.T) {
	p := newTestProvider(nil)
	cases := map[string]struct {
		declared, narrowTo, want []string
	}{
		"unconstrained":  {want: []string{"us-east-1", "eu-north-1", "us-west-2"}},
		"geography":      {declared: []string{"us"}, want: []string{"us-east-1", "us-west-2"}},
		"region":         {declared: []string{"eu-north-1"}, want: []string{"eu-north-1"}},
		"cluster name":   {declared: []string{"eks-usw2"}},
		"declared order": {declared: []string{"us-west-2", "us-east-1"}, want: []string{"us-west-2", "us-east-1"}},
		"narrowed":       {narrowTo: []string{"eu"}, want: []string{"eu-north-1"}},
		"narrowed out":   {declared: []string{"us-east-1"}, narrowTo: []string{"eu"}},
		"no match":       {declared: []string{"ap-south-1"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := p.ResolveRegions(tc.declared, tc.narrowTo); !slices.Equal(got, tc.want) {
				t.Fatalf("ResolveRegions = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyProvisionError(t *testing.T) {
	p := newTestProvider(nil)
	region := "us-east-1"

	if s := p.ClassifyProvisionError(nil, "H100:1", region); s != (provider.BlockScope{}) {
		t.Errorf("nil error blocked %+v", s)
	}

	spot := p.ClassifyProvisionError(provider.WithTier(nebulav1alpha1.CapacitySpot,
		fmt.Errorf("x: %w", provider.ErrNoCapacity)), "H100:1", region)
	want := provider.BlockScope{Deny: provider.DenyCandidate, Accelerator: "H100:1",
		CapacityType: nebulav1alpha1.CapacitySpot, Region: region}
	if spot != want {
		t.Errorf("spot shortage scope = %+v, want %+v", spot, want)
	}

	// One cluster's access entry or RBAC, so the region (cluster) alone, never the provider.
	auth := p.ClassifyProvisionError(classifyAPI(forbidden()), "H100:1", region)
	if auth != (provider.BlockScope{Deny: provider.DenyRegion, Region: region}) {
		t.Errorf("auth failure must block only the cluster, got %+v", auth)
	}
}

func forbidden() error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "x", fmt.Errorf("nope"))
}

func TestAcceleratorTolerations(t *testing.T) {
	c := corev1.Container{Resources: corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU:      resource.MustParse("4"),
			corev1.ResourceMemory:   resource.MustParse("8Gi"),
			"amd.com/gpu":           resource.MustParse("1"),
			"hugepages-2Mi":         resource.MustParse("1Gi"),
			"aws.amazon.com/neuron": resource.MustParse("1"),
		},
		Limits: corev1.ResourceList{"amd.com/gpu": resource.MustParse("1")},
	}}
	tols := acceleratorTolerations(&c)
	keys := make([]string, 0, len(tols))
	for _, tol := range tols {
		if tol.Operator != corev1.TolerationOpExists || tol.Effect != corev1.TaintEffectNoSchedule {
			t.Errorf("toleration %+v, want Exists/NoSchedule", tol)
		}
		keys = append(keys, tol.Key)
	}
	if want := []string{"amd.com/gpu", "aws.amazon.com/neuron"}; !slices.Equal(keys, want) {
		t.Fatalf("tolerated keys = %v, want %v", keys, want)
	}
	if got := acceleratorTolerations(&corev1.Container{}); len(got) != 0 {
		t.Fatalf("CPU-only container tolerates %v, want nothing", got)
	}
}

type catalogByProvider map[string][]provider.Offering

func (c catalogByProvider) Offerings(name string) []provider.Offering { return c[name] }

// Each cluster maps GPUs through its own cloud's catalog; the provider offers their union.
func TestFamiliesPerCluster(t *testing.T) {
	clusterCatalogs["l4cloud"] = "l4-only"
	t.Cleanup(func() { delete(clusterCatalogs, "l4cloud") })
	cat := catalogByProvider{
		provider.ProviderAWS: {{AcceleratorType: "H100", AcceleratorID: "p5.48xlarge", GPUCount: 8}},
		"l4-only":            {{AcceleratorType: "L4", AcceleratorID: "g6.xlarge", GPUCount: 1}},
	}
	clusters := []ClusterConfig{
		{Name: "eks-use1", Provider: ClusterProviderEKS, Region: "us-east-1", Geography: "us"},
		{Name: "l4", Provider: "l4cloud", Region: "us-east-2", Geography: "us"},
	}
	tc := newTestCluster(t, true)
	p := New(context.Background(), clusters, map[string]clusterClients{
		"eks-use1": {kube: kubefake.NewClientset()}, "l4": tc.clients(),
	}, cat, nil)

	if fs, ok := p.MapAccelerator("H100", 1); !ok || !slices.Equal(fs, []string{"p5"}) {
		t.Fatalf("MapAccelerator(H100) = %v, %v; the provider offers what any cluster has", fs, ok)
	}
	req := provider.ProvisionRequest{ClaimName: "org-1-work-1", Region: "us-east-2"}
	_, err := p.Provision(context.Background(), placeholder(), req)
	if !errors.Is(err, provider.ErrUnsupportedAccelerator) {
		t.Fatalf("Provision of H100 on the L4-only cluster = %v, want ErrUnsupportedAccelerator", err)
	}
}

// The real pod keeps what the user wrote and loses what is Nebula's or the home cluster's.
func TestBuildPodCopiesThenOverrides(t *testing.T) {
	local := placeholder()
	probe := &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: []string{"ready"}}}}
	local.Spec.Containers[0].ReadinessProbe = probe
	local.Spec.Containers[0].Env = []corev1.EnvVar{{Name: "SECRET", ValueFrom: &corev1.EnvVarSource{}}}
	local.Spec.SecurityContext = &corev1.PodSecurityContext{RunAsNonRoot: ptrTo(true)}
	local.Spec.Tolerations = []corev1.Toleration{
		{Key: nebulav1alpha1.ProviderLabel, Operator: corev1.TolerationOpExists},
		{Key: "team", Operator: corev1.TolerationOpEqual, Value: "ml"},
	}
	local.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: nebulav1alpha1.ProviderSelectionGate}}
	local.Spec.ServiceAccountName = "default"
	local.Spec.PriorityClassName, local.Spec.Priority = "high", ptrTo(int32(1000))
	zone := func(z string) corev1.NodeSelectorTerm {
		return corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{
			{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{z}},
		}}
	}
	local.Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{zone("a"), zone("b")},
		},
	}}

	got := buildPod(local, podSpec{
		claim: "org-1-work-1", selection: karpenter{}.selection([]string{"p5"}, nebulav1alpha1.CapacityOnDemand),
	})
	spec, c := got.Spec, got.Spec.Containers[0]

	if c.ReadinessProbe == nil || spec.SecurityContext == nil {
		t.Error("the user's probe and security context must carry over")
	}
	// The placeholder's env holds a reference into the home cluster; only the resolved env may
	// appear, and this test passes none.
	if c.Env != nil || len(c.EnvFrom) != 0 || len(c.VolumeMounts) != 0 || len(spec.Volumes) != 0 {
		t.Errorf("env/volumes leaked: env=%v envFrom=%v mounts=%v volumes=%v", c.Env, c.EnvFrom, c.VolumeMounts, spec.Volumes)
	}
	if spec.NodeName != "" || len(spec.SchedulingGates) != 0 || len(spec.NodeSelector) != 0 {
		t.Errorf("Nebula's scheduling leaked: node=%q gates=%v selector=%v",
			spec.NodeName, spec.SchedulingGates, spec.NodeSelector)
	}
	if spec.ServiceAccountName != "" {
		t.Error("the service account is the home cluster's and must be cleared")
	}
	keys := make([]string, 0, len(spec.Tolerations))
	for _, tol := range spec.Tolerations {
		keys = append(keys, tol.Key)
	}
	if slices.Contains(keys, nebulav1alpha1.ProviderLabel) || !slices.Contains(keys, "team") ||
		!slices.Contains(keys, string(util.NvidiaGPUResource)) {
		t.Errorf("tolerations = %v; want the user's and the GPU's, not the virtual node's", keys)
	}
	// Both of the user's ORed terms must also carry the instance family, or one of them
	// would let the pod land on any node.
	for i, term := range spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		if !slices.ContainsFunc(term.MatchExpressions, func(r corev1.NodeSelectorRequirement) bool {
			return r.Key == instanceFamilyLabel
		}) {
			t.Errorf("term %d lacks the instance family: %v", i, term.MatchExpressions)
		}
	}
	if local.Spec.NodeName != "nebula-kubernetes" || len(local.Spec.Tolerations) != 2 {
		t.Error("buildPod must not modify the placeholder")
	}
}

func TestIsExtendedResource(t *testing.T) {
	for name, want := range map[corev1.ResourceName]bool{
		"nvidia.com/gpu":          true,
		"amd.com/gpu":             true,
		"aws.amazon.com/neuron":   true,
		corev1.ResourceCPU:        false,
		corev1.ResourceMemory:     false,
		"hugepages-2Mi":           false,
		"kubernetes.io/batch-cpu": false, // the reserved domain is native
		"requests.nvidia.com/gpu": false, // a quota name, not a resource
		"example.com/has space":   false, // not a valid qualified name
	} {
		if got := isExtendedResource(name); got != want {
			t.Errorf("isExtendedResource(%q) = %v, want %v", name, got, want)
		}
	}
}

func TestToStateNeedsReady(t *testing.T) {
	running := func(ready corev1.ConditionStatus) *corev1.Pod {
		pod := &corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodRunning}}
		if ready != "" {
			pod.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodReady, Status: ready}}
		}
		return pod
	}
	for name, tc := range map[string]struct {
		pod  *corev1.Pod
		want provider.InstanceState
	}{
		"ready":            {running(corev1.ConditionTrue), provider.InstanceRunning},
		"probe failing":    {running(corev1.ConditionFalse), provider.InstanceNotReady},
		"no condition yet": {running(""), provider.InstanceNotReady},
		"terminating, still ready": {func() *corev1.Pod {
			pod := running(corev1.ConditionTrue)
			now := metav1.Now()
			pod.DeletionTimestamp = &now
			return pod
		}(), provider.InstanceNotReady},
		"unbound": {&corev1.Pod{Status: corev1.PodStatus{Phase: corev1.PodPending}}, provider.InstanceProvisioning},
		"bound, starting": {&corev1.Pod{
			Spec: corev1.PodSpec{NodeName: "node-1"}, Status: corev1.PodStatus{Phase: corev1.PodPending},
		}, provider.InstancePending},
	} {
		if got := toState(tc.pod); got != tc.want {
			t.Errorf("%s: toState = %s, want %s", name, got, tc.want)
		}
	}
}

// The kubelet expands $(VAR) and folds $$ in env values; resolved values must come out as
// they went in, so every $ is escaped.
func TestEnvVarsEscapeDollar(t *testing.T) {
	got := envVars(map[string]string{"REF": "$(TOKEN)", "TWO": "a$$b", "PLAIN": "x"})
	want := []corev1.EnvVar{{Name: "PLAIN", Value: "x"}, {Name: "REF", Value: "$$(TOKEN)"}, {Name: "TWO", Value: "a$$$$b"}}
	if !slices.Equal(got, want) {
		t.Fatalf("envVars = %+v, want %+v", got, want)
	}
}
