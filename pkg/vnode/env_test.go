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
	"maps"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

// envPod is a Pod whose single container carries the given env sources.
func envPod(envFrom []corev1.EnvFromSource, env []corev1.EnvVar) *corev1.Pod {
	pod := testPod("default", "p1")
	pod.Spec.Containers[0].EnvFrom = envFrom
	pod.Spec.Containers[0].Env = env
	return pod
}

func secretObj(name string, data map[string]string) *corev1.Secret {
	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Data:       map[string][]byte{},
	}
	for k, v := range data {
		s.Data[k] = []byte(v)
	}
	return s
}

func configMapObj(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
		Data:       data,
	}
}

func ptrBool(b bool) *bool { return &b }

// TestResolveEnv_Precedence pins the kubelet's ordering: envFrom in listed order, each later
// source overwriting the earlier, then the explicit env list beating all of them. Getting it
// backwards looks like a working deployment until a value comes from the wrong place.
func TestResolveEnv_Precedence(t *testing.T) {
	client := fake.NewSimpleClientset(
		configMapObj("cm", map[string]string{"SHARED": "from-cm", "ONLY_CM": "cm"}),
		secretObj("sec", map[string]string{"SHARED": "from-secret", "ONLY_SECRET": "sec"}),
	)
	pod := envPod(
		[]corev1.EnvFromSource{
			{ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}}},
			{SecretRef: &corev1.SecretEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}}},
		},
		[]corev1.EnvVar{{Name: "SHARED", Value: "explicit"}, {Name: "PLAIN", Value: "p"}},
	)

	got, err := resolveEnv(context.Background(), client, pod)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	want := map[string]string{
		"SHARED":      "explicit", // env beats both envFrom sources
		"ONLY_CM":     "cm",       // envFrom configMapRef
		"ONLY_SECRET": "sec",      // envFrom secretRef
		"PLAIN":       "p",        // literal
	}
	if !maps.Equal(got, want) {
		t.Fatalf("env mismatch\n got: %v\nwant: %v", got, want)
	}
}

// TestResolveEnv_Prefix checks each source's Prefix is prepended, which is the only way two
// ConfigMaps with the same keys can both be consumed.
func TestResolveEnv_Prefix(t *testing.T) {
	client := fake.NewSimpleClientset(configMapObj("cm", map[string]string{"KEY": "v"}))
	pod := envPod([]corev1.EnvFromSource{{
		Prefix:       "APP_",
		ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}},
	}}, nil)

	got, err := resolveEnv(context.Background(), client, pod)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if got["APP_KEY"] != "v" || len(got) != 1 {
		t.Fatalf("expected only APP_KEY=v, got %v", got)
	}
}

// TestResolveEnv_SecretKeyRef covers the whole secretKeyRef matrix, since these four cases
// are what separate "wait for the Secret" from "boot a GPU without its token".
func TestResolveEnv_SecretKeyRef(t *testing.T) {
	client := fake.NewSimpleClientset(secretObj("sec", map[string]string{"TOKEN": "t0ken"}))
	ref := func(name, k string, optional bool) []corev1.EnvVar {
		return []corev1.EnvVar{{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: name},
				Key:                  k,
				Optional:             ptrBool(optional),
			},
		}}}
	}

	cases := []struct {
		name    string
		env     []corev1.EnvVar
		want    string // "" means the variable must be unset
		wantErr string // substring; "" means no error
	}{
		{name: "resolved", env: ref("sec", "TOKEN", false), want: "t0ken"},
		{name: "missing secret, required", env: ref("nope", "TOKEN", false), wantErr: `Secret "nope" not found`},
		{name: "missing secret, optional", env: ref("nope", "TOKEN", true)},
		{name: "missing key, required", env: ref("sec", "OTHER", false), wantErr: `key "OTHER" not in Secret "sec"`},
		{name: "missing key, optional", env: ref("sec", "OTHER", true)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveEnv(context.Background(), client, envPod(nil, tc.env))
			switch {
			case tc.wantErr != "":
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("expected error containing %q, got %v", tc.wantErr, err)
				}
				// A failed resolve must yield NOTHING: a partial map handed to a provider
				// would boot an instance with half its configuration.
				if got != nil {
					t.Fatalf("expected no env on error, got %v", got)
				}
			case err != nil:
				t.Fatalf("resolveEnv: %v", err)
			case got["TOKEN"] != tc.want:
				t.Fatalf("TOKEN = %q, want %q", got["TOKEN"], tc.want)
			}
		})
	}
}

// TestResolveEnv_ReadFailureIsNeverOptional pins what `optional` does NOT license: it says
// the object may not exist, not that we may fail to look. A transport failure must fail the
// resolve even for an optional ref, or an API outage would silently strip credentials.
func TestResolveEnv_ReadFailureIsNeverOptional(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("apiserver is having a day")
	})
	pod := envPod([]corev1.EnvFromSource{{
		SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "sec"},
			Optional:             ptrBool(true),
		},
	}}, nil)

	if _, err := resolveEnv(context.Background(), client, pod); err == nil {
		t.Fatal("expected a read failure to fail the resolve even though the ref is optional")
	}
}

// TestResolveEnv_MemoizesReads asserts a Pod referencing one Secret many times costs one GET:
// at fleet scale that is one read per Pod instead of one per variable.
func TestResolveEnv_MemoizesReads(t *testing.T) {
	client := fake.NewSimpleClientset(secretObj("sec", map[string]string{"A": "1", "B": "2"}))
	gets := 0
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		return false, nil, nil // fall through to the tracker
	})

	keyRef := func(varName, k string) corev1.EnvVar {
		return corev1.EnvVar{Name: varName, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: k,
			},
		}}
	}
	pod := envPod(
		[]corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}}}},
		[]corev1.EnvVar{keyRef("X", "A"), keyRef("Y", "B")},
	)

	got, err := resolveEnv(context.Background(), client, pod)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if gets != 1 {
		t.Fatalf("expected 1 Secret GET for 3 references, got %d", gets)
	}
	if got["X"] != "1" || got["Y"] != "2" || got["A"] != "1" {
		t.Fatalf("unexpected env: %v", got)
	}
}

// TestResolveEnv_MemoizesMisses is the same guarantee for an absent object: several optional
// references to one missing Secret must not re-ask per variable.
func TestResolveEnv_MemoizesMisses(t *testing.T) {
	client := fake.NewSimpleClientset()
	gets := 0
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		gets++
		return false, nil, nil
	})
	optRef := func(varName string) corev1.EnvVar {
		return corev1.EnvVar{Name: varName, ValueFrom: &corev1.EnvVarSource{
			SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "gone"},
				Key:                  "K",
				Optional:             ptrBool(true),
			},
		}}
	}

	got, err := resolveEnv(context.Background(), client, envPod(nil, []corev1.EnvVar{optRef("A"), optRef("B")}))
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if gets != 1 {
		t.Fatalf("expected 1 GET for 2 optional refs to the same missing Secret, got %d", gets)
	}
	if len(got) != 0 {
		t.Fatalf("expected no variables set, got %v", got)
	}
}

// TestResolveEnv_FieldRef covers the downward-API subset a virtual node can answer, and the
// refusal of the rest. status.podIP is the one that matters: answering it with a placeholder
// would make a workload advertise an address nothing can reach.
func TestResolveEnv_FieldRef(t *testing.T) {
	pod := envPod(nil, nil)
	pod.UID = "uid-1"
	pod.Labels["app"] = "vllm"
	pod.Annotations = map[string]string{"team": "infra"}
	pod.Spec.NodeName = "nebula-fake"
	pod.Spec.ServiceAccountName = "sa"

	cases := []struct {
		path    string
		want    string
		wantErr bool
	}{
		{path: "metadata.name", want: "p1"},
		{path: "metadata.namespace", want: "default"},
		{path: "metadata.uid", want: "uid-1"},
		{path: "spec.nodeName", want: "nebula-fake"},
		{path: "spec.serviceAccountName", want: "sa"},
		{path: "metadata.labels['app']", want: "vllm"},
		{path: "metadata.annotations['team']", want: "infra"},
		// Absent label: the empty string, as on a real kubelet — the PATH is supported.
		{path: "metadata.labels['nope']", want: ""},
		{path: "status.podIP", wantErr: true},
		{path: "metadata.labels", wantErr: true}, // whole-map form is volume-only
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			got, err := fieldRefValue(pod, &corev1.ObjectFieldSelector{FieldPath: tc.path})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected %q to be refused, got %q", tc.path, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("fieldRefValue(%q): %v", tc.path, err)
			}
			if got != tc.want {
				t.Fatalf("fieldRefValue(%q) = %q, want %q", tc.path, got, tc.want)
			}
		})
	}
}

// TestResolveEnv_ResourceFieldRefRefused pins the refusal: this node advertises synthetic
// capacity (virtualCapacity), so the kubelet's "default an unset limit to the node's
// allocatable" rule would hand a workload a number no instance has.
func TestResolveEnv_ResourceFieldRefRefused(t *testing.T) {
	pod := envPod(nil, []corev1.EnvVar{{Name: "MEM", ValueFrom: &corev1.EnvVarSource{
		ResourceFieldRef: &corev1.ResourceFieldSelector{Resource: "limits.memory"},
	}}})

	_, err := resolveEnv(context.Background(), fake.NewSimpleClientset(), pod)
	if err == nil || !strings.Contains(err.Error(), "limits.memory") {
		t.Fatalf("expected a resourceFieldRef refusal naming the resource, got %v", err)
	}
	// The variable name belongs in the message too: a Pod with several refs needs to know
	// which one to fix.
	if !strings.Contains(err.Error(), `env "MEM"`) {
		t.Fatalf("expected the error to name the variable, got %v", err)
	}
}

// TestResolveEnv_DropsIllegalNames matches the kubelet: a ConfigMap consumed wholesale
// carries whatever keys it was written with, and dropping an unusable one beats failing the
// Pod over it. The legal keys in the same source must still arrive — including "app.conf",
// since Kubernetes' env-name rule permits dots even though C_IDENTIFIER does not.
func TestResolveEnv_DropsIllegalNames(t *testing.T) {
	client := fake.NewSimpleClientset(configMapObj("cm", map[string]string{
		"1_LEADING_DIGIT": "ignored", "GOOD": "v", "app.conf": "legal-here",
	}))
	pod := envPod([]corev1.EnvFromSource{{
		ConfigMapRef: &corev1.ConfigMapEnvSource{LocalObjectReference: corev1.LocalObjectReference{Name: "cm"}},
	}}, nil)

	got, err := resolveEnv(context.Background(), client, pod)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if _, bad := got["1_LEADING_DIGIT"]; bad {
		t.Fatalf("expected the illegal name to be dropped, got %v", got)
	}
	if got["GOOD"] != "v" || got["app.conf"] != "legal-here" {
		t.Fatalf("expected the legal keys to survive, got %v", got)
	}
}

// TestResolveEnv_NilClient documents the test seam: with no cluster to read, literals still
// resolve and references are skipped rather than erroring.
func TestResolveEnv_NilClient(t *testing.T) {
	pod := envPod(
		[]corev1.EnvFromSource{{SecretRef: &corev1.SecretEnvSource{
			LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}}}},
		[]corev1.EnvVar{
			{Name: "PLAIN", Value: "p"},
			{Name: "REF", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: "K"}}},
		},
	)

	got, err := resolveEnv(context.Background(), nil, pod)
	if err != nil {
		t.Fatalf("resolveEnv: %v", err)
	}
	if !maps.Equal(got, map[string]string{"PLAIN": "p"}) {
		t.Fatalf("expected literals only, got %v", got)
	}
}

// TestCreatePod_PassesResolvedEnvToProvider is the contract between the two halves: the
// virtual node resolves, the provider receives values.
//
// It also pins what must NOT happen — the Pod's spec keeps its reference. VK compares
// Spec.Containers between the API server's Pod and the one GetPod returns (podsEqual), so a
// rewritten env list would look like a spec change on every sync; and it would put Secret
// values in an object this package copies, emits and patches.
func TestCreatePod_PassesResolvedEnvToProvider(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	pod := envPod(nil, []corev1.EnvVar{
		{Name: "PLAIN", Value: "p"},
		{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "sec"}, Key: "K"}}},
	})
	client := fake.NewSimpleClientset(pod, secretObj("sec", map[string]string{"K": "t0ken"}))
	h := NewHandler(fp, client, nil, openCluster())

	if err := h.CreatePod(context.Background(), pod); err != nil {
		t.Fatalf("CreatePod: %v", err)
	}
	if !maps.Equal(fp.lastReq.Env, map[string]string{"PLAIN": "p", "TOKEN": "t0ken"}) {
		t.Fatalf("provider got env %v", fp.lastReq.Env)
	}
	if vf := pod.Spec.Containers[0].Env[1].ValueFrom; vf == nil || vf.SecretKeyRef == nil {
		t.Fatal("the Pod's env must keep its reference; resolution belongs on the request")
	}
}

// TestCreatePod_UnresolvableEnvIsNonTerminal is why resolution runs before the provider call.
// A referenced Secret often lands moments after the Pod, so this behaves like the kubelet's
// CreateContainerConfigError: nothing provisioned, nothing blocklisted, the Pod waiting at
// ConfigError, and the error returned for VK to retry.
func TestCreatePod_UnresolvableEnvIsNonTerminal(t *testing.T) {
	fp := &fakeProvider{provisionID: "inst-1"}
	bl := &recordingBlocklist{}
	pod := envPod(nil, []corev1.EnvVar{{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{
		SecretKeyRef: &corev1.SecretKeySelector{
			LocalObjectReference: corev1.LocalObjectReference{Name: "not-yet"}, Key: "K"},
	}}})
	client := fake.NewSimpleClientset(pod)
	h := NewHandler(fp, client, bl, openCluster())

	err := h.CreatePod(context.Background(), pod)
	if err == nil {
		t.Fatal("expected CreatePod to fail so VK retries the sync")
	}
	if fp.provisionCnt != 0 {
		t.Fatalf("expected no provision attempt, got %d", fp.provisionCnt)
	}
	if bl.calls != 0 {
		t.Fatalf("a Pod-spec problem must not blocklist a candidate, got %d records", bl.calls)
	}
	if pod.Status.Phase != corev1.PodPending || pod.Status.Reason != reasonConfigError {
		t.Fatalf("expected Pending/%s, got %s/%s", reasonConfigError, pod.Status.Phase, pod.Status.Reason)
	}
	// Untracked, like every other pre-instance failure: a tracked pod with no instance id
	// reads as absent from List and gets written Terminated.
	if h.Tracks(pod.Namespace, pod.Name) {
		t.Fatal("a pod that never reached the provider must not be tracked")
	}
}

// TestResolveEnv_NoEnvIsNil keeps the common case allocation-free and the request field nil,
// so a provider can tell "nothing to set" from "an empty environment".
func TestResolveEnv_NoEnvIsNil(t *testing.T) {
	got, err := resolveEnv(context.Background(), fake.NewSimpleClientset(), testPod("default", "p1"))
	if err != nil || got != nil {
		t.Fatalf("expected (nil, nil) for a Pod with no env, got (%v, %v)", got, err)
	}
}
