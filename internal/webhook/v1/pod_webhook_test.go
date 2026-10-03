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

package v1

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

func gated(pod *corev1.Pod) bool {
	return hasGate(pod, nebulav1alpha1.ProviderSelectionGate)
}

func podWith(labels map[string]string, nodeName string, gates ...string) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default", Labels: labels},
		Spec:       corev1.PodSpec{NodeName: nodeName},
	}
	for _, g := range gates {
		p.Spec.SchedulingGates = append(p.Spec.SchedulingGates, corev1.PodSchedulingGate{Name: g})
	}
	return p
}

func TestDefault_InjectsGateForOptedInPod(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "")

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if !gated(pod) {
		t.Fatal("expected provider-selection gate to be injected")
	}
	if len(pod.Spec.SchedulingGates) != 1 {
		t.Fatalf("expected exactly 1 gate, got %d", len(pod.Spec.SchedulingGates))
	}
}

func TestDefault_PreservesExistingGates(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "", "example.com/other-gate")

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if len(pod.Spec.SchedulingGates) != 2 {
		t.Fatalf("expected the existing gate to be kept alongside ours, got %d", len(pod.Spec.SchedulingGates))
	}
	if !gated(pod) {
		t.Fatal("expected provider-selection gate present")
	}
}

func TestDefault_SkipsWhenNotOptedIn(t *testing.T) {
	d := &PodCustomDefaulter{}
	cases := map[string]map[string]string{
		"no labels":      nil,
		"label absent":   {"other": "x"},
		"label not true": {nebulav1alpha1.EnabledLabel: "false"},
	}
	for name, labels := range cases {
		t.Run(name, func(t *testing.T) {
			pod := podWith(labels, "")
			if err := d.Default(context.Background(), pod); err != nil {
				t.Fatalf("Default: %v", err)
			}
			if gated(pod) {
				t.Fatal("expected no gate for a non-opted-in Pod")
			}
		})
	}
}

func TestDefault_SkipsAlreadyScheduledPod(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "node-1")

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if gated(pod) {
		t.Fatal("must not add a scheduling gate to an already-scheduled Pod")
	}
}

func TestDefault_IdempotentWhenGateAlreadyPresent(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "", nebulav1alpha1.ProviderSelectionGate)

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if len(pod.Spec.SchedulingGates) != 1 {
		t.Fatalf("expected gate not to be duplicated, got %d", len(pod.Spec.SchedulingGates))
	}
}

func tolerated(pod *corev1.Pod) bool {
	return hasProviderToleration(pod)
}

func TestDefault_InjectsProviderToleration(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "")

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if len(pod.Spec.Tolerations) != 1 {
		t.Fatalf("expected exactly 1 toleration, got %d", len(pod.Spec.Tolerations))
	}
	tol := pod.Spec.Tolerations[0]
	if tol.Key != nebulav1alpha1.ProviderLabel ||
		tol.Operator != corev1.TolerationOpExists ||
		tol.Effect != corev1.TaintEffectNoSchedule {
		t.Fatalf("unexpected toleration: %+v", tol)
	}
}

func TestDefault_DoesNotDuplicateProviderToleration(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "")
	pod.Spec.Tolerations = []corev1.Toleration{{
		Key:      nebulav1alpha1.ProviderLabel,
		Operator: corev1.TolerationOpExists,
		Effect:   corev1.TaintEffectNoSchedule,
	}}

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if len(pod.Spec.Tolerations) != 1 {
		t.Fatalf("expected the existing provider toleration to be kept without duplication, got %d", len(pod.Spec.Tolerations))
	}
}

func TestDefault_NoTolerationForNonOptedInPod(t *testing.T) {
	d := &PodCustomDefaulter{}
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "false"}, "")

	if err := d.Default(context.Background(), pod); err != nil {
		t.Fatalf("Default: %v", err)
	}
	if tolerated(pod) {
		t.Fatal("expected no provider toleration for a non-opted-in Pod")
	}
}

func TestDefault_RejectsNonPod(t *testing.T) {
	d := &PodCustomDefaulter{}
	if err := d.Default(context.Background(), &corev1.Service{}); err == nil {
		t.Fatal("expected an error for a non-Pod object")
	}
}

func TestValidateCreate_OneContainerOnePort(t *testing.T) {
	optedIn := map[string]string{nebulav1alpha1.EnabledLabel: "true"}
	container := func(name string, ports ...int32) corev1.Container {
		c := corev1.Container{Name: name}
		for _, p := range ports {
			c.Ports = append(c.Ports, corev1.ContainerPort{ContainerPort: p})
		}
		return c
	}
	cases := []struct {
		name       string
		labels     map[string]string
		containers []corev1.Container
		inits      []corev1.Container
		wantErr    string
	}{
		{"one container, one port", optedIn, []corev1.Container{container("main", 8080)}, nil, ""},
		{"one container, no port", optedIn, []corev1.Container{container("main")}, nil, ""},
		{"two ports", optedIn, []corev1.Container{container("main", 8080, 9090)}, nil, "at most one port"},
		{"two containers", optedIn, []corev1.Container{container("main", 8080), container("sidecar")}, nil, "exactly one container"},
		{"no container", optedIn, nil, nil, "exactly one container"},
		{"init container", optedIn, []corev1.Container{container("main")}, []corev1.Container{container("setup")}, "init containers"},
		// Pods outside Nebula are never Nebula's to judge, whatever the selector says.
		{"not opted in", nil, []corev1.Container{container("a", 1, 2), container("b")}, []corev1.Container{container("i")}, ""},
	}
	v := &PodCustomValidator{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := podWith(tc.labels, "")
			pod.Spec.Containers = tc.containers
			pod.Spec.InitContainers = tc.inits
			_, err := v.ValidateCreate(context.Background(), pod)
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("ValidateCreate err = %v, want nil", err)
			case tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)):
				t.Fatalf("ValidateCreate err = %v, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateCreate_RejectsNonPod(t *testing.T) {
	if _, err := (&PodCustomValidator{}).ValidateCreate(context.Background(), &corev1.Service{}); err == nil {
		t.Fatal("expected an error for a non-Pod object")
	}
}

// A pre-bound Pod skips placement (see needsPlacement), so no provider ever runs it and its
// shape is not Nebula's to judge, even when it carries the opt-in label.
func TestValidateCreate_SkipsPreBoundPod(t *testing.T) {
	pod := podWith(map[string]string{nebulav1alpha1.EnabledLabel: "true"}, "node-1")
	pod.Spec.Containers = []corev1.Container{
		{Name: "main", Ports: []corev1.ContainerPort{{ContainerPort: 8080}, {ContainerPort: 9090}}},
		{Name: "sidecar"},
	}
	pod.Spec.InitContainers = []corev1.Container{{Name: "setup"}}
	if _, err := (&PodCustomValidator{}).ValidateCreate(context.Background(), pod); err != nil {
		t.Fatalf("ValidateCreate err = %v, want nil for a pre-bound Pod", err)
	}
}

func TestValidateUpdate_JudgesOnlyTransitionsIntoOptedIn(t *testing.T) {
	optedIn := map[string]string{nebulav1alpha1.EnabledLabel: "true"}
	sidecarPod := func(labels map[string]string) *corev1.Pod {
		pod := podWith(labels, "", nebulav1alpha1.ProviderSelectionGate)
		pod.Spec.Containers = []corev1.Container{{Name: "main"}, {Name: "sidecar"}}
		return pod
	}
	cases := []struct {
		name     string
		old, new *corev1.Pod
		wantErr  bool
	}{
		// The bypass: created unlabelled (so no webhook ran) with the gate already set, then
		// relabelled, which would hand placement a Pod it cannot run.
		{"relabelled into opted-in", sidecarPod(nil), sidecarPod(optedIn), true},
		// An opted-in Pod admitted before this check existed: rejecting its later writes
		// would leave it gated forever.
		{"already opted in", sidecarPod(optedIn), sidecarPod(optedIn), false},
		{"still not opted in", sidecarPod(nil), sidecarPod(map[string]string{"other": "x"}), false},
		{"opting out", sidecarPod(optedIn), sidecarPod(nil), false},
	}
	v := &PodCustomValidator{}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := v.ValidateUpdate(context.Background(), tc.old, tc.new)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ValidateUpdate err = %v, want error: %t", err, tc.wantErr)
			}
		})
	}
}
