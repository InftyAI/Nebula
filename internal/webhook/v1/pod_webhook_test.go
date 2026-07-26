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
