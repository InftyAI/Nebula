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

package util

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// acceleratorPod builds a Pod carrying an accelerator-type label (when non-empty)
// and a single container requesting the given nvidia.com/gpu count (when > 0).
func acceleratorPod(typ string, gpuLimit int64) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
	}
	if typ != "" {
		pod.Labels = map[string]string{nebulav1alpha1.AcceleratorTypeLabel: typ}
	}
	if gpuLimit > 0 {
		pod.Spec.Containers[0].Resources.Limits = corev1.ResourceList{
			NvidiaGPUResource: *resource.NewQuantity(gpuLimit, resource.DecimalSI),
		}
	}
	return pod
}

func TestAcceleratorRequest(t *testing.T) {
	cases := []struct {
		name      string
		typ       string
		gpuLimit  int64
		wantAccel string
		wantCount int32
		wantErr   bool
	}{
		{name: "no label is CPU-only", typ: "", gpuLimit: 0, wantAccel: "", wantCount: 0},
		{name: "type and count", typ: "a100-40gb", gpuLimit: 8, wantAccel: "a100-40gb", wantCount: 8},
		{name: "type only defaults to 1", typ: "h100", gpuLimit: 0, wantAccel: "h100", wantCount: 1},
		{name: "casing preserved", typ: "A100-80GB", gpuLimit: 2, wantAccel: "A100-80GB", wantCount: 2},
		{name: "count without type is contradictory", typ: "", gpuLimit: 4, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			accel, count, err := AcceleratorRequest(acceleratorPod(tc.typ, tc.gpuLimit))
			if tc.wantErr {
				if err == nil {
					t.Fatalf("AcceleratorRequest = (%q, %d, nil), want error", accel, count)
				}
				return
			}
			if err != nil {
				t.Fatalf("AcceleratorRequest: unexpected error %v", err)
			}
			if accel != tc.wantAccel || count != tc.wantCount {
				t.Fatalf("AcceleratorRequest = (%q, %d), want (%q, %d)",
					accel, count, tc.wantAccel, tc.wantCount)
			}
		})
	}
}

// TestAcceleratorRequest_CountFromRequestsWhenNoLimit verifies the fallback to
// resource requests when a container sets only requests (not limits).
func TestAcceleratorRequest_CountFromRequestsWhenNoLimit(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{nebulav1alpha1.AcceleratorTypeLabel: "h100"}},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				NvidiaGPUResource: *resource.NewQuantity(3, resource.DecimalSI),
			}},
		}}},
	}
	accel, count, err := AcceleratorRequest(pod)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if accel != "h100" || count != 3 {
		t.Fatalf("AcceleratorRequest = (%q, %d), want (h100, 3)", accel, count)
	}
}
