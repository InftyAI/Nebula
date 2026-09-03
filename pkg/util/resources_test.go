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
)

func podWith(reqs, limits corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name:      "main",
		Resources: corev1.ResourceRequirements{Requests: reqs, Limits: limits},
	}}}}
}

func TestPodReservation(t *testing.T) {
	cases := map[string]struct {
		pod     *corev1.Pod
		wantCPU float64
		wantMiB int
		whatFor string
	}{
		"requests win over limits": {
			pod: podWith(
				corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")},
				corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")},
			),
			wantCPU: 2, wantMiB: 4096,
			whatFor: "a burstable Pod is billed for what it reserved, not its ceiling",
		},
		"falls back to limits": {
			pod: podWith(nil,
				corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("8"), corev1.ResourceMemory: resource.MustParse("16Gi")},
			),
			wantCPU: 8, wantMiB: 16384,
			whatFor: "Kubernetes defaults the request to the limit",
		},
		"fractional cores": {
			pod: podWith(corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse("250m"),
				corev1.ResourceMemory: resource.MustParse("512Mi"),
			}, nil),
			wantCPU: 0.25, wantMiB: 512,
			whatFor: "millicores are the common way to express a fraction",
		},
		"decimal memory units convert to MiB": {
			pod:     podWith(corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("1G")}, nil),
			wantCPU: 0, wantMiB: 953, // 1e9 / 1048576, truncated
			whatFor: "1G is not 1Gi, and the price is quoted per GiB",
		},
		"nothing declared": {
			pod:     podWith(nil, nil),
			wantCPU: 0, wantMiB: 0,
			whatFor: "0 reaches the provider as its own default",
		},
		"no containers": {
			pod:     &corev1.Pod{},
			wantCPU: 0, wantMiB: 0,
			whatFor: "must not panic on a Pod nothing has filled in yet",
		},
		"nil pod": {
			pod:     nil,
			wantCPU: 0, wantMiB: 0,
			whatFor: "callers hold a Pod that may be absent",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			cpu, mib := PodReservation(tc.pod)
			if cpu != tc.wantCPU || mib != tc.wantMiB {
				t.Fatalf("PodReservation = (%v cores, %v MiB), want (%v, %v): %s",
					cpu, mib, tc.wantCPU, tc.wantMiB, tc.whatFor)
			}
		})
	}
}

// Only the first container counts, matching the single-workload-container shape the
// provisioning path assumes; a sidecar must not inflate the priced reservation.
func TestPodReservation_FirstContainerOnly(t *testing.T) {
	pod := podWith(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}, nil)
	pod.Spec.Containers = append(pod.Spec.Containers, corev1.Container{
		Name: "sidecar",
		Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
			corev1.ResourceCPU: resource.MustParse("32"),
		}},
	})

	if cpu, _ := PodReservation(pod); cpu != 1 {
		t.Fatalf("PodReservation cpu = %v, want 1 (the first container's)", cpu)
	}
}
