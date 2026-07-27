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
	"fmt"

	corev1 "k8s.io/api/core/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// NvidiaGPUResource is the extended-resource key a GPU count is expressed under.
// It mirrors the ecosystem-standard nvidia.com/gpu so a Pod's accelerator count
// drives both the scheduler's fit check (against the virtual node's advertised
// capacity) and provisioning from a single number.
const NvidiaGPUResource corev1.ResourceName = "nvidia.com/gpu"

// AcceleratorRequest reads a Pod's accelerator request: the TYPE from the
// AcceleratorTypeLabel and the COUNT from the container's nvidia.com/gpu
// resource. This is the single source of truth for the request grammar, shared
// by the placement controller and the provider adapters.
//
// It returns:
//   - ("", 0, nil)      no GPU type requested => a CPU-only workload.
//   - (type, count, nil) a GPU workload; count defaults to 1 when the type is
//     set but no nvidia.com/gpu resource is present, so authors can request a
//     single GPU with just the label.
//
// The type is returned verbatim (case is normalized downstream by the provider
// catalog's MapAccelerator). An error is returned only for a genuinely
// contradictory request: an explicit nvidia.com/gpu count with no GPU type.
func AcceleratorRequest(pod *corev1.Pod) (accelerator string, count int32, err error) {
	typ := pod.Labels[nebulav1alpha1.AcceleratorTypeLabel]
	n := gpuCount(pod)

	if typ == "" {
		if n > 0 {
			return "", 0, fmt.Errorf("nvidia.com/gpu=%d requested without a %s label", n, nebulav1alpha1.AcceleratorTypeLabel)
		}
		return "", 0, nil // CPU-only
	}
	if n == 0 {
		n = 1 // a GPU type with no explicit count means one GPU
	}
	return typ, n, nil
}

// gpuCount returns the largest nvidia.com/gpu quantity requested across the
// Pod's containers, preferring limits and falling back to requests (Kubernetes
// treats an extended resource's request and limit as equal, but a Pod may set
// only one). Returns 0 when no container asks for a GPU.
func gpuCount(pod *corev1.Pod) int32 {
	var max int64
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		if q, ok := c.Resources.Limits[NvidiaGPUResource]; ok {
			if v := q.Value(); v > max {
				max = v
			}
		} else if q, ok := c.Resources.Requests[NvidiaGPUResource]; ok {
			if v := q.Value(); v > max {
				max = v
			}
		}
	}
	return int32(max)
}
