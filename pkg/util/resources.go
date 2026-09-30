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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// mibBytes is one MiB, the unit provider.PriceRequest quotes memory in.
const mibBytes = 1024 * 1024

// PodReservation returns the workload's CPU (vCPUs) and memory (MiB): limits, else
// requests, else 0 (the provider's default). The limit wins because Modal bills the greater
// of reservation and usage; it provisions this value as both request and limit, so the
// price is exact.
//
// Memory below 1 MiB truncates to 0 (Modal's default, uncapped), so its cost cannot be
// tracked correctly. Accepted: no working Pod declares one.
//
// The FIRST container only, matching the single-workload-container shape the whole
// provisioning path assumes (see modal.sandboxSpecFromPod). Returns (0, 0) for a Pod with
// no containers.
func PodReservation(pod *corev1.Pod) (cpuCores float64, memoryMiB int) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return 0, 0
	}
	c := &pod.Spec.Containers[0]
	cpu := reservedQty(c, corev1.ResourceCPU)
	mem := reservedQty(c, corev1.ResourceMemory)
	// MilliValue is cores*1000; Value is bytes.
	return float64(cpu.MilliValue()) / 1000.0, int(mem.Value() / mibBytes)
}

// reservedQty returns the container's limit for name, falling back to its request, and a
// zero quantity when it declares neither. By value, so the caller never holds a pointer
// into the Pod it was read from.
func reservedQty(c *corev1.Container, name corev1.ResourceName) resource.Quantity {
	if q, ok := c.Resources.Limits[name]; ok {
		return q
	}
	if q, ok := c.Resources.Requests[name]; ok {
		return q
	}
	return resource.Quantity{}
}
