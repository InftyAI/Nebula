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

// PodReservation reads the workload's CPU and memory RESERVATION in the units
// provider.PriceRequest quotes: fractional physical cores and MiB. Requests, falling
// back to limits, and 0 for either when neither is set — which a provider reads as
// "your default", so a priced 0 is a floor, not a claim that nothing was reserved.
//
// Reservation and not the limit, because a provider metering CPU/memory apart from the
// accelerator (Modal) bills what was held for the workload; a burstable Pod's ceiling is
// not what shows up on the invoice.
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

// reservedQty returns the container's request for name, falling back to its limit, and a
// zero quantity when it declares neither. By value, so the caller never holds a pointer
// into the Pod it was read from.
func reservedQty(c *corev1.Container, name corev1.ResourceName) resource.Quantity {
	if q, ok := c.Resources.Requests[name]; ok {
		return q
	}
	if q, ok := c.Resources.Limits[name]; ok {
		return q
	}
	return resource.Quantity{}
}
