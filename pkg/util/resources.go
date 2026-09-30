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

// Resources is a container's CPU (vCPUs) and memory (MiB). Zero means undeclared.
type Resources struct {
	CPU       float64
	MemoryMiB int
}

// PodResources returns the first container's requests and limits. A missing request falls
// back to the limit, as Kubernetes defaults it; a missing limit stays 0 (no cap). Memory
// rounds UP to whole MiB, so a sub-MiB size never becomes 0, i.e. unset.
//
// The FIRST container only, matching the single-workload-container shape the whole
// provisioning path assumes (see modal.sandboxSpecFromPod).
func PodResources(pod *corev1.Pod) (requests, limits Resources) {
	if pod == nil || len(pod.Spec.Containers) == 0 {
		return Resources{}, Resources{}
	}
	c := &pod.Spec.Containers[0]
	limits = Resources{
		CPU:       vCPUs(c.Resources.Limits[corev1.ResourceCPU]),
		MemoryMiB: ceilMiB(c.Resources.Limits[corev1.ResourceMemory]),
	}
	requests = limits
	if q, ok := c.Resources.Requests[corev1.ResourceCPU]; ok {
		requests.CPU = vCPUs(q)
	}
	if q, ok := c.Resources.Requests[corev1.ResourceMemory]; ok {
		requests.MemoryMiB = ceilMiB(q)
	}
	return requests, limits
}

// PodReservation is the size a Pod is priced at: its limit, else its request. Modal bills
// the greater of reservation and usage, so a limit bounds the bill and the price is an
// upper bound. A request-only Pod has no bound and may bill above its price.
func PodReservation(pod *corev1.Pod) (cpuCores float64, memoryMiB int) {
	requests, limits := PodResources(pod)
	cpuCores, memoryMiB = limits.CPU, limits.MemoryMiB
	if cpuCores == 0 {
		cpuCores = requests.CPU
	}
	if memoryMiB == 0 {
		memoryMiB = requests.MemoryMiB
	}
	return cpuCores, memoryMiB
}

func vCPUs(q resource.Quantity) float64 { return float64(q.MilliValue()) / 1000.0 }

func ceilMiB(q resource.Quantity) int { return int((q.Value() + mibBytes - 1) / mibBytes) }
