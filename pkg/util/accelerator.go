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
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
)

// acceleratorSep joins the accelerator type and count in the canonical
// "type:count" identity (e.g. "H100:8").
const acceleratorSep = ":"

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

// AcceleratorPool is the canonical identity of the accelerator POOL a request
// targets: the type and count joined as "type:count" (e.g. "H100:8"). It is the
// key the failover blocklist records/queries and the value the NodeClaim reports,
// chosen over the provider's resolved SKU id because a single launch may span
// several interchangeable instance types (AWS's fleet) — the pool identity stays
// truthful whichever alternate lands, and it keeps distinct (type, count) pairs on
// distinct keys so an H100:8 shortage never disqualifies H100:1. Returns "" for a
// CPU-only request (empty type), which has no accelerator pool.
func AcceleratorPool(accelerator string, count int32) string {
	if accelerator == "" {
		return ""
	}
	return fmt.Sprintf("%s%s%d", accelerator, acceleratorSep, count)
}

// SplitAcceleratorPool is the inverse of AcceleratorPool: it recovers the type and count
// from a "type:count" pool identity. For readers that hold only the joined form —
// NodeClaimSpec.Accelerator — and need the two apart, as metrics labels do so both
// "every H100 size" and "every 8-GPU request" stay aggregatable.
//
// ("", 0) for an empty pool (a CPU-only claim) and for anything not in the grammar, so a
// hand-edited claim degrades to "no accelerator" rather than minting a garbage label value.
func SplitAcceleratorPool(pool string) (accelerator string, count int32) {
	typ, n, found := strings.Cut(pool, acceleratorSep)
	if !found || typ == "" {
		return "", 0
	}
	parsed, err := strconv.ParseInt(n, 10, 32)
	if err != nil || parsed <= 0 {
		return "", 0
	}
	return typ, int32(parsed)
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
