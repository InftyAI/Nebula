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

import corev1 "k8s.io/api/core/v1"

// IsTerminalPodPhase reports whether a Pod phase is an end state that will not progress.
//
// Shared because the two readers must agree: the control plane reaps and un-gates on a terminal
// Pod, while the virtual node refuses to move one (a provider still listing the instance would
// otherwise walk a Failed Pod back to Running). A predicate that drifted between them would
// resurrect exactly the Pods the other side had written off.
func IsTerminalPodPhase(phase corev1.PodPhase) bool {
	return phase == corev1.PodFailed || phase == corev1.PodSucceeded
}
