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
)

func TestIsTerminalPodPhase(t *testing.T) {
	// Pending is the one worth stating: a Pod that has not started yet is not terminal, and
	// treating it as one would reap a workload mid-provision.
	for phase, want := range map[corev1.PodPhase]bool{
		corev1.PodFailed:    true,
		corev1.PodSucceeded: true,
		corev1.PodRunning:   false,
		corev1.PodPending:   false,
		corev1.PodUnknown:   false,
		"":                  false,
	} {
		if got := IsTerminalPodPhase(phase); got != want {
			t.Errorf("IsTerminalPodPhase(%q) = %v, want %v", phase, got, want)
		}
	}
}
