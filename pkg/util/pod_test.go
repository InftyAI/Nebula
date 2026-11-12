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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
)

func TestFinished(t *testing.T) {
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
		if got := Finished(phase); got != want {
			t.Errorf("Finished(%q) = %v, want %v", phase, got, want)
		}
	}
}

// Replicas share their controller's key, so one spec error blocks them together; a bare Pod
// is its own workload.
func TestWorkloadKey(t *testing.T) {
	owned := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ns", Name: "rs-x1",
		OwnerReferences: []metav1.OwnerReference{
			{Kind: "ConfigMap", Name: "not-controller"},
			{Kind: "ReplicaSet", Name: "rs", Controller: ptr.To(true)},
		},
	}}
	if got := WorkloadKey(owned); got != "ns/ReplicaSet/rs" {
		t.Errorf("owned Pod: %q, want its controller's", got)
	}
	if got := WorkloadKey(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "p"}}); got != "ns/Pod/p" {
		t.Errorf("bare Pod: %q, want its own", got)
	}
	// A StatefulSet keeps its name across template changes, so its revision tells them apart.
	stateful := func(rev string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns", Name: "sts-0",
			Labels:          map[string]string{appsv1.ControllerRevisionHashLabelKey: rev},
			OwnerReferences: []metav1.OwnerReference{{Kind: "StatefulSet", Name: "sts", Controller: ptr.To(true)}},
		}}
	}
	if got := WorkloadKey(stateful("sts-5d9f")); got != "ns/StatefulSet/sts/sts-5d9f" {
		t.Errorf("StatefulSet Pod: %q, want its controller's and revision", got)
	}
	if WorkloadKey(stateful("sts-5d9f")) == WorkloadKey(stateful("sts-7c1a")) {
		t.Error("two revisions of one StatefulSet share a key, so a fixed one stays blocked")
	}
}
