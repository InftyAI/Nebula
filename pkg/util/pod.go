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
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Finished reports whether a Pod phase is final (Succeeded or Failed). Shared so the control
// plane, which reaps finished Pods, and the virtual node, which never moves one back, agree.
func Finished(phase corev1.PodPhase) bool {
	return phase == corev1.PodFailed || phase == corev1.PodSucceeded
}

// WorkloadKey identifies the workload a Pod belongs to, as namespace/Kind/name of its
// controller, else of the Pod itself, so a changed template is a new workload. A Deployment's
// gets a new ReplicaSet, named after the template's hash; a StatefulSet or DaemonSet keeps its
// name, so the revision it stamps on its Pods is part of their key. A recreated Pod of the
// same name and revision is the same workload.
func WorkloadKey(pod *corev1.Pod) string {
	kind, name := "Pod", pod.Name
	if ref := metav1.GetControllerOf(pod); ref != nil {
		kind, name = ref.Kind, ref.Name
	}
	key := pod.Namespace + "/" + kind + "/" + name
	if rev := pod.Labels[appsv1.ControllerRevisionHashLabelKey]; rev != "" {
		key += "/" + rev
	}
	return key
}
