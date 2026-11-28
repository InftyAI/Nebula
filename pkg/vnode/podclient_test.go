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

package vnode

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

const deletionCost = "controller.kubernetes.io/pod-deletion-cost"

// livePod is the Pod as the API server holds it, after a controller marked it.
func livePod(uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "p1", UID: uid,
			Labels:      map[string]string{"app": "trainer", "added": "later"},
			Annotations: map[string]string{deletionCost: "100"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodPending, Reason: "Provisioning", Message: "queued"},
	}
}

// providerCopy is the handler's copy, taken before the mark, with a newer status.
func providerCopy(uid types.UID) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default", Name: "p1", UID: uid,
			Labels: map[string]string{"app": "trainer"},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Reason: "Running"},
	}
}

func TestStatusPods_WritesOnlyTheStatus(t *testing.T) {
	for name, uid := range map[string]types.UID{"with a UID": "uid-1", "re-adoption stub": ""} {
		t.Run(name, func(t *testing.T) {
			kube := fake.NewClientset(livePod("uid-1"))
			pods := statusPods{kube.CoreV1()}.Pods("default")

			if _, err := pods.UpdateStatus(context.Background(), providerCopy(uid), metav1.UpdateOptions{}); err != nil {
				t.Fatalf("UpdateStatus: %v", err)
			}
			got, err := kube.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
			if err != nil {
				t.Fatal(err)
			}
			if got.Annotations[deletionCost] != "100" || got.Labels["added"] != "later" {
				t.Errorf("metadata = %v %v; the push removed what was added after the copy", got.Labels, got.Annotations)
			}
			// Replaced, not merged: the message the new status leaves empty is cleared.
			if got.Status.Phase != corev1.PodRunning || got.Status.Reason != "Running" || got.Status.Message != "" {
				t.Errorf("status = %+v, want the copy's", got.Status)
			}
		})
	}
}

func TestStatusPods_RefusesAPushForARecreatedPod(t *testing.T) {
	kube := fake.NewClientset(livePod("uid-2"))
	pods := statusPods{kube.CoreV1()}.Pods("default")

	if _, err := pods.UpdateStatus(context.Background(), providerCopy("uid-1"), metav1.UpdateOptions{}); err == nil {
		t.Fatal("a push for the earlier Pod of this name was accepted")
	}
	got, err := kube.CoreV1().Pods("default").Get(context.Background(), "p1", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status.Phase != corev1.PodPending {
		t.Errorf("phase = %s; the recreated Pod's status was overwritten", got.Status.Phase)
	}
}
