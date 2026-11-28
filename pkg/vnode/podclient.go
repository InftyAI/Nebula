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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
)

// statusPods is the PodClient virtual-kubelet's pod controller writes through. Its UpdateStatus
// writes only the status; every other call goes to the client as is.
//
// virtual-kubelet (through at least v1.14) pushes a status by calling UpdateStatus on the
// provider's copy of the whole Pod, and the pods/status endpoint keeps that copy's labels and
// annotations: it resets only spec, deletionTimestamp and ownerReferences. The copy dates from
// CreatePod, so the push removed anything added since, a ReplicaSet's
// controller.kubernetes.io/pod-deletion-cost among them. Nebula's own annotations are written
// by patchMeta, not by the push.
type statusPods struct{ corev1client.PodsGetter }

func (s statusPods) Pods(namespace string) corev1client.PodInterface {
	return statusPodInterface{s.PodsGetter.Pods(namespace)}
}

type statusPodInterface struct{ corev1client.PodInterface }

// UpdateStatus implements corev1client.PodInterface by patching the status subresource.
func (p statusPodInterface) UpdateStatus(
	ctx context.Context, pod *corev1.Pod, opts metav1.UpdateOptions,
) (*corev1.Pod, error) {
	patch, err := statusPatch(pod)
	if err != nil {
		return nil, err
	}
	return p.Patch(ctx, pod.Name, types.JSONPatchType, patch, metav1.PatchOptions{
		DryRun: opts.DryRun, FieldManager: opts.FieldManager, FieldValidation: opts.FieldValidation,
	}, "status")
}

type jsonPatchOp struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value any    `json:"value"`
}

// statusPatch replaces the Pod's status with pod's. A JSON patch, not a merge patch, so a field
// the new status leaves empty is cleared, as the update cleared it. Testing the UID first keeps
// the update's guarantee that a late push never lands on a Pod recreated under the same name;
// a Pod with no UID (GetPod's re-adoption stub) is not tested, as an update would not test it
// either.
func statusPatch(pod *corev1.Pod) ([]byte, error) {
	ops := make([]jsonPatchOp, 0, 2)
	if pod.UID != "" {
		ops = append(ops, jsonPatchOp{Op: "test", Path: "/metadata/uid", Value: pod.UID})
	}
	ops = append(ops, jsonPatchOp{Op: "replace", Path: "/status", Value: pod.Status})
	patch, err := json.Marshal(ops)
	if err != nil {
		return nil, fmt.Errorf("marshal the status patch for pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return patch, nil
}
