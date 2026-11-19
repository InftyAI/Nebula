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
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/rest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/InftyAI/Nebula/pkg/util"
)

// statusManager is the field manager Nebula applies Pod status under. An apply owns only the
// fields it sends, so a condition someone else adds (DisruptionTarget, on eviction) survives,
// and one Nebula stops sending is removed.
const statusManager = "nebula-vnode"

// vkFieldManager is the name the API server recorded VK's status writes under: the client sets
// no FieldManager or UserAgent, so the server takes the default user agent's first segment.
var vkFieldManager = strings.SplitN(rest.DefaultKubernetesUserAgent(), "/", 2)[0]

// vkProvider is the handler as VK sees it, with its status callback discarded. Nebula writes
// Pod status itself (see persistStatus): VK's write sent its copy of the whole Pod, taken at
// CreatePod, and the status endpoint stores that copy's annotations too, erasing any added
// since. Without the callback VK never holds a copy, and its lifecycle work, deleting a Pod
// once its status is terminal, reads the API server's.
type vkProvider struct{ *Handler }

// NotifyPods implements VK's PodNotifier, starting the handler's poll loop without VK's cb.
func (p vkProvider) NotifyPods(ctx context.Context, _ func(*corev1.Pod)) {
	p.Handler.NotifyPods(ctx, func(*corev1.Pod) {})
}

// persistStatus server-side-applies pod's status. The poll loop re-emits every Pod each tick,
// so an unchanged status is skipped, and a terminal one is written once and then never again,
// as VK did: the API server itself lets a Failed Pod go back to Running. The first write in
// this process reads the live Pod first (see adoptStatus).
//
// Best-effort like persistMetadata: a nil client (tests) is a no-op, a failure is retried on
// the next tick, and NotFound means the Pod is gone.
func (h *Handler) persistStatus(ctx context.Context, pod *corev1.Pod) {
	if h.client == nil {
		return
	}
	k := key(pod.Namespace, pod.Name)
	h.mu.Lock()
	var applied *corev1.PodStatus
	final := false
	if tp, ok := h.tracked[k]; ok {
		applied, final = tp.appliedStatus, tp.statusFinal
	}
	h.mu.Unlock()
	if final || applied != nil && equality.Semantic.DeepEqual(*applied, pod.Status) {
		return
	}

	log := logf.FromContext(ctx).WithName("vnode-handler").WithValues("pod", k)
	if applied == nil {
		skip, err := h.adoptStatus(ctx, pod)
		if err != nil {
			if !apierrors.IsNotFound(err) {
				log.Error(err, "read pod before its first status write; the poll loop retries next tick")
			}
			return
		}
		if skip {
			h.recordStatus(k, nil, true)
			return
		}
	}
	if err := h.applyStatus(ctx, pod); err != nil {
		if !apierrors.IsNotFound(err) {
			log.Error(err, "write pod status; the poll loop retries next tick", "phase", pod.Status.Phase)
		}
		return
	}
	h.recordStatus(k, pod.Status.DeepCopy(), util.Finished(pod.Status.Phase))
}

// recordStatus stores what persistStatus wrote for a tracked Pod; an untracked one is not
// remembered, so its next emit writes again.
func (h *Handler) recordStatus(k string, applied *corev1.PodStatus, final bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if tp, ok := h.tracked[k]; ok {
		tp.appliedStatus, tp.statusFinal = applied, final
	}
}

// adoptStatus reads the live Pod before this process first writes its status, and reports skip
// for one that must not be written: already terminal, or another Pod under the same name.
//
// Otherwise it drops VK's managedFields entry for the status VK wrote before this release.
// Left in place, that entry co-owns every field VK set, so a field Nebula later stops sending
// would stay, half-removed (a condition with no status), instead of being cleared.
func (h *Handler) adoptStatus(ctx context.Context, pod *corev1.Pod) (skip bool, err error) {
	pods := h.client.CoreV1().Pods(pod.Namespace)
	live, err := pods.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return false, err
	}
	if pod.UID != "" && live.UID != pod.UID || util.Finished(live.Status.Phase) {
		return true, nil
	}
	for i, mf := range live.ManagedFields {
		if mf.Manager != vkFieldManager || mf.Operation != metav1.ManagedFieldsOperationUpdate || mf.Subresource != "status" {
			continue
		}
		// Tested before removal, so a concurrent change to the list cannot drop another entry.
		path := fmt.Sprintf("/metadata/managedFields/%d", i)
		patch, err := json.Marshal([]map[string]any{
			{"op": "test", "path": path + "/manager", "value": mf.Manager},
			{"op": "test", "path": path + "/subresource", "value": mf.Subresource},
			{"op": "test", "path": path + "/operation", "value": mf.Operation},
			{"op": "remove", "path": path},
		})
		if err != nil {
			return false, err
		}
		_, err = pods.Patch(ctx, pod.Name, types.JSONPatchType, patch, metav1.PatchOptions{})
		return false, err
	}
	return false, nil
}

// applyStatus applies pod's status as Nebula's, forcing ownership of what VK wrote before. The
// UID, when known, makes a late write for a deleted Pod fail on one recreated under its name.
func (h *Handler) applyStatus(ctx context.Context, pod *corev1.Pod) error {
	raw, err := json.Marshal(pod.Status)
	if err != nil {
		return err
	}
	status := &corev1ac.PodStatusApplyConfiguration{}
	if err := json.Unmarshal(raw, status); err != nil {
		return err
	}
	// The two fields markStatus clears. Empty, the round trip above drops them, and a value
	// someone else wrote (VK, before this release) would outlive the transition that cleared it.
	if status.Reason == nil {
		status.WithReason("")
	}
	if status.Message == nil {
		status.WithMessage("")
	}
	ac := corev1ac.Pod(pod.Name, pod.Namespace).WithStatus(status)
	if pod.UID != "" {
		ac.WithUID(pod.UID)
	}
	_, err = h.client.CoreV1().Pods(pod.Namespace).ApplyStatus(ctx, ac,
		metav1.ApplyOptions{FieldManager: statusManager, Force: true})
	return err
}
