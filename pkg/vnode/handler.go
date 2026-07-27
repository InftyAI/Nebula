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
	"io"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/api/statsv1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// defaultPollInterval is how often the notifier re-lists the provider to detect
// state changes (ready, preemption, disappearance). Detection is poll-based
// everywhere — no provider pushes preemption/termination — so this is the
// resolution at which the virtual node notices instance state transitions. It is
// only the fallback: a provider may override the cadence via
// Capabilities.PollInterval (e.g. a spot-heavy backend polls faster to notice
// reclaims sooner; an OnDemand-only one can poll lazily).
const defaultPollInterval = 30 * time.Second

// Handler bridges one provider into the virtual kubelet: it implements the VK
// PodLifecycleHandler (+ PodNotifier) so the pod controller's CreatePod
// provisions an external instance through the provider seam and DeletePod
// terminates it. This is the "VK owns provisioning" model — there is no separate
// controller issuing Provision/Terminate.
//
// Leak-safety: the pod controller only calls DeletePod for a pod it is tracking,
// and CreatePod records the instance id before returning success, so a paid
// instance is always reachable for teardown. Provision is idempotent on
// ClaimName, so a CreatePod retried after a crash between provider-create and
// status-write adopts the existing instance rather than creating a second.
type Handler struct {
	prov provider.Provider

	mu      sync.Mutex
	tracked map[string]*trackedPod // key: namespace/name
	notify  func(*corev1.Pod)

	// nowFn and pollEvery are seams for tests.
	nowFn     func() metav1.Time
	pollEvery time.Duration
}

// trackedPod is the virtual node's local record of one pod it provisioned for.
// The Pod object is the source of truth for the workload; we additionally hold
// the provider instance id (for Terminate) and the derived claim name (to match
// this pod against the provider's List during polling).
type trackedPod struct {
	pod       *corev1.Pod
	claimName string
	instance  string
}

// NewHandler builds a Handler for the given provider backend. The poll cadence
// comes from the provider's Capabilities.PollInterval, falling back to
// defaultPollInterval when the provider does not set one.
func NewHandler(prov provider.Provider) *Handler {
	poll := prov.Capabilities().PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	return &Handler{
		prov:      prov,
		tracked:   make(map[string]*trackedPod),
		nowFn:     metav1.Now,
		pollEvery: poll,
	}
}

// Compile-time proof the Handler satisfies the VK interfaces we rely on.
var (
	_ vknode.PodLifecycleHandler = (*Handler)(nil)
	_ vknode.PodNotifier         = (*Handler)(nil)
)

// claimName derives a stable instance identity from the Pod. The Pod is the
// source of truth, so the claim — which providers encode into the instance
// name/tag for tag-less recovery — must be a deterministic function of it.
// namespace/name is stable across reconciles; the instance is always torn down
// on DeletePod, so name reuse after deletion is not a concern. It delegates to
// util.ClaimName so the vnode handler and the NodeClaim teardown backstop
// produce identical tokens.
func claimName(pod *corev1.Pod) string {
	return util.ClaimName(pod.Namespace, pod.Name)
}

func key(namespace, name string) string { return namespace + "/" + name }

// CreatePod provisions an external instance for the Pod through the provider.
// The Pod carries the whole workload shape; the only out-of-band input, the
// optimizer's capacity tier, rides on CapacityTypeAnnotation.
func (h *Handler) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	claim := claimName(pod)
	req := provider.ProvisionRequest{
		ClaimName:    claim,
		CapacityType: nebulav1alpha1.CapacityType(pod.Annotations[nebulav1alpha1.CapacityTypeAnnotation]),
	}

	id, err := h.prov.Provision(ctx, pod, req)
	if err != nil {
		// Surface the failure on the Pod so the placement controller can fail
		// over, and return the error so the pod controller retries with backoff.
		h.markStatus(pod, corev1.PodFailed, reasonProvisionFailed, err.Error())
		h.store(pod, claim, "")
		h.emit(pod)
		return err
	}

	// Record the instance id before reporting success; teardown relies on it.
	h.markStatus(pod, corev1.PodPending, reasonProvisioning, "provisioning external instance")
	h.store(pod, claim, id)
	h.emit(pod)
	return nil
}

// UpdatePod is a no-op: the external instance's shape is immutable once
// provisioned (recovery from any change is delete-and-recreate, matching the
// NodeClaim ledger's immutability). We still refresh our tracked copy so
// GetPod reflects the latest metadata.
func (h *Handler) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if tp, ok := h.tracked[key(pod.Namespace, pod.Name)]; ok {
		// Preserve the status we compute from the provider; only adopt spec/meta.
		status := tp.pod.Status
		tp.pod = pod.DeepCopy()
		tp.pod.Status = status
	}
	return nil
}

// DeletePod terminates the external instance and drops the pod from tracking.
// Terminate is idempotent, so a repeated DeletePod (VK may call it more than
// once) is safe.
func (h *Handler) DeletePod(ctx context.Context, pod *corev1.Pod) error {
	h.mu.Lock()
	tp, ok := h.tracked[key(pod.Namespace, pod.Name)]
	instance := ""
	if ok {
		instance = tp.instance
	}
	h.mu.Unlock()

	if err := h.prov.Terminate(ctx, instance); err != nil {
		return err
	}

	// Report a terminal status, then forget the pod. VK expects the containers
	// and pod to reach a terminal state after DeletePod.
	h.markStatus(pod, corev1.PodSucceeded, "Terminated", "external instance terminated")
	pod.DeletionTimestamp = ptrNow(h.nowFn())
	h.emit(pod)

	h.mu.Lock()
	delete(h.tracked, key(pod.Namespace, pod.Name))
	h.mu.Unlock()
	return nil
}

// GetPod returns the tracked pod, or a NotFound error the pod controller
// understands.
func (h *Handler) GetPod(_ context.Context, namespace, name string) (*corev1.Pod, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tp, ok := h.tracked[key(namespace, name)]
	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s not found on virtual node", namespace, name)
	}
	return tp.pod.DeepCopy(), nil
}

// GetPodStatus returns the tracked pod's status.
func (h *Handler) GetPodStatus(_ context.Context, namespace, name string) (*corev1.PodStatus, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	tp, ok := h.tracked[key(namespace, name)]
	if !ok {
		return nil, errdefs.NotFoundf("pod %s/%s not found on virtual node", namespace, name)
	}
	return tp.pod.Status.DeepCopy(), nil
}

// GetPods returns every pod this virtual node is tracking.
func (h *Handler) GetPods(_ context.Context) ([]*corev1.Pod, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	pods := make([]*corev1.Pod, 0, len(h.tracked))
	for _, tp := range h.tracked {
		pods = append(pods, tp.pod.DeepCopy())
	}
	return pods, nil
}

// NotifyPods registers the async status callback and starts the poll loop. VK
// calls this once at startup; the loop runs until ctx is cancelled.
func (h *Handler) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	h.mu.Lock()
	h.notify = cb
	h.mu.Unlock()

	go h.pollLoop(ctx)
}

// pollLoop periodically reconciles tracked pods against the provider's live
// instance list and pushes any status change through the notify callback.
func (h *Handler) pollLoop(ctx context.Context) {
	t := time.NewTicker(h.pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			h.reconcileOnce(ctx)
		}
	}
}

// reconcileOnce lists the provider once and updates every tracked pod's status
// from the matching instance (matched by claim name, since a pod maps 1:1 to a
// claim). A tracked pod whose instance is absent from the list is treated as
// terminated (preempted / externally torn down), per the provider contract.
func (h *Handler) reconcileOnce(ctx context.Context) {
	instances, err := h.prov.List(ctx)
	if err != nil {
		return // transient; retry on the next tick
	}
	byClaim := make(map[string]provider.Instance, len(instances))
	for _, inst := range instances {
		byClaim[inst.ClaimName] = inst
	}

	h.mu.Lock()
	changed := make([]*corev1.Pod, 0)
	for _, tp := range h.tracked {
		inst, present := byClaim[tp.claimName]
		before := tp.pod.Status.Phase
		if !present {
			applyState(tp.pod, provider.InstanceTerminated, "", h.nowFn())
		} else {
			applyState(tp.pod, inst.State, inst.Endpoint, h.nowFn())
		}
		if tp.pod.Status.Phase != before {
			changed = append(changed, tp.pod.DeepCopy())
		}
	}
	notify := h.notify
	h.mu.Unlock()

	if notify != nil {
		for _, p := range changed {
			notify(p)
		}
	}
}

// store records/updates the tracked pod under lock.
func (h *Handler) store(pod *corev1.Pod, claim, instance string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tracked[key(pod.Namespace, pod.Name)] = &trackedPod{
		pod:       pod.DeepCopy(),
		claimName: claim,
		instance:  instance,
	}
}

// emit pushes a status update through the notify callback if one is registered.
func (h *Handler) emit(pod *corev1.Pod) {
	h.mu.Lock()
	notify := h.notify
	h.mu.Unlock()
	if notify != nil {
		notify(pod.DeepCopy())
	}
}

// markStatus sets a coarse pod phase and a single readiness-style condition on
// the passed Pod (which VK then reports to the API server).
func (h *Handler) markStatus(pod *corev1.Pod, phase corev1.PodPhase, reason, msg string) {
	setPhase(pod, phase, reason, msg, h.nowFn())
}

func ptrNow(t metav1.Time) *metav1.Time { return &t }

// --- Unused nodeutil.Provider surface --------------------------------------
//
// The kubelet API (logs/exec/attach/stats/port-forward) is not part of the v1
// scope: Nebula routes external GPU workloads, it does not proxy their consoles.
// These satisfy the nodeutil.Provider interface required by nodeutil.NewNode and
// return a NotFound so the VK core reports them cleanly rather than panicking.

func (h *Handler) GetContainerLogs(
	context.Context, string, string, string, vkapi.ContainerLogOpts,
) (io.ReadCloser, error) {
	return nil, errdefs.NotFound("container logs are not supported by the Nebula virtual node")
}

func (h *Handler) RunInContainer(context.Context, string, string, string, []string, vkapi.AttachIO) error {
	return errdefs.NotFound("exec is not supported by the Nebula virtual node")
}

func (h *Handler) AttachToContainer(context.Context, string, string, string, vkapi.AttachIO) error {
	return errdefs.NotFound("attach is not supported by the Nebula virtual node")
}

func (h *Handler) GetStatsSummary(context.Context) (*statsv1alpha1.Summary, error) {
	return nil, errdefs.NotFound("stats are not supported by the Nebula virtual node")
}

func (h *Handler) GetMetricsResource(context.Context) ([]*dto.MetricFamily, error) {
	return nil, errdefs.NotFound("resource metrics are not supported by the Nebula virtual node")
}

func (h *Handler) PortForward(context.Context, string, string, int32, io.ReadWriteCloser) error {
	return errdefs.NotFound("port-forward is not supported by the Nebula virtual node")
}
