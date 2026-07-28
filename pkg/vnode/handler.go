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
	logf "sigs.k8s.io/controller-runtime/pkg/log"

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
const defaultPollInterval = 15 * time.Second

// defaultBlocklistTTL bounds how long a failed placement is excluded when the Pod
// carries no BlocklistTTLAnnotation (an out-of-band Pod, or one placed before the
// annotation existed). It mirrors FailoverPolicy.BlocklistTTL's own default so the
// behaviour is identical whether or not the pool policy reached the handler.
const defaultBlocklistTTL = 10 * time.Minute

// Blocklister records a failed placement so the placement controller can fail over
// to the next candidate instead of hot-looping against a provider that just
// rejected the request. It is the write half of pkg/failover.Blocklist; the
// handler depends on this narrow interface (not the concrete type) so it stays
// testable and a nil blocklist is a no-op.
type Blocklister interface {
	Record(prov string, scope provider.BlockScope, ttl time.Duration)
}

// defaultProvisionTimeout bounds a single Provision call when the provider does
// not override it (Capabilities.ProvisionTimeout). Provisioning is a network
// call to the backend (Modal's gRPC CreateSandbox, AWS's RunInstances and its
// per-zone capacity failover), and none of them are guaranteed to return
// promptly — a hung call would otherwise pin the pod controller's worker
// indefinitely. Enforcing it here, at the single call site, keeps every provider
// bounded by default; a provider that legitimately needs longer (AWS sweeping
// several zones) raises it via Capabilities. It is deliberately generous: the
// happy path returns in well under this, so it is a backstop, not a tuning knob.
const defaultProvisionTimeout = 90 * time.Second

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

	// blocklist records failed placements so the placement controller can fail over
	// (zone → region → tier). Shared across every provider's handler and the
	// placement controller. May be nil (a no-op), keeping tests and any
	// blocklist-less wiring simple.
	blocklist Blocklister

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
// defaultPollInterval when the provider does not set one. blocklist is the shared
// failover blocklist a Provision failure is recorded on; it may be nil (no-op).
func NewHandler(prov provider.Provider, blocklist Blocklister) *Handler {
	poll := prov.Capabilities().PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	return &Handler{
		prov:      prov,
		blocklist: blocklist,
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
		Region:       pod.Annotations[nebulav1alpha1.RegionAnnotation],
	}

	// Bound the provision call so a wedged backend cannot pin this worker forever.
	// The provider may raise the deadline via Capabilities.ProvisionTimeout (AWS
	// does, to leave room for cross-zone failover); zero means "use the default".
	timeout := h.prov.Capabilities().ProvisionTimeout
	if timeout <= 0 {
		timeout = defaultProvisionTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	id, err := h.prov.Provision(ctx, pod, req)
	if err != nil {
		// Record the failure on the shared blocklist so placement fails over to the
		// next candidate (zone → region → tier) instead of hot-looping here. The
		// provider classifies its own error into the precise BlockScope (a Spot
		// no-capacity in one region blocks only that, not OnDemand or other regions;
		// auth/quota blocks the whole provider); the TTL rides on the Pod from the
		// pool's FailoverPolicy.
		h.recordBlock(pod, err)
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
	log := logf.FromContext(ctx).WithName("vnode-poll").WithValues("provider", h.prov.Name())
	log.Info("poll loop started", "interval", h.pollEvery.String())
	t := time.NewTicker(h.pollEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			log.Info("poll loop stopped")
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
//
// A List error is logged and skipped (retry next tick) rather than swallowed:
// a provider whose List fails every tick would otherwise strand every tracked
// pod in its last status forever with no signal, which is exactly the failure
// mode this logging exists to surface.
func (h *Handler) reconcileOnce(ctx context.Context) {
	log := logf.FromContext(ctx).WithName("vnode-poll").WithValues("provider", h.prov.Name())
	instances, err := h.prov.List(ctx)
	if err != nil {
		log.Error(err, "provider List failed; tracked pod statuses will not advance this tick")
		return // transient; retry on the next tick
	}
	byClaim := make(map[string]provider.Instance, len(instances))
	for _, inst := range instances {
		byClaim[inst.ClaimName] = inst
	}

	h.mu.Lock()
	changed := make([]*corev1.Pod, 0)
	tracked := len(h.tracked)
	matched := 0
	for _, tp := range h.tracked {
		inst, present := byClaim[tp.claimName]
		before := tp.pod.Status.Phase
		if !present {
			applyState(tp.pod, provider.InstanceTerminated, "", h.nowFn())
		} else {
			matched++
			applyState(tp.pod, inst.State, inst.Endpoint, h.nowFn())
		}
		if tp.pod.Status.Phase != before {
			changed = append(changed, tp.pod.DeepCopy())
		}
	}
	notify := h.notify
	h.mu.Unlock()

	// At V(1) so a healthy steady state stays quiet, but the counts are there when
	// pods appear stuck: tracked>0 with matched==0 means List returns instances the
	// claim names don't line up with (the classic "provisioned but never Running").
	log.V(1).Info("poll tick",
		"listed", len(instances), "tracked", tracked, "matched", matched, "changed", len(changed))

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

// recordBlock classifies a Provision failure into a BlockScope (via the provider)
// and records it on the shared blocklist for the pool's BlocklistTTL, so placement
// fails over instead of retrying the same failing candidate. It is a no-op when no
// blocklist is wired or the error yields an empty scope (nothing to block).
//
// The provider owns the whole scope: the handler resolves the requested accelerator
// off the Pod (the error does not carry it) and passes it in, but does NOT assemble
// or mutate the scope itself — narrowing to the failing accelerator and any
// region axis is the provider's job (see ClassifyError / the adapters). This keeps
// scope derivation in one place per provider rather than spread across the handler.
//
// Because the provider stamps the accelerator, the block errs NARROW: a truly
// account/region-wide error (e.g. a per-region Spot limit that spans accelerators)
// is narrowed to just this accelerator too, since the error text does not
// distinguish it from an instance-type shortage; the cost is that a sibling
// accelerator gets one wasted re-probe before it re-records. That is the right
// trade — over-narrow costs a fast retry, over-broad wrongly excludes serviceable
// accelerators. DenyAll (auth/quota) ignores the accelerator: it fails for all.
func (h *Handler) recordBlock(pod *corev1.Pod, err error) {
	if h.blocklist == nil {
		return
	}
	// The accelerator is a property of the REQUEST, not the error; the provider
	// cannot see it, so we resolve it off the Pod and hand it over ("" for a
	// CPU-only Pod, which the provider treats as "not applicable").
	accel, _, _ := util.AcceleratorRequest(pod)
	scope := h.prov.ClassifyProvisionError(err, accel)
	if scope == (provider.BlockScope{}) {
		// An empty scope means the error is not one we know how to blocklist; do not
		// install a wildcard block that would exclude everything on the provider.
		return
	}
	h.blocklist.Record(h.prov.Name(), scope, blocklistTTL(pod))
}

// blocklistTTL reads the pool's FailoverPolicy.BlocklistTTL off the Pod annotation
// the placement controller stamped, falling back to defaultBlocklistTTL when it is
// absent or unparseable (an out-of-band Pod, or a non-positive value that could
// otherwise install a permanent block).
func blocklistTTL(pod *corev1.Pod) time.Duration {
	raw := pod.Annotations[nebulav1alpha1.BlocklistTTLAnnotation]
	if raw == "" {
		return defaultBlocklistTTL
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d <= 0 {
		return defaultBlocklistTTL
	}
	return d
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
