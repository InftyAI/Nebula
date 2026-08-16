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
	"io"
	"math/rand/v2"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/virtual-kubelet/virtual-kubelet/errdefs"
	vknode "github.com/virtual-kubelet/virtual-kubelet/node"
	vkapi "github.com/virtual-kubelet/virtual-kubelet/node/api"
	"github.com/virtual-kubelet/virtual-kubelet/node/api/statsv1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	nebulav1alpha1 "github.com/InftyAI/Nebula/api/v1alpha1"
	"github.com/InftyAI/Nebula/pkg/metrics"
	"github.com/InftyAI/Nebula/pkg/provider"
	"github.com/InftyAI/Nebula/pkg/util"
)

// defaultPollInterval is how often we re-list the provider to notice state changes
// (ready, preempted, gone). No provider pushes those events, so polling is the only
// signal. A provider can override the cadence via Capabilities.PollInterval.
const defaultPollInterval = 15 * time.Second

// defaultBlocklistTTL is the base exclusion for a failed placement when the Pod
// carries no BlocklistTTLAnnotation. Short on purpose: the jitter added in
// recordBlock, not a long floor, is what spreads retries.
const defaultBlocklistTTL = 30 * time.Second

// blocklistJitter is the largest random delay added to a block's base TTL. Without
// it, every Pod that failed for one scope re-tries the instant the shared entry
// expires and they stampede the same just-freed candidate together.
const blocklistJitter = 30 * time.Second

// Blocklister records a failed placement so the placement controller fails over to
// the next candidate instead of hot-looping on a provider that just said no. The
// write half of pkg/failover.Blocklist; a nil blocklist is a no-op.
type Blocklister interface {
	Record(prov string, scope provider.BlockScope, ttl time.Duration)
}

// defaultProvisionTimeout bounds one Provision call, so a hung backend cannot pin a
// pod-controller worker forever. Deliberately generous — a backstop, not a tuning
// knob. A provider that needs longer (AWS sweeping zones) raises it via
// Capabilities.ProvisionTimeout.
const defaultProvisionTimeout = 90 * time.Second

// Handler bridges one provider into the virtual kubelet: CreatePod provisions an
// external instance, DeletePod terminates it. This is the "VK owns provisioning"
// model — no separate controller issues Provision/Terminate.
//
// Leak-safety: CreatePod records the instance id before returning success, so a paid
// instance is always reachable for teardown, and Provision is idempotent on
// ClaimName, so a retry after a crash adopts the existing instance instead of
// creating a second.
type Handler struct {
	prov provider.Provider

	// client patches the endpoint annotation onto the Pod's metadata. VK persists only
	// the status subresource, so metadata needs a write of its own. Nil in tests, where
	// the annotation stays on the in-memory Pod.
	client kubernetes.Interface

	// blocklist records failed placements so placement fails over (zone → region →
	// tier). Shared with every other handler and the placement controller. Nil is a
	// no-op.
	blocklist Blocklister

	mu sync.Mutex

	// tracked is the poll loop's work list and what GetPod/GetPodStatus serve, keyed by
	// namespace/name.
	//
	// INVARIANT: only pods the provider has acknowledged (Provision returned an id), or
	// already terminal ones. Never one with Provision still in flight — reconcileOnce
	// writes Terminated for any tracked pod missing from List(), which for a live
	// provision is wrong and unrecoverable.
	tracked map[string]*trackedPod

	notify func(*corev1.Pod)

	// nowFn and pollEvery are seams for tests.
	nowFn     func() metav1.Time
	pollEvery time.Duration

	// jitterFn returns the delay added to a block's base TTL (see recordBlock). A seam
	// so tests can pin it to 0 and assert an exact TTL.
	jitterFn func() time.Duration
}

// trackedPod is the local record of one pod we provisioned for: the Pod itself, plus
// the instance id (for Terminate) and the claim name (to match it against the
// provider's List while polling).
type trackedPod struct {
	pod       *corev1.Pod
	claimName string
	instance  string
	// connectEndpoint is the endpoint last patched onto the Pod. notify fires every poll
	// tick, so this narrows the patch to the ticks where the address actually changed.
	//
	// A cache, not a record: losing it on a restart costs one redundant patch, never a
	// lost value, since the annotation lives in etcd. Holds no credential — see
	// persistCredential.
	connectEndpoint string

	// provisionStart is when THIS process began provisioning. It arms the one
	// metrics.InstanceReadyDuration observation the poll loop makes on the first
	// Running, and is consumed by it, so zero means "do not observe" for either reason:
	//
	//   - never armed: a pod re-adopted after a restart lost its real start time with
	//     the process, and measuring from re-adoption would report minutes as
	//     milliseconds — a missing sample beats a wrong one;
	//   - already spent: the poll loop sees Running every tick for the pod's whole life,
	//     so a still-armed token would fire per tick with a growing duration, measuring
	//     longevity instead of readiness.
	//
	// Known bias: a provision still in flight across a restart never contributes, so the
	// histogram under-samples the slowest boots. Fixing it means persisting the start
	// time, a write on the provisioning path we have not taken.
	provisionStart time.Time
}

// NewHandler builds a Handler for one provider backend. The poll cadence comes from
// Capabilities.PollInterval, falling back to defaultPollInterval. blocklist (failover
// recording) and client (the endpoint patch) may both be nil.
func NewHandler(prov provider.Provider, client kubernetes.Interface, blocklist Blocklister) *Handler {
	poll := prov.Capabilities().PollInterval
	if poll <= 0 {
		poll = defaultPollInterval
	}
	return &Handler{
		prov:      prov,
		client:    client,
		blocklist: blocklist,
		tracked:   make(map[string]*trackedPod),
		nowFn:     metav1.Now,
		pollEvery: poll,
		// rand/v2's top-level source is auto-seeded and safe for concurrent use, so
		// every handler draws an independent jitter without shared seeding.
		jitterFn: func() time.Duration { return time.Duration(rand.Int64N(int64(blocklistJitter))) },
	}
}

// Compile-time proof the Handler satisfies the VK interfaces we rely on.
var (
	_ vknode.PodLifecycleHandler = (*Handler)(nil)
	_ vknode.PodNotifier         = (*Handler)(nil)
)

func key(namespace, name string) string { return namespace + "/" + name }

// CreatePod provisions an external instance for the Pod through the provider.
// The Pod carries the whole workload shape; the only out-of-band input, the
// optimizer's capacity tier, rides on CapacityTypeAnnotation.
func (h *Handler) CreatePod(ctx context.Context, pod *corev1.Pod) error {
	claim := util.ClaimName(pod.Namespace, pod.Name)
	req := provider.ProvisionRequest{
		ClaimName:    claim,
		CapacityType: nebulav1alpha1.CapacityType(pod.Annotations[nebulav1alpha1.CapacityTypeAnnotation]),
		Region:       pod.Annotations[nebulav1alpha1.RegionAnnotation],
	}

	log := logf.FromContext(ctx).WithName("vnode-handler").WithValues(
		"provider", h.prov.Name(), "pod", key(pod.Namespace, pod.Name), "claim", claim)

	// Bound the provision call so a wedged backend cannot pin this worker forever. A
	// provider may raise the deadline via Capabilities.ProvisionTimeout (AWS does, for
	// cross-zone failover).
	//
	// Scoped to that ONE call: the writes after it stay on the caller's ctx. A Provision
	// returning just under the deadline would leave them no budget, so they would fail
	// on success — and the credential write cannot be retried, so a timeout there loses
	// the token for good.
	timeout := h.prov.Capabilities().ProvisionTimeout
	if timeout <= 0 {
		timeout = defaultProvisionTimeout
	}
	provisionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Info("provisioning external instance",
		"capacityType", req.CapacityType, "region", req.Region, "timeout", timeout.String())

	// Two clocks for two different waits: provisionStart measures the end-to-end wait a
	// user feels (until the instance reports Running; handed to store below), callStart
	// only the Provision call. Not interchangeable — the emit between them is a
	// synchronous notify that can issue an API write, which would otherwise be charged
	// to the provider's latency.
	provisionStart := time.Now()
	mlabels := h.metricLabels(pod)

	// Report Provisioning BEFORE the call: it can run for minutes (AWS sweeps a region's
	// zones on a capacity error), and until it returns this is the only explanation the
	// Pod carries. Emit but do NOT store — see the tracked invariant.
	h.markStatus(pod, corev1.PodPending, reasonProvisioning, "allocating external instance")
	h.emit(pod)

	callStart := time.Now()
	res, err := h.prov.Provision(provisionCtx, pod, req)
	// One call site for both outcomes, so the attempt and failure counters cannot drift.
	// An unreachable provider still counts as a failed attempt even though nothing gets
	// blocklisted for it.
	metrics.ObserveProvision(mlabels, time.Since(callStart), err)
	if err != nil {
		// An error the provider never attributed to this request — a transport failure,
		// our own timeout, a 503 — is not a rejection (see provider.IsRejection). The
		// request may even have been accepted, so failing the Pod would reap it out from
		// under a paid instance whose id we never learned, and blocklisting would fence
		// off a candidate that never misbehaved.
		//
		// So leave it NON-terminal at the Provisioning already stamped, with the error as
		// its message, and return the error for VK to retry with backoff. Provision is
		// idempotent on ClaimName, so the retry adopts whatever the failed attempt created.
		//
		// Deliberately NOT stored: a tracked pod with no instance id would be read as
		// absent from List and written the very Terminated this branch avoids.
		if !provider.IsRejection(err) {
			log.Error(err, "provision failed with an error the provider did not attribute "+
				"to this request; Pod left provisioning for retry, nothing blocklisted")
			h.markStatus(pod, corev1.PodPending, reasonProvisioning, "retrying: "+err.Error())
			h.emit(pod)
			return err
		}

		log.Error(err, "provision rejected by the provider; Pod marked Failed for failover")
		// Record the failure so placement fails over to the next candidate (zone → region
		// → tier) instead of hot-looping here. The provider narrows its own error into a
		// BlockScope (a Spot shortage in one region blocks only that; auth/quota blocks
		// the whole provider); the TTL rides on the Pod from the pool's FailoverPolicy.
		h.recordBlock(ctx, pod, err)
		// Surface the failure so placement can fail over, and return the error so the pod
		// controller retries with backoff.
		h.markStatus(pod, corev1.PodFailed, reasonProvisionFailed, err.Error())
		// Zero start: terminal, so it never reaches Running and has no ready-duration.
		h.store(pod, claim, "", time.Time{})
		h.emit(pod)
		return err
	}

	// Only a RESERVED instance advances: capacity is committed and it is booting. An
	// unreserved id (a Modal sandbox accepted but still queued for a GPU) stays at the
	// Provisioning stamped above, which is exactly true — the id is real and must be
	// reclaimed, but nothing is allocated yet.
	//
	// store runs either way: an id means the instance exists. markStatus first, because
	// store deep-copies and would otherwise track a copy without the new status.
	log.Info("external instance provisioned", "instanceID", res.InstanceID, "reserved", res.Reserved)
	if res.Reserved {
		h.markStatus(pod, corev1.PodPending, reasonInitializing, "external instance is initializing")
	}
	// Stamp the create-time address BEFORE store, since store deep-copies and the tracked
	// copy is what the poll loop re-emits. The emit below publishes it, and every later
	// tick re-offers it until a write lands. No lock: the Pod is not shared until store.
	setEndpoint(pod, res.ConnectURL)
	h.store(pod, claim, res.InstanceID, provisionStart)

	// The TOKEN cannot ride the Pod (readable with `get pod`, unencrypted in etcd), so it
	// gets its own write — the only place it exists, since the provider mints it once and
	// cannot re-read it. After store and before emit, so a tracked pod is never visible
	// without its credential. It does not gate the status: a Pod that provisioned is
	// reported provisioned even if the Secret write failed.
	h.persistCredential(ctx, pod, res.ConnectURL, res.ConnectToken)

	h.emit(pod)
	return nil
}

// persistCredential writes the bearer token, plus the address it authenticates against,
// into a Secret.
//
// Only the secret half. The address is published on the Pod (see setEndpoint), but a
// token cannot travel that way: an annotation is readable with `get pod` and lands
// unencrypted in etcd. So it goes to an access-controlled Secret, with the URL alongside
// it so the pair is usable from one object.
//
// One-shot and unrepeatable: minting is create-only with no read-back, so a failed write
// cannot be retried, here or later. That is the difference from the address, which the
// poll loop keeps re-offering until it lands.
//
// An empty url means the provider mints no credential (AWS, whose address is only known
// at boot); an empty token means an address with nothing to authenticate. Either way
// there is nothing to write.
//
// Best-effort: a nil client (tests) is a no-op, failures are logged without the token.
func (h *Handler) persistCredential(ctx context.Context, pod *corev1.Pod, url, token string) {
	if h.client == nil || url == "" || token == "" {
		return
	}
	h.createConnectSecret(ctx, pod, url, token)
}

// UpdatePod is a no-op: an instance's shape is immutable once provisioned (recovery
// from any change is delete-and-recreate). We still refresh the tracked copy so GetPod
// reflects the latest metadata.
func (h *Handler) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if tp, ok := h.tracked[key(pod.Namespace, pod.Name)]; ok {
		// Keep what WE own and the API server does not know yet: the status we compute,
		// and the endpoint — possibly an address minted at create whose patch has not
		// landed, so the incoming Pod lacks it. Dropping it would discard the only copy,
		// since a minted URL is never re-observed. Everything else is adopted.
		status := tp.pod.Status
		endpoint := tp.pod.Annotations[nebulav1alpha1.EndpointAnnotation]
		tp.pod = pod.DeepCopy()
		tp.pod.Status = status
		setEndpoint(tp.pod, endpoint)
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

	log := logf.FromContext(ctx).WithName("vnode-handler").WithValues(
		"provider", h.prov.Name(), "pod", key(pod.Namespace, pod.Name), "instanceID", instance)

	log.Info("terminating external instance")
	if err := h.prov.Terminate(ctx, instance); err != nil {
		// The leak-risk path: VK retries DeletePod, and if that never succeeds the
		// NodeClaim backstop is the last line of defense. Log loudly.
		log.Error(err, "terminate failed; external instance may still be running (NodeClaim backstop will retry)")
		return err
	}

	// Report a terminal status, then forget the pod. VK expects the containers
	// and pod to reach a terminal state after DeletePod.
	log.Info("external instance terminated")
	h.markStatus(pod, corev1.PodSucceeded, "Terminated", "external instance terminated")
	pod.DeletionTimestamp = ptrNow(h.nowFn())
	h.emit(pod)

	h.mu.Lock()
	delete(h.tracked, key(pod.Namespace, pod.Name))
	h.mu.Unlock()
	return nil
}

// GetPod returns the tracked pod, or a NotFound error the pod controller understands.
//
// Tracking is in-memory, so a restart (redeploy, crash, leader handoff) starts with an
// empty map while the instances are still running. VK's createOrUpdatePod treats a nil
// result as "create", which would re-drive provisioning. So on a cold map we RE-ADOPT:
// ask the provider whether an instance with this claim is live, and rebuild the entry
// from it. VK then takes the adopt branch and the next poll tick advances the pod.
//
// The three outcomes stay DISTINCT, because "no such instance" and "could not ask" have
// opposite consequences. Reporting the latter as NotFound is what let one failed List
// destroy a healthy workload: VK discards this error and branches on nil-ness alone, so
// a nil pod re-issues CreatePod against a running instance — and with the provider still
// unreachable that Provision fails too, marking the Pod Failed for a reap while the real
// instance keeps billing behind a zero id. So an unreachable provider returns a non-nil
// pod WITH an error: the pod suppresses the create, and the non-NotFound error makes the
// delete path requeue instead of terminating. Both wait for the next sync, the only
// correct move while ownership is unknown.
func (h *Handler) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	h.mu.Lock()
	tp, ok := h.tracked[key(namespace, name)]
	h.mu.Unlock()
	if ok {
		return tp.pod.DeepCopy(), nil
	}

	log := logf.FromContext(ctx).WithName("vnode-handler").WithValues(
		"provider", h.prov.Name(), "pod", key(namespace, name))

	// Cold map: re-adopt from the live provider if the instance still exists.
	claim := util.ClaimName(namespace, name)
	inst, found, err := h.instanceByClaim(ctx, claim)
	if err != nil {
		// Loud, and never swallowed: this is the only signal a Pod is being held back, and
		// every alternative guesses — costing either a duplicate paid instance or a
		// reaped live one.
		log.Error(err, "cannot determine whether an instance exists for this Pod; "+
			"holding off create and delete until the provider answers", "claim", claim)
		stub := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
		return stub, fmt.Errorf("provider %q unreachable, ownership of pod %s/%s unknown: %w",
			h.prov.Name(), namespace, name, err)
	}
	if !found {
		return nil, errdefs.NotFoundf("pod %s/%s not found on virtual node", namespace, name)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	applyState(pod, inst.State, inst.Endpoint, h.nowFn())
	// Zero start: this process never provisioned it, so the real start time is gone and
	// the ready-duration is not observable (see trackedPod.provisionStart).
	h.store(pod, claim, inst.ID, time.Time{})
	log.Info("re-adopted live instance after cold tracking map (VK restart)",
		"claim", claim, "instanceID", inst.ID, "state", inst.State)
	return pod.DeepCopy(), nil
}

// instanceByClaim returns the live instance whose ClaimName matches, whether one was
// found, and any List error. It backs GetPod's post-restart re-adoption.
//
// The error is kept separate from found=false because they are different answers:
// found=false ASSERTS the instance does not exist, an error means we do not know.
// Conflating them turns "unknown" into "absent" — the strongest claim from the least
// information — and GetPod acts very differently on each.
func (h *Handler) instanceByClaim(ctx context.Context, claim string) (provider.Instance, bool, error) {
	instances, err := h.prov.List(ctx)
	if err != nil {
		return provider.Instance{}, false, err
	}
	for _, inst := range instances {
		if inst.ClaimName == claim {
			return inst, true, nil
		}
	}
	return provider.Instance{}, false, nil
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

// NotifyPods registers the async status callback and starts the poll loop. VK calls it
// once at startup; the loop runs until ctx is cancelled.
//
// We WRAP VK's callback: its status path writes only the /status subresource and
// silently drops metadata changes on the same object, but the endpoint has to live on
// metadata (PodIP cannot hold a DNS name — see applyState). So the wrapper writes the
// access details first, then hands the same Pod to VK. Every status push therefore also
// reconciles how the workload is reached.
func (h *Handler) NotifyPods(ctx context.Context, cb func(*corev1.Pod)) {
	h.mu.Lock()
	h.notify = func(pod *corev1.Pod) {
		h.persistEndpoint(ctx, pod)
		cb(pod)
	}
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

// reconcileOnce lists the provider once and updates every tracked pod from its matching
// instance (matched by claim name, 1:1 with a pod). A tracked pod absent from the list is
// treated as terminated — preempted, or torn down externally.
//
// A List error is logged and retried next tick rather than swallowed: a provider whose
// List always fails would otherwise strand every pod in its last status with no signal.
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
	emit := make([]*corev1.Pod, 0, len(h.tracked))
	tracked := len(h.tracked)
	matched := 0
	for _, tp := range h.tracked {
		inst, present := byClaim[tp.claimName]
		before := statusSignature(tp.pod)
		if !present {
			applyState(tp.pod, provider.InstanceTerminated, "", h.nowFn())
		} else {
			matched++
			applyState(tp.pod, inst.State, inst.Endpoint, h.nowFn())
			// The observed address, for a provider that cannot know it before boot.
			// Empty for one that published at create, which must not clear it.
			setEndpoint(tp.pod, inst.Endpoint)
			h.observeReady(tp, inst.State)
		}
		// Log before -> after every tick: a differing pair is the lifecycle moving
		// (Provisioning -> Initializing -> Running, or -> Terminated), an equal pair
		// confirms the pod is still being observed rather than stuck.
		log.V(1).Info("observed pod status",
			"pod", key(tp.pod.Namespace, tp.pod.Name), "before", before,
			"after", statusSignature(tp.pod))
		emit = append(emit, tp.pod.DeepCopy())
	}
	notify := h.notify
	h.mu.Unlock()

	// V(1) so a healthy steady state stays quiet. tracked>0 with matched==0 means the
	// claim names don't line up with what List returns — the classic "provisioned but
	// never Running".
	log.V(1).Info("poll tick",
		"listed", len(instances), "tracked", tracked, "matched", matched)

	// Re-emit EVERY tracked pod each tick, which makes status propagation
	// level-triggered. VK dedups an emit against the last status IT received from us,
	// never against the API server, so one dropped UpdateStatus (a conflict, cache lag)
	// leaves VK believing it delivered that status — and an edge-triggered loop never
	// re-sends it, wedging the Pod on a stale status (instance Running, Pod still
	// Pending). Re-handing the unchanged status is cheap (dedup, no API write) and arms
	// VK's own drift correction: the dedup sets lastPodStatusUpdateSkipped, so the next
	// resync notices the API server disagrees and re-issues the write.
	if notify != nil {
		for _, p := range emit {
			notify(p)
		}
	}
}

// observeReady records the end-to-end provisioning wait the first time an instance
// reports Running. The poll loop is the only place that number exists, since a
// provider's create returns long before the instance is usable.
//
// It measures with time.Since, not h.nowFn: nowFn is the STATUS clock, which a test may
// pin to a fixed instant, and subtracting a real start time from a pinned now would give
// a nonsense duration.
//
// ONE-SHOT — it consumes provisionStart, whose zero value covers both "never armed" and
// "already recorded" (see trackedPod.provisionStart). Callers must hold h.mu.
func (h *Handler) observeReady(tp *trackedPod, state provider.InstanceState) {
	if state != provider.InstanceRunning || tp.provisionStart.IsZero() {
		return
	}
	metrics.ObserveReady(h.metricLabels(tp.pod), time.Since(tp.provisionStart))
	tp.provisionStart = time.Time{} // spent; never observe this pod again
}

// setEndpoint stamps a reachable address onto the Pod's annotation — the one assignment
// site, wherever the address came from. persistEndpoint then patches it to the API
// server (PodIP cannot hold a DNS name — see applyState).
//
// Callers, and what each knows:
//
//   - CreatePod — an address MINTED at create (Modal's connect URL, which exists before
//     the sandbox does and is never reported again);
//   - reconcileOnce — one OBSERVED at boot (AWS assigns a public DNS name only once EC2
//     has one, so it can only arrive on the read path);
//   - UpdatePod — nothing new; it re-applies what VK's replacement Pod may have dropped.
//
// An empty address is ignored, not cleared, which is what lets those coexist: a provider
// that published at create and reports no observed endpoint never erases a working
// address. Never a credential — see persistCredential.
//
// Callers stamping a tracked pod's own Pod must hold h.mu.
func setEndpoint(pod *corev1.Pod, endpoint string) {
	if endpoint == "" || pod.Annotations[nebulav1alpha1.EndpointAnnotation] == endpoint {
		return
	}
	if pod.Annotations == nil {
		pod.Annotations = map[string]string{}
	}
	pod.Annotations[nebulav1alpha1.EndpointAnnotation] = endpoint
}

// statusSignature is a compact rendering of the status fields the poll loop reports,
// logged before/after each tick. It goes beyond Phase because reason, readiness and the
// assigned IP all move WITHIN one phase, which a phase-only view would hide. Keep in
// sync with what applyState writes.
func statusSignature(pod *corev1.Pod) string {
	ready := corev1.ConditionUnknown
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			ready = pod.Status.Conditions[i].Status
			break
		}
	}
	return string(pod.Status.Phase) + "|" + pod.Status.Reason + "|" + string(ready) + "|" + pod.Status.PodIP
}

// store records/updates the tracked pod under lock. provisionStart arms the
// ready-duration observation (see trackedPod.provisionStart); pass the zero Time from
// any path that cannot know it — a re-adoption, or an already-terminal pod.
func (h *Handler) store(pod *corev1.Pod, claim, instance string, provisionStart time.Time) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.tracked[key(pod.Namespace, pod.Name)] = &trackedPod{
		pod:            pod.DeepCopy(),
		claimName:      claim,
		instance:       instance,
		provisionStart: provisionStart,
	}
}

// metricLabels renders the provisioning metric labels for a Pod, read off what placement
// stamped on it.
//
// Type and count stay SEPARATE here, unlike recordBlock, which files the joined pool key:
// a blocklist needs one opaque key so an H100:8 shortage never excludes H100:1, while a
// metric needs two dimensions to be aggregated either way.
func (h *Handler) metricLabels(pod *corev1.Pod) metrics.Labels {
	accel, count, _ := util.AcceleratorRequest(pod)
	return metrics.Labels{
		Provider:         h.prov.Name(),
		Region:           pod.Annotations[nebulav1alpha1.RegionAnnotation],
		CapacityType:     pod.Annotations[nebulav1alpha1.CapacityTypeAnnotation],
		Accelerator:      accel,
		AcceleratorCount: count,
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

// persistEndpoint writes the address on the emitted Pod to the API server — the write
// half for whichever path stamped it (see setEndpoint). It runs inside the notify wrapper,
// just before VK's status callback, which would drop this metadata change.
//
// Because the poll loop re-emits every pod each tick, this is also the retry for any
// failed patch. It carries no credential — a token is written once on the create path.
//
// It runs per pod per tick, so anything unconditional is multiplied by the whole fleet;
// hence the dedup below.
//
// Best-effort: a nil client (tests) is a no-op, and a failure is retried next tick.
func (h *Handler) persistEndpoint(ctx context.Context, pod *corev1.Pod) {
	if h.client == nil {
		return
	}
	endpoint := pod.Annotations[nebulav1alpha1.EndpointAnnotation]
	if endpoint == "" {
		return
	}

	h.mu.Lock()
	tp, tracked := h.tracked[key(pod.Namespace, pod.Name)]
	// An untracked pod has nothing to dedup against, so patch unconditionally: the
	// annotation is the only place the address is published.
	patched := false
	if tracked {
		patched = tp.connectEndpoint == endpoint
	}
	h.mu.Unlock()

	if !patched {
		h.patchEndpoint(ctx, pod, endpoint)
	}
}

// patchEndpoint merge-patches the endpoint annotation onto the Pod metadata, which needs
// its own write since VK's status callback drops metadata. Scoped to the single
// annotation, so it does not collide with the status write that follows.
//
// The ONLY write of this annotation, and persistEndpoint its only caller, so every
// address — minted at create or observed at boot — reaches etcd through here. It is never
// called with an empty value, so nothing ever clears it: the annotation is where the
// address lives for the Pod's life.
//
// connectEndpoint advances only on success, so a failed patch retries next tick. NotFound
// is ignored — the Pod is gone.
func (h *Handler) patchEndpoint(ctx context.Context, pod *corev1.Pod, endpoint string) {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{nebulav1alpha1.EndpointAnnotation: endpoint},
		},
	})
	if err != nil {
		// A fixed-shape map cannot realistically fail to marshal; guard anyway so a future
		// change surfaces rather than panics.
		logf.FromContext(ctx).WithName("vnode-handler").Error(err,
			"marshal endpoint annotation patch", "pod", key(pod.Namespace, pod.Name))
		return
	}
	if _, err := h.client.CoreV1().Pods(pod.Namespace).Patch(
		ctx, pod.Name, types.MergePatchType, patch, metav1.PatchOptions{}); err != nil {
		if !apierrors.IsNotFound(err) {
			logf.FromContext(ctx).WithName("vnode-handler").Error(err,
				"persist endpoint annotation; the poll loop retries next tick",
				"pod", key(pod.Namespace, pod.Name), "endpoint", endpoint)
		}
		return // leave connectEndpoint unchanged so the next tick retries
	}

	// Record success so subsequent ticks skip the patch until the endpoint changes.
	h.mu.Lock()
	if tp, ok := h.tracked[key(pod.Namespace, pod.Name)]; ok {
		tp.connectEndpoint = endpoint
	}
	h.mu.Unlock()
}

// ConnectSecretName is the Secret holding a Pod's connect credential.
func ConnectSecretName(podName string) string { return podName + "-connect" }

// createConnectSecret writes the instance's connect URL and bearer token to a Secret in
// the Pod's namespace, so a consumer can reach the workload with
// `curl -H "Authorization: Bearer $token" $url`.
//
// A Secret, not an annotation: the token authenticates every request, and an annotation
// would expose it to anyone with `get pod` and store it unencrypted in etcd. The URL
// rides along so the pair is usable from one object.
//
// The ownerReference to the Pod is what reclaims it — deleting the Secret on the
// DeletePod path would leak on every path that skips DeletePod (a force delete, a VK
// outage), the same reason the NodeClaim finalizer exists.
//
// Write-once and unrepeatable: there is nothing to retry WITH, since minting is one-shot
// and cannot be re-read. A failure means no Secret for this instance's life, and the fix
// is to replace the instance, not to poll.
//
// AlreadyExists therefore counts as success: it means a Pod name was reused before the GC
// collected the previous Secret. Overwriting is no better — the old one still belongs to a
// live instance — and the stale copy goes away with its owner.
//
// Failures are logged, never with the token.
func (h *Handler) createConnectSecret(ctx context.Context, pod *corev1.Pod, url, token string) {
	// No UID means a synthesized Pod (GetPod's re-adoption stub), so an ownerReference
	// would be invalid and the Secret never collected. The create path always has the
	// real Pod, so this is a guard, not a case.
	if pod.UID == "" {
		return
	}
	k := key(pod.Namespace, pod.Name)
	log := logf.FromContext(ctx).WithName("vnode-handler")
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      ConnectSecretName(pod.Name),
			Namespace: pod.Namespace,
			Labels: map[string]string{
				nebulav1alpha1.ManagedByLabel: nebulav1alpha1.ManagedByValue,
			},
			// UID is what makes this a real ownerReference; a name alone is ignored by
			// the GC.
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "v1",
				Kind:       "Pod",
				Name:       pod.Name,
				UID:        pod.UID,
			}},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"token": token,
			"url":   url,
		},
	}
	_, err := h.client.CoreV1().Secrets(pod.Namespace).Create(ctx, secret, metav1.CreateOptions{})
	switch {
	case apierrors.IsAlreadyExists(err):
		return // a Secret under this name already exists; see above
	case err != nil:
		// Loud, because it is not retried: the token cannot be re-minted, so this instance
		// stays credential-less until it is replaced.
		log.Error(err, "write connect secret; the workload's credential is LOST (delete the Pod to re-provision)",
			"pod", k, "secret", ConnectSecretName(pod.Name))
		return
	}
	log.Info("wrote connect secret", "pod", k, "secret", ConnectSecretName(pod.Name))
}

// markStatus sets a coarse pod phase and a single readiness-style condition on
// the passed Pod (which VK then reports to the API server).
func (h *Handler) markStatus(pod *corev1.Pod, phase corev1.PodPhase, reason, msg string) {
	setPhase(pod, phase, reason, msg, h.nowFn())
}

// recordBlock classifies a Provision failure into a BlockScope and records it for the
// pool's BlocklistTTL, so placement fails over instead of retrying the same candidate.
// A no-op with no blocklist wired, or when the error yields an empty scope.
//
// The provider owns the scope: the handler resolves the requested accelerator off the Pod
// (the error does not carry it) and passes it in, but never assembles the scope itself, so
// scope derivation lives in one place per provider.
//
// Because the accelerator is always stamped, blocks err NARROW: a genuinely region-wide
// error is narrowed to this accelerator too, since the error text cannot be told apart
// from an instance-type shortage. The cost is one wasted re-probe by a sibling
// accelerator, which is the right trade — over-broad would exclude serviceable
// accelerators. DenyAll (auth/quota) ignores the accelerator: it fails for all.
func (h *Handler) recordBlock(ctx context.Context, pod *corev1.Pod, err error) {
	if h.blocklist == nil {
		return
	}
	// Pool and region are properties of the REQUEST, not the error, so we resolve them off
	// the Pod. The key is the POOL identity (type:count, e.g. "H100:8") — the same key
	// placement queries a candidate by, since a block filed under any other key would
	// never be read and failover would re-place onto the candidate that just failed.
	//
	// The pool, NOT the provider's resolved SKU, because one launch may span several
	// interchangeable instance types (AWS fleets) and only fails when every one is dry, so
	// the pool truthfully names the request whichever alternate was tried. Distinct
	// (type, count) pools stay on distinct keys, so an H100:8 shortage never excludes
	// H100:1. "" means "not applicable" — a CPU-only Pod, or a region-simple provider.
	accel, count, _ := util.AcceleratorRequest(pod)
	accelerator := util.AcceleratorPool(accel, count)
	region := pod.Annotations[nebulav1alpha1.RegionAnnotation]
	scope := h.prov.ClassifyProvisionError(err, accelerator, region)

	if scope == (provider.BlockScope{}) {
		// An error we do not know how to blocklist. Do not install a wildcard block that
		// would exclude the whole provider.
		return
	}

	// TTL = base (pool policy or default) + jitter, so Pods that failed for the SAME scope
	// do not all re-probe the just-freed candidate at once. Coalescing keeps the latest
	// expiry, so jittered records spread the shared deadline instead of pinning it.
	ttl := blocklistTTL(pod) + h.jitterFn()
	// The ctx logger, because it carries the virtualNode/provider values attached
	// upstream; a fresh context.Background() would fall back to the global delegate and
	// could be dropped before the real sink is installed.
	logf.FromContext(ctx).WithName("vnode-blocklist").Info(
		"recording blocklist entry for failed placement",
		"provider", h.prov.Name(), "scope", scope, "ttl", ttl.String(), "error", err.Error())
	h.blocklist.Record(h.prov.Name(), scope, ttl)
}

// blocklistTTL reads the pool's BlocklistTTL off the annotation placement stamped,
// falling back to defaultBlocklistTTL when it is absent or unparseable — including a
// non-positive value, which would otherwise install a permanent block.
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

// Tracks reports whether this virtual node is the one running the Pod. The kubelet API
// has one listener for every provider and its routes carry no node name, so this is how a
// request finds its handler; see kubelet.go.
func (h *Handler) Tracks(namespace, podName string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.tracked[key(namespace, podName)]
	return ok
}

// instanceFor returns the external instance backing a Pod, for the kubelet routes that
// need one. Both failures are NotFound, since both mean "there is nothing to read or run
// against": the Pod is not tracked here, or it is but Provision has not returned an id
// yet.
func (h *Handler) instanceFor(namespace, podName string) (string, error) {
	h.mu.Lock()
	tp, tracked := h.tracked[key(namespace, podName)]
	var instance string
	if tracked {
		instance = tp.instance
	}
	h.mu.Unlock()

	if !tracked {
		return "", errdefs.NotFoundf("pod %q is not known to virtual node %q",
			key(namespace, podName), NodeName(h.prov.Name()))
	}
	if instance == "" {
		return "", errdefs.NotFoundf("pod %q has no external instance yet", key(namespace, podName))
	}
	return instance, nil
}

// GetContainerLogs serves `kubectl logs` for a Pod on this virtual node (kubelet.go
// carries the endpoint). Three things must line up, each failing as NotFound: the Pod is
// TRACKED here, it has an instance id (one still inside Provision has nothing to read),
// and the provider implements provider.LogStreamer (logs are optional).
//
// containerName is IGNORED: a Nebula Pod is one external instance with one console, so
// there is no per-container stream to select.
func (h *Handler) GetContainerLogs(
	ctx context.Context, namespace, podName, containerName string, opts vkapi.ContainerLogOpts,
) (io.ReadCloser, error) {
	streamer, ok := h.prov.(provider.LogStreamer)
	if !ok {
		return nil, errdefs.NotFoundf("provider %q does not support container logs", h.prov.Name())
	}

	instance, err := h.instanceFor(namespace, podName)
	if err != nil {
		return nil, err
	}

	logf.FromContext(ctx).WithName("vnode-logs").V(1).Info("streaming container logs",
		"provider", h.prov.Name(), "pod", key(namespace, podName), "container", containerName,
		"instanceID", instance, "follow", opts.Follow, "tail", opts.Tail)

	// The provider stream is option-free by contract; kubeletLogStream applies the
	// kubelet's options and owns teardown. ctx is passed so a disconnected client releases
	// the stream.
	src, err := streamer.Logs(ctx, instance)
	if err != nil {
		return nil, fmt.Errorf("stream logs for instance %s: %w", instance, err)
	}
	return kubeletLogStream(ctx, src, opts), nil
}

// RunInContainer serves `kubectl exec` for a Pod on this virtual node: it starts cmd in
// the external instance and hands the streams to runExec, which owns the pumping.
//
// Same three preconditions as logs, each a NotFound: the Pod is tracked here, it has an
// instance, and the provider implements provider.Executor (exec is optional — a backend
// with no way into the box simply cannot serve it).
//
// containerName is IGNORED, as for logs: a Nebula Pod is one external instance, so there
// is no second container to pick.
func (h *Handler) RunInContainer(
	ctx context.Context, namespace, podName, containerName string, cmd []string, attach vkapi.AttachIO,
) error {
	executor, ok := h.prov.(provider.Executor)
	if !ok {
		return errdefs.NotFoundf("provider %q does not support exec", h.prov.Name())
	}
	if len(cmd) == 0 {
		return errdefs.InvalidInput("exec: no command given")
	}

	instance, err := h.instanceFor(namespace, podName)
	if err != nil {
		return err
	}

	logf.FromContext(ctx).WithName("vnode-exec").V(1).Info("running command in instance",
		"provider", h.prov.Name(), "pod", key(namespace, podName), "container", containerName,
		"instanceID", instance, "command", cmd, "tty", attach.TTY())

	// Starting is bounded, running is not: a provider may wait for the instance to be
	// ready (Modal polls for a task id for five minutes), and a client staring at a blank
	// terminal that long is worse than being told the box is not ready. Only the start is
	// capped — the command itself runs under ctx, for as long as the client stays.
	startCtx, cancelStart := context.WithTimeout(ctx, execStartTimeout)
	defer cancelStart()

	proc, err := executor.Exec(startCtx, instance, cmd, provider.ExecOptions{TTY: attach.TTY()})
	if err != nil {
		return fmt.Errorf("start command in instance %s: %w", instance, err)
	}
	return runExec(ctx, proc, attach)
}

// --- Unused nodeutil.Provider surface --------------------------------------
//
// Attach/stats/port-forward are out of scope: beyond logs and exec, Nebula does not proxy
// a workload's console. These satisfy the nodeutil.Provider interface and return NotFound
// so the VK core reports them cleanly rather than panicking.

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
