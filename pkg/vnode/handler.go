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

// defaultBlocklistTTL is the BASE exclusion for a failed placement when the Pod
// carries no BlocklistTTLAnnotation (an out-of-band Pod, or one placed before the
// annotation existed). It mirrors FailoverPolicy.BlocklistTTL's own default so the
// behaviour is identical whether or not the pool policy reached the handler. The
// effective TTL is this base plus a random jitter of up to blocklistJitter (see
// recordBlock), so it is deliberately short: the jitter, not a long fixed floor,
// is what spreads retries.
const defaultBlocklistTTL = 30 * time.Second

// blocklistJitter is the upper bound on the random delay ADDED to the base TTL for
// every recorded block. Without it, all Pods that failed for one (accelerator,
// tier, region) scope re-block in lockstep and their exclusions lapse at the same
// instant, so they stampede the same just-freed candidate together — and, because
// records coalesce on the LATEST expiry (see Blocklist.Record), the shared entry
// would otherwise carry an identical deadline for all of them. A per-record jitter
// in [0, blocklistJitter) decorrelates those wake-ups so freed capacity is sampled
// across a spread of moments instead of one thundering retry.
const blocklistJitter = 30 * time.Second

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

	// client patches the endpoint annotation onto the Pod's metadata. VK persists
	// only the status subresource (its UpdateStatus never touches metadata), so the
	// reachable address — which must be visible on the Pod for anyone to reach the
	// workload — needs a metadata write of its own. May be nil in tests, where the
	// annotation is set on the in-memory tracked Pod but not persisted.
	client kubernetes.Interface

	// blocklist records failed placements so the placement controller can fail over
	// (zone → region → tier). Shared across every provider's handler and the
	// placement controller. May be nil (a no-op), keeping tests and any
	// blocklist-less wiring simple.
	blocklist Blocklister

	mu sync.Mutex

	// tracked is the poll loop's work list and what GetPod/GetPodStatus serve, keyed
	// by namespace/name.
	//
	// INVARIANT: only pods whose instance the provider has acknowledged (Provision
	// returned an id) or that are already terminal. Never one whose Provision call is
	// still in flight — reconcileOnce maps a tracked pod absent from List() to
	// Terminated, which for a live provision is a wrong, unrecoverable write.
	tracked map[string]*trackedPod

	notify func(*corev1.Pod)

	// nowFn and pollEvery are seams for tests.
	nowFn     func() metav1.Time
	pollEvery time.Duration

	// jitterFn returns the random delay added to a block's base TTL (see
	// recordBlock). A seam so tests can pin it (e.g. to 0) and assert an exact TTL;
	// production wires it to a uniform draw in [0, blocklistJitter).
	jitterFn func() time.Duration
}

// trackedPod is the virtual node's local record of one pod it provisioned for.
// The Pod object is the source of truth for the workload; we additionally hold
// the provider instance id (for Terminate) and the derived claim name (to match
// this pod against the provider's List during polling).
type trackedPod struct {
	pod       *corev1.Pod
	claimName string
	instance  string
	// connectEndpoint is the endpoint value last written to the Pod's metadata
	// annotation via patchEndpoint. The notify callback fires every poll tick, so
	// this dedups the metadata patch to the ticks where the reachable address
	// actually changed (first appearance, or a re-provision) rather than every tick.
	//
	// It is a cache, not a record: losing it (a restart) costs one redundant patch,
	// never a lost value, because the annotation itself lives in etcd. No credential is
	// held here — see persistCredential.
	connectEndpoint string
}

// NewHandler builds a Handler for the given provider backend. The poll cadence
// comes from the provider's Capabilities.PollInterval, falling back to
// defaultPollInterval when the provider does not set one. blocklist is the shared
// failover blocklist a Provision failure is recorded on; it may be nil (no-op).
// client is used to patch the endpoint annotation onto the Pod metadata (VK only
// writes status); it may be nil in tests, where the annotation stays in-memory.
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

	// Bound the provision call so a wedged backend cannot pin this worker forever.
	// The provider may raise the deadline via Capabilities.ProvisionTimeout (AWS
	// does, to leave room for cross-zone failover); zero means "use the default".
	//
	// The deadline is scoped to that ONE call and must not be reused for the writes
	// that follow it. A Provision returning just under the timeout would leave those
	// writes with no time budget at all, so they would fail with DeadlineExceeded on
	// success — and the credential write below cannot be retried, so a timeout there
	// loses the token for good. The follow-up writes stay on the caller's ctx.
	timeout := h.prov.Capabilities().ProvisionTimeout
	if timeout <= 0 {
		timeout = defaultProvisionTimeout
	}
	provisionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	log.Info("provisioning external instance",
		"capacityType", req.CapacityType, "region", req.Region, "timeout", timeout.String())

	// Report Provisioning BEFORE the call. It can run for minutes (AWS sweeps the
	// region's zones on a capacity error), and until it returns this is the only
	// explanation the Pod carries. Emit but do NOT store — see the tracked invariant.
	h.markStatus(pod, corev1.PodPending, reasonProvisioning, "allocating external instance")
	h.emit(pod)

	res, err := h.prov.Provision(provisionCtx, pod, req)
	if err != nil {
		log.Error(err, "provision failed; Pod marked Failed for failover")
		// Record the failure on the shared blocklist so placement fails over to the
		// next candidate (zone → region → tier) instead of hot-looping here. The
		// provider classifies its own error into the precise BlockScope (a Spot
		// no-capacity in one region blocks only that, not OnDemand or other regions;
		// auth/quota blocks the whole provider); the TTL rides on the Pod from the
		// pool's FailoverPolicy.
		h.recordBlock(ctx, pod, err)
		// Surface the failure on the Pod so the placement controller can fail
		// over, and return the error so the pod controller retries with backoff.
		h.markStatus(pod, corev1.PodFailed, reasonProvisionFailed, err.Error())
		h.store(pod, claim, "")
		h.emit(pod)
		return err
	}

	// Only a RESERVED instance advances: capacity is committed and it is booting. An
	// unreserved id (a Modal sandbox accepted but still queued for a GPU) is left at
	// the Provisioning stamped above, which is exactly true — the id is real and must
	// be reclaimed, but nothing is allocated yet. One poll tick later the queued
	// sandbox reads Initializing too, since Modal cannot tell queued from booting (see
	// docs/status.md).
	//
	// store runs either way: an id means the instance exists. markStatus precedes it
	// because store deep-copies — storing first would track a copy without the status
	// just written.
	log.Info("external instance provisioned", "instanceID", res.InstanceID, "reserved", res.Reserved)
	if res.Reserved {
		h.markStatus(pod, corev1.PodPending, reasonInitializing, "external instance is initializing")
	}
	// Stamp a create-time address BEFORE store, because store deep-copies and the
	// tracked copy is what the poll loop re-emits. That is the whole publication: the
	// emit below carries it to the API server through the same notify wrapper the poll
	// loop uses, and every later tick re-offers it until a write lands. No lock: this
	// Pod is not shared until store, VK having handed us a copy of its own.
	setEndpoint(pod, res.ConnectURL)
	h.store(pod, claim, res.InstanceID)

	// The TOKEN, unlike the address, cannot ride the Pod (it would be readable by
	// anyone with `get pod` and sit unencrypted in etcd), so it gets its own write
	// here — the only place it exists, since the provider mints it once and cannot
	// re-read it (see provider.ProvisionResult). It runs after store and before emit so
	// a tracked pod is never observable without its credential, and it does not gate
	// the status: a Pod that provisioned is reported provisioned even if the Secret
	// write failed.
	h.persistCredential(ctx, pod, res.ConnectURL, res.ConnectToken)

	h.emit(pod)
	return nil
}

// persistCredential writes the bearer token, and a copy of the address it
// authenticates against, into a Secret.
//
// It handles ONLY the secret half. The address is published by stamping it on the Pod
// (see CreatePod / setEndpoint), which routes it through the one endpoint write path
// every provider shares; this function exists because the token cannot travel that way.
// An annotation is readable by anyone with `get pod` and lands unencrypted in etcd,
// which is fine for an address and unacceptable for a credential, so the token goes to
// an access-controlled Secret — with the URL alongside it, so the pair is usable from
// one object.
//
// The token is one-shot and unrepeatable: minting is create-only with no read-back, so
// a failed write cannot be retried, here or later. That is the difference from the
// address, which the poll loop keeps re-offering until it lands. The Secret is instead
// written once and never rewritten, so it needs nothing held in memory.
//
// An empty url means the provider mints no credential (AWS, whose address is not known
// until boot and is reported through the observed endpoint instead) or that minting
// failed; an empty token means an address with nothing to authenticate, so a Secret
// would imply a credential that does not exist. Either way there is nothing to write
// and nothing to fail — the poll loop still reports the instance, and an unreachable
// workload is reported unreachable.
//
// Best-effort: a nil client (tests) is a no-op, and a failure is logged, never with the
// token.
func (h *Handler) persistCredential(ctx context.Context, pod *corev1.Pod, url, token string) {
	if h.client == nil || url == "" || token == "" {
		return
	}
	h.createConnectSecret(ctx, pod, url, token)
}

// UpdatePod is a no-op: the external instance's shape is immutable once
// provisioned (recovery from any change is delete-and-recreate, matching the
// NodeClaim ledger's immutability). We still refresh our tracked copy so
// GetPod reflects the latest metadata.
func (h *Handler) UpdatePod(_ context.Context, pod *corev1.Pod) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if tp, ok := h.tracked[key(pod.Namespace, pod.Name)]; ok {
		// Preserve what WE own and the API server does not yet know: the status we
		// compute from the provider, and the endpoint — which may be an address minted
		// at create whose patch has not landed yet, so the incoming Pod would not carry
		// it. Dropping it would discard the only copy (a minted URL is never
		// re-observed) and strand the retry with nothing to replay. Everything else is
		// adopted from the incoming spec/meta.
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
		// A failed Terminate is the leak-risk path: VK will retry DeletePod, but if it
		// never succeeds the NodeClaim backstop is the last line of defense. Log loudly.
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

// GetPod returns the tracked pod, or a NotFound error the pod controller
// understands.
//
// Tracking is in-memory, so a VK restart (redeploy, crash, OOM, leader handoff)
// starts with an empty map even though the external instances are still running.
// VK's createOrUpdatePod calls GetPod to decide adopt-vs-create: a nil result
// makes it re-issue CreatePod, which resets the Pod's status to Provisioning and
// re-drives provisioning. To avoid that regression we RE-ADOPT here — when the
// Pod is not in the map we ask the provider whether an instance with this Pod's
// claim is still live, and if so rebuild the tracking entry from it. VK then sees
// a non-nil pod and takes the UpdatePod (adopt) branch, and the re-tracked pod is
// advanced to its true state by the next poll tick.
func (h *Handler) GetPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	h.mu.Lock()
	tp, ok := h.tracked[key(namespace, name)]
	h.mu.Unlock()
	if ok {
		return tp.pod.DeepCopy(), nil
	}

	// Cold map: re-adopt from the live provider if the instance still exists.
	claim := util.ClaimName(namespace, name)
	inst, found := h.instanceByClaim(ctx, claim)
	if !found {
		return nil, errdefs.NotFoundf("pod %s/%s not found on virtual node", namespace, name)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}}
	applyState(pod, inst.State, inst.Endpoint, h.nowFn())
	h.store(pod, claim, inst.ID)
	logf.FromContext(ctx).WithName("vnode-handler").Info(
		"re-adopted live instance after cold tracking map (VK restart)",
		"provider", h.prov.Name(), "pod", key(namespace, name), "claim", claim,
		"instanceID", inst.ID, "state", inst.State)
	return pod.DeepCopy(), nil
}

// instanceByClaim returns the live provider instance whose ClaimName matches, and
// whether one was found. A List error yields (zero,false): re-adoption then falls
// through to NotFound and VK retries on its next sync rather than acting on a
// half-known fleet. It backs GetPod's post-restart re-adoption.
func (h *Handler) instanceByClaim(ctx context.Context, claim string) (provider.Instance, bool) {
	instances, err := h.prov.List(ctx)
	if err != nil {
		return provider.Instance{}, false
	}
	for _, inst := range instances {
		if inst.ClaimName == claim {
			return inst, true
		}
	}
	return provider.Instance{}, false
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
//
// We WRAP VK's callback rather than store it raw. VK's cb drives its status path
// (enqueuePodStatusUpdate -> UpdateStatus), which writes only the /status
// subresource and silently drops any metadata change on the same object. The
// reachable endpoint must live on the Pod's metadata (PodIP cannot hold a DNS
// name — see applyState), so the wrapper publishes the instance's access details
// with their own writes first, then hands the same Pod to VK for the status write.
// This folds those writes onto the one notify signal: every status push also
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
		}
		// Log the before -> after status every tick. This is the "how does the system
		// look" signal an operator watches: the lifecycle progression (Provisioning ->
		// Initializing -> Running, or -> Terminated on a disappearance) shows as the
		// before/after differing; a steady state shows them equal, which is itself the
		// confirmation the pod is being observed and re-emitted, not stuck.
		log.V(1).Info("observed pod status",
			"pod", key(tp.pod.Namespace, tp.pod.Name), "before", before,
			"after", statusSignature(tp.pod))
		emit = append(emit, tp.pod.DeepCopy())
	}
	notify := h.notify
	h.mu.Unlock()

	// At V(1) so a healthy steady state stays quiet, but the counts are there when
	// pods appear stuck: tracked>0 with matched==0 means List returns instances the
	// claim names don't line up with (the classic "provisioned but never Running").
	log.V(1).Info("poll tick",
		"listed", len(instances), "tracked", tracked, "matched", matched)

	// Re-emit EVERY tracked pod's current status each tick. Notification is otherwise
	// edge-triggered (notify only on a signature change), and VK dedups each emit
	// against the last status IT received from us — never against the API server. So
	// if a single UpdateStatus is dropped (a transient conflict, informer-cache lag),
	// VK believes it already delivered that status and never retries, and an
	// edge-triggered loop never re-sends it: the Pod wedges on a stale status forever
	// (classic: instance is Running but the Pod stays Pending/Initializing). Re-handing
	// VK the unchanged status is cheap — it dedups the identical object without an API
	// write — and, crucially, arms VK's own drift-correction: the dedup sets
	// lastPodStatusUpdateSkipped, so the next informer resync notices the API server
	// disagrees with what we last sent and re-issues the write. This makes status
	// propagation level-triggered and self-healing within one resync period.
	if notify != nil {
		for _, p := range emit {
			notify(p)
		}
	}
}

// setEndpoint stamps a reachable address onto a Pod's annotation — the single stamp
// every path uses, so the annotation has one assignment site regardless of where the
// address came from. The notify wrapper then patches it to the API server (PodIP cannot
// hold a DNS name — see applyState), and its write half (persistEndpoint) reads the
// value back off the emitted Pod knowing nothing about its origin.
//
// Callers, and what each one knows:
//
//   - CreatePod — an address MINTED at create, from ProvisionResult. Modal's connect
//     URL, which exists before the sandbox does and is never reported again.
//   - reconcileOnce — an address OBSERVED at boot, from a listed Instance. AWS assigns a
//     public DNS name only once EC2 has one, so it can only arrive on the read path.
//   - UpdatePod — nothing new; it re-applies what VK's replacement Pod may have
//     dropped.
//
// An empty address is ignored rather than cleared, which is what lets those coexist: a
// provider that published at create and then reports no observed endpoint (Modal), or
// one that momentarily omits the value, never erases a working address. No credential
// is ever stamped here — a token cannot ride the Pod (see persistCredential), and
// cannot be observed at all, being minted once on the create path.
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

// statusSignature is a compact rendering of the Pod status fields the poll loop
// surfaces to the API server, logged before/after each tick so a pod's lifecycle
// progression is visible. It intentionally goes beyond Phase: reason (Provisioning
// -> Initializing -> Running), readiness, and the assigned IP all move WITHIN a
// single phase, so a phase-only view would hide those transitions. Keep this in
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

// persistEndpoint writes the reachable address on the emitted Pod to the API server. It
// is the write half for whichever path stamped it (see setEndpoint): CreatePod for an
// address minted at create, the poll loop for one observed at boot. It runs inside the
// notify wrapper (see NotifyPods), just before VK's status callback, because VK's
// callback writes only the /status subresource and drops everything else on the same
// object.
//
// Because the poll loop re-emits every tracked pod each tick, this is also the retry for
// any patch that failed, including a create-time one: it keeps patching until a write
// succeeds. It carries no credential — a token is written once on the create path (see
// persistCredential), never per tick.
//
// This runs per pod per tick, so anything it does unconditionally is multiplied by the
// whole fleet — hence the dedup below, which narrows it to the ticks where the address
// changed or a previous patch has not yet landed.
//
// Best-effort: a nil client (tests) is a no-op, and a failure is logged and retried
// next tick rather than failing the poll.
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
	// An untracked pod has nothing to dedup against, so its endpoint is patched
	// unconditionally: the annotation is the only place that address is published, and
	// skipping it would strand an unreachable Pod.
	patched := false
	if tracked {
		patched = tp.connectEndpoint == endpoint
	}
	h.mu.Unlock()

	if !patched {
		h.patchEndpoint(ctx, pod, endpoint)
	}
}

// patchEndpoint patches the endpoint annotation onto the Pod metadata. The
// reachable address — which is how anything reaches the workload, and which PodIP
// cannot hold when it is a DNS name (see applyState) — needs this dedicated
// metadata write, since VK's status callback drops metadata. A merge patch scoped to
// the single annotation is a no-op for every other field and so does not collide
// with the status write that follows.
//
// This is the ONLY write of the endpoint annotation, and persistEndpoint its only
// caller, so every address — minted at create or observed at boot — reaches etcd
// through this one merge patch. The annotation is then where the address LIVES, and
// nothing clears it: this is only ever called with a non-empty value, so it survives
// for the Pod's life without the provider being asked for it again.
//
// connectEndpoint advances only on success, so a failed patch is retried on the next
// tick. NotFound is ignored — the Pod is gone.
func (h *Handler) patchEndpoint(ctx context.Context, pod *corev1.Pod, endpoint string) {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{nebulav1alpha1.EndpointAnnotation: endpoint},
		},
	})
	if err != nil {
		// A marshal of a fixed-shape map cannot realistically fail; guard anyway so a
		// future change surfaces rather than panics.
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

// createConnectSecret writes the instance's connect URL and bearer token to a
// Secret in the Pod's namespace, so a consumer can reach the workload with
// `curl -H "Authorization: Bearer $token" $url`.
//
// A Secret, not an annotation: the token authenticates every request to the
// endpoint, and an annotation would expose it to anyone with `get pod` in the
// namespace and store it unencrypted in etcd. The URL is duplicated in here
// alongside it so the pair is usable from one object.
//
// It is ownerReferenced to the Pod, which is what reclaims it: teardown deletes the
// Pod, and the garbage collector removes the Secret with it. Deleting it on the
// DeletePod path instead would leak on every path that skips DeletePod (a force
// delete, a VK outage) — the same reason the NodeClaim finalizer exists.
//
// Write-once, and unrepeatable: this runs on the create path, from the one
// ProvisionResult that ever carries the token (see persistCredential). There is no
// retry loop behind it, because there is nothing to retry WITH — a later attempt has
// no credential to write, since minting is one-shot and cannot be re-read. So a
// failure here means the Secret is missing for this instance's life, and the recovery
// is to replace the instance (delete the Pod), not to poll.
//
// AlreadyExists is therefore treated as success rather than reconciled: it means a
// Secret under this name already exists, which happens when a Pod name is reused
// before the GC has collected the previous owner's Secret. Overwriting is not
// obviously better — the old Secret still belongs to a live-until-collected instance —
// and the stale copy resolves itself when the ownerReference GC removes it.
//
// A failure is logged (never with the token).
func (h *Handler) createConnectSecret(ctx context.Context, pod *corev1.Pod, url, token string) {
	// No UID means the Pod is synthesized (GetPod's re-adoption stub), so an
	// ownerReference would be invalid and the Secret would never be collected. Skip
	// rather than create an unowned Secret that nothing would ever reclaim. In practice
	// the create path always has the real Pod, so this is a guard, not a case.
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
			// UID is what makes this a real ownerReference; a name alone would be
			// ignored by the GC. It is set on any Pod that exists at the API server.
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
		// Loud, because it is not retried: the token cannot be re-minted, so this
		// instance stays credential-less until it is replaced.
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
func (h *Handler) recordBlock(ctx context.Context, pod *corev1.Pod, err error) {
	if h.blocklist == nil {
		return
	}
	// The accelerator pool and region are properties of the REQUEST, not the error;
	// the provider cannot see them, so we resolve them off the Pod and hand them over.
	// The block is keyed on the POOL identity (type:count, e.g. "H100:8") — the SAME
	// key placement resolves and queries a candidate by (selectPlacement uses
	// util.AcceleratorPool). A block filed under any other key would never be queried,
	// and failover would silently re-place onto the candidate that just failed. We key
	// on the pool, NOT the provider's resolved SKU, because a single launch may span
	// several interchangeable instance types (AWS's fleet): the pool stays truthful
	// whichever alternate lands, and a block is only written when the WHOLE launch
	// failed (it succeeds if any alternate had capacity), so the pool key correctly
	// names a request whose every equivalent option is dry. It keeps distinct
	// (type, count) pools on distinct keys, so an H100:8 shortage never excludes
	// H100:1. "" for either means "not applicable" (a CPU-only Pod, or a region-simple
	// provider), which the provider treats accordingly.
	accel, count, _ := util.AcceleratorRequest(pod)
	accelerator := util.AcceleratorPool(accel, count)
	region := pod.Annotations[nebulav1alpha1.RegionAnnotation]
	scope := h.prov.ClassifyProvisionError(err, accelerator, region)

	if scope == (provider.BlockScope{}) {
		// An empty scope means the error is not one we know how to blocklist; do not
		// install a wildcard block that would exclude everything on the provider.
		return
	}

	// Effective TTL = base (pool policy or default) + a per-record jitter, so Pods
	// that failed for the SAME scope do not all re-probe the just-freed candidate at
	// the same instant. Coalescing keeps the latest expiry, so distinct jittered
	// records spread the shared entry's deadline instead of pinning it.
	ttl := blocklistTTL(pod) + h.jitterFn()
	// Use the request-scoped logger from ctx (it carries the virtualNode/provider
	// values controller-runtime attached upstream); a logf.FromContext on a fresh
	// context.Background() would fall back to the global delegate and could be
	// silently dropped before the real sink is installed.
	logf.FromContext(ctx).WithName("vnode-blocklist").Info(
		"recording blocklist entry for failed placement",
		"provider", h.prov.Name(), "scope", scope, "ttl", ttl.String(), "error", err.Error())
	h.blocklist.Record(h.prov.Name(), scope, ttl)
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
