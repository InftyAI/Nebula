# Nebula Architecture

Nebula is a Kubernetes control plane for GPU-as-a-Service: it schedules GPU
workloads across heterogeneous compute providers ("NeoClouds" — RunPod, Modal,
CoreWeave, Lambda — and vanilla Kubernetes) as if they were one cluster. A user
submits an ordinary `Deployment`/`Pod`; Nebula decides *which provider* runs it
based on cost, availability, and policy, provisions the external instance, and
reclaims it on teardown so a paid GPU is never leaked.

This document is the top-level design. It is descriptive of decisions already
made and prescriptive for the parts not yet built (marked **PLANNED**). For
building and deploying the manager (including provider credentials), see
[docs/deploy.md](deploy.md).

- [Goals and non-goals](#goals-and-non-goals)
- [System overview](#system-overview)
- [The placement flow](#the-placement-flow-end-to-end)
  - [Status flow — two independent lanes](#status-flow--two-independent-lanes)
- [Components](#components)
  - [Scheduling-gate webhook](#1-scheduling-gate-webhook)
  - [Placement controller](#2-placement-controller-done)
  - [Virtual Kubelet node](#3-virtual-kubelet-node-done)
  - [NodeClaim controller](#4-nodeclaim-controller)
  - [NodePool controller](#5-nodepool-controller)
  - [Provider abstraction](#6-provider-abstraction)
  - [Optimizer & poll loop](#7-optimizer--poll-loop-planned)
- [CRDs](#crds)
- [Failure domains & HA](#failure-domains--ha)
- [Build status](#build-status)

---

## Goals and non-goals

**Goals**

- One Kubernetes API surface over many GPU clouds. Users write standard Pods;
  they never call a provider SDK.
- Cost/availability-aware placement with failover: try the cheapest viable
  {capacity tier, provider} first, fall back on capacity errors.
- Never leak a paid instance: teardown is guaranteed by a finalizer even if the
  node/Pod disappears.
- Provider quirks stay behind one narrow seam; the control plane is
  provider-agnostic.

**Non-goals (v1)**

- Region/zone placement. v1 targets NeoClouds, which are region-simple. Region
  is a planned additive field, not modeled yet.
- Bin-packing many pods per instance. One workload → one external instance.
- In-place migration. Recovery from preemption is delete-and-recreate.

---

## System overview

Nebula borrows two proven patterns and fuses them:

- **Virtual Kubelet (VK)** provides the *mechanism*: each provider is a single
  virtual Node in the cluster. A Pod scheduled to that node is provisioned on
  the NeoCloud via that provider's API instead of by a container runtime.
- **Karpenter-style CRDs** provide the *policy + ledger*: `NodePool` (which
  providers, how to choose) and `NodeClaim` (a durable, cluster-scoped record of
  one external instance. The virtual kubelet owns provisioning and the happy-path
  teardown; the claim outlives the Pod and carries a finalizer so it can act as a
  level-triggered teardown *backstop* when the Pod is force-deleted during a VK
  outage — a paid GPU is never leaked).
- **Scheduling gates** are the *placement hook*: a mutating webhook holds an
  opted-in Pod until Nebula's placement controller picks a provider, then
  releases it to the native scheduler.

```
                    ┌──────────────────────────────────────────────────────┐
                    │                    Nebula manager                     │
   kubectl apply    │  ┌────────────┐  ┌─────────────┐  ┌────────────────┐  │
   Deployment ──────┼─▶│  webhook   │  │  placement  │  │   NodePool /   │  │
   (labeled         │  │ (gate Pod) │  │ controller  │  │NodeClaim ctrls │  │
    enabled=true)   │  └────────────┘  └─────────────┘  └────────────────┘  │
                    │        │                │                  │          │
                    └────────┼────────────────┼──────────────────┼──────────┘
                             │                 │                  │
                    gate added│      nodeSelector+ungate│  provision/terminate
                             ▼                 ▼                  ▼
                    ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
                    │ SchedulingGated│  │native scheduler│  │  Provider    │
                    │      Pod      │──▶│ binds to vnode │──▶│  seam (Get/  │
                    └──────────────┘   └──────────────┘   │ Provision..) │
                                              │            └──────┬───────┘
                                              ▼                   │
                                     ┌─────────────────┐          ▼
                                     │ Virtual Kubelet │   ┌──────────────┐
                                     │ node (1/provider)│──▶│  NeoCloud API│
                                     │  CreatePod →    │   │ (RunPod/Modal│
                                     │   provision     │   │  /CoreWeave) │
                                     └─────────────────┘   └──────────────┘
```

Everything runs in one controller-runtime manager binary (`cmd/main.go`) today.
The VK node processes are the one component that may run separately — see
[Virtual Kubelet node](#3-virtual-kubelet-node-done).

---

## The placement flow (end to end)

Follow a single GPU Pod from `kubectl apply` to a running instance:

1. **Opt-in.** User creates a Pod (usually via a Deployment) carrying the label
   `nebula.inftyai.com/enabled: "true"` and the label
   `nebula.inftyai.com/accelerator-type: a100-40gb` (accelerator type), with the
   count expressed as a standard `nvidia.com/gpu` resource on the container
   (omit it for a single GPU). The Pod is the *source of truth* for the workload
   shape — image, command, env, resources, the accelerator type (label) and count
   (`nvidia.com/gpu`); keeping the count on the standard resource means the
   scheduler's fit check and provisioning read the same number.

2. **Gate (webhook).** The mutating webhook, scoped by an objectSelector to
   opted-in Pods outside system namespaces, injects the scheduling gate
   `nebula.inftyai.com/provider-selection` and a key-only `Exists` toleration for
   the virtual-node taint `nebula.inftyai.com/provider:NoSchedule`. The Pod is
   now `SchedulingGated`; the native scheduler will not bind it. `failurePolicy=
   Fail` — a Pod that should be placed by Nebula must never slip past ungated.

3. **Place (placement controller).** Watches SchedulingGated Pods with our
   gate. For each opted-in, gated, not-yet-bound Pod it resolves the Pod's
   `NodePool` (via the `nebula.inftyai.com/nodepool` label), selects a provider
   (v1 policy: **first matching provider** — the first in the pool's provider list
   whose catalog offers the Pod's GPU type; a CPU-only Pod matches any), and then,
   **in this order**:
   - creates the `NodeClaim` **first** — named deterministically
     `<namespace>-<name>`, back-referencing the Pod by UID, recording the chosen
     provider, capacity tier, and pool. The claim is the durable teardown ledger,
     so it must exist *before* the Pod can bind and provision. `Create` is
     idempotent on the fixed name, but idempotency is **UID-scoped**: if a claim
     of that name already exists, placement ungates only when it pins *this* Pod's
     UID (a genuine retry after a crash between create and ungate). If it still
     pins a prior same-named Pod (recreated faster than the backstop reaped the old
     claim), placement does **not** ungate — it requeues until the NodeClaim
     backstop sees the old Pod is gone, terminates that instance, and deletes the
     stale claim, after which the create succeeds with the correct UID. This is
     what makes "the claim exists before bind/provision" a guarantee that the
     claim names *this* workload's instance, not a stale one.
   - then, in one Pod update: adds `nodeSelector:
     {nebula.inftyai.com/provider: <chosen>}`, writes the chosen tier to
     `nebula.inftyai.com/capacity-type` (the one provisioning input not otherwise
     on the Pod), and removes the gate.

   If no pool resolves, or no provider in the pool can serve the Pod, the Pod is
   **left gated** (no guessing); a later reconcile — a pool edit that adds a
   matching provider re-enqueues its gated Pods — can place it.

4. **Bind.** With the gate gone and a provider nodeSelector set, the native
   scheduler binds the Pod to that provider's virtual Node.

5. **Provision (VK).** Binding writes `spec.nodeName = nebula-<provider>`;
   that provider's VK node observes the Pod via its node-scoped informer and fires
   `CreatePod`. It reads the workload off the Pod, builds a
   `ProvisionRequest{ClaimName, CapacityType}` (ClaimName derived deterministically
   from the Pod, CapacityType from the annotation), and calls `provider.Provision`.
   **The virtual kubelet owns provisioning** — the placement controller never
   calls Provision; it only routes the Pod. The observed instance state is
   projected onto the Pod's status.

6. **Observe (poll loop in VK).** The VK handler's poll loop calls
   `provider.List()` periodically (no NeoCloud pushes preemption events) and
   pushes observed instance state onto Pod status via VK's `NotifyPods`. The
   NodeClaim controller does **not** mirror this finer runtime status — the Pod is
   the source of truth for it. The claim tracks only a coarse guard: once its
   served Pod is observed, the claim is marked `Bound` (see step 7).

7. **Teardown (VK happy path + NodeClaim backstop — DONE).** Two layers guarantee
   the paid instance is reclaimed:
   - **Happy path (VK):** when the Pod/Deployment is deleted, VK calls
     `DeletePod`, which runs `provider.Terminate(instanceID)`.
   - **Backstop (NodeClaim):** the claim is cluster-scoped and *outlives* the Pod,
     and carries the `TerminateInstance` finalizer. The NodeClaim controller is
     level-triggered: it marks a claim `Bound` once it has seen the served Pod
     (the durable guard), and when a **Bound** claim's Pod later vanishes it treats
     that as a real teardown and self-deletes; the finalizer then resolves the
     provider, finds the instance by **claim name via `List()`** (not
     `status.InstanceID`, which VK tracks in-memory and loses on a crash), and
     calls `Terminate` before releasing. On the happy path this is a redundant
     idempotent no-op (VK already terminated, so `List` finds nothing); its whole
     reason to exist is the case where the Pod is force-deleted while that
     provider's VK is down, so `DeletePod` never runs. A claim that was **never**
     Bound and whose Pod is absent is treated as possible cache lag: it waits a
     `placementGracePeriod` (2m) before being deleted, so a fresh claim isn't
     reaped in the window between create and the Pod appearing.

### Status flow — two independent lanes

The single most subtle part of the design: **the Pod and the NodeClaim are
populated from different sources and never mirror each other.** Runtime status
lives on the Pod; the claim tracks only a coarse teardown guard.

```
  Pod.status  ◀── (pkg/vnode)      NodeClaim.status ◀── (NodeClaim controller)
  ═══════════════════════════      ═══════════════════════════════════════════
  source: the provider instance    source: whether the served Pod EXISTS
                                    (never the instance's runtime state)

  edge-triggered (instant):        Reconcile(Pod present/absent):
    CreatePod → PodPending           Pod present  → phase=Bound   (one-way latch)
    Provision err → PodFailed        Pod absent + wasBound → self-delete → backstop
    DeletePod → PodSucceeded         Pod absent + !Bound + grace → self-delete
                                     Pod absent + !Bound + in grace → requeue
  level-triggered (poll, §7):
    List() → match by ClaimName
      InstanceRunning    → PodRunning, Ready=True, PodIP=endpoint
      InstancePending    → PodPending, Ready=False
      InstanceFailed     → PodFailed
      InstanceTerminated → PodFailed/"Preempted"  (also: absent from List)
```

- **Pod lane (`pkg/vnode`).** The happy-path transitions are pushed
  synchronously — `CreatePod` emits `Pending` (or `Failed`) the moment
  `Provision` returns, `DeletePod` emits a terminal phase — so provisioning-start
  and teardown do **not** wait on any poll. The poll loop's *only* job is the
  out-of-band transitions nothing pushes: an instance becoming reachable
  (`Pending→Running`) and, the costly one, a spot reclaim (`Running→Terminated`).
  `applyState` (`status.go`) is the sole mapping from `provider.InstanceState` to
  a standard Pod phase/condition, so `kubectl get pod` shows Running/Ready/PodIP.

- **NodeClaim lane (NodeClaim controller).** The claim deliberately does **not**
  copy the instance's runtime state. Its phase is only `Pending → Bound`, where
  `Bound` is a one-way latch meaning "I have observed the served Pod at least
  once." That latch is the *arming signal for the teardown backstop*: a `Bound`
  claim whose Pod later vanishes is a real teardown (self-delete → finalizer
  terminates the instance), whereas a claim that never reached `Bound` and has no
  Pod is treated as possible cache lag and waits out `placementGracePeriod`.

**Why the split.** An earlier design had the claim mirror the instance state
(the still-present but now-unused `NodeClaimProvisioning/Running/Preempted/
Terminating` constants are its residue). It was dropped because it put two
pollers — VK writing the Pod, the claim controller writing the claim — on the
same fact from different lag windows, so they disagreed during the poll window
for no benefit: the claim needs only "did the Pod ever exist" + "is it gone now"
to do its one job. `NodeClaimStatus.InstanceID`/`Endpoint` fields exist but are
currently unwritten; the backstop re-derives the instance via `List()` +
`ClaimName` precisely so teardown survives a VK crash that lost the in-memory id.

**Failover (PLANNED).** If `Provision` fails (e.g. RunPod reports no capacity),
the provider classifies the error into a `BlockScope` (this accelerator+tier, or
the whole provider for auth/quota). The optimizer will exclude that scope for
`BlocklistTTL`; the Deployment recreates the Pod (gated again) and the controller
ungates it toward the next candidate. v1's "first matching provider" selection
does not yet consult a blocklist — a failed Provision surfaces on the Pod and is
retried against the same first match until the optimizer lands.

---

## Components

### 1. Scheduling-gate webhook

A mutating (defaulting) webhook on the external `core/v1` Pod type. On CREATE it
injects the `provider-selection` scheduling gate **and** a key-only `Exists`
toleration for the virtual-node taint (`nebula.inftyai.com/provider:NoSchedule`),
when and only when:

- the Pod carries `nebula.inftyai.com/enabled: "true"`;
- the Pod is not already scheduled (`spec.nodeName == ""` — the API server
  rejects adding a gate to a bound Pod).

Both mutations are idempotent (the gate is not duplicated; a matching provider
toleration is not re-added). The toleration is injected here rather than by the
placement controller because it must be present before the scheduler evaluates
the Pod, and it is provider-agnostic (Exists matches whichever provider the
placement controller later selects).

**Scoping is critical** and lives in `config/webhook/selector_patch.yaml` (a
kustomize patch, since controller-gen markers cannot express selectors):

- `objectSelector` on `enabled=true` — only opted-in Pods reach the webhook;
- `namespaceSelector` excluding `kube-system`, `kube-node-lease`, `kube-public`,
  `nebula-system`.

This matters because `failurePolicy=Fail`: an over-broad webhook would make
*every* Pod creation in the cluster depend on the Nebula webhook being healthy.
The narrow selector + verbs=CREATE-only keeps the blast radius to opted-in Pods.

### 2. Placement controller

The consumer of the gate — the middle of the critical path. Reconciles Pods that
are opted-in, still hold our gate, and are not yet bound (`needsPlacement`
filters out anything else, so it is a no-op for ordinary Pods). For each:

- Resolve the target `NodePool` (Pod label `nebula.inftyai.com/nodepool`). Missing or
  mislabeled → leave the Pod gated (an operator sees a `SchedulingGated` Pod and a
  missing pool, rather than a silent mis-placement).
- **Select a provider.** v1 policy is **first matching provider**: walk the pool's
  providers in listed order and pick the first whose registered adapter offers the
  Pod's GPU type (`MapAccelerator`); a CPU-only Pod matches any. `selectPlacement`
  is the seam the richer optimizer (LowestPrice/Weighted, capacity-tier fallback,
  blocklist) swaps in behind later — the surrounding flow does not change. No
  match → leave the Pod gated.
- **Create the `NodeClaim` first, then ungate** (ordering is load-bearing — see
  the [placement flow](#the-placement-flow-end-to-end) step 3). The claim carries
  a fixed, Pod-derived name so `Create` is idempotent across retries; ungating is
  a single Pod `Update` that sets the provider `nodeSelector`, the `capacity-type`
  annotation, and removes the gate.

A pool edit re-enqueues that pool's still-gated Pods (`podsForPool`), so adding a
provider that can now serve a stuck Pod retries placement promptly instead of
waiting for the resync. A "gated too long" alert remains a required operational
safeguard (see [HA](#failure-domains--ha)).

### 3. Virtual Kubelet node

This is the mechanism that turns "a Pod bound to a fake node" into "an instance
running on a NeoCloud". **Decision: one static virtual Node per provider**, and
**the virtual kubelet owns provisioning** (CreatePod → Provision, DeletePod →
Terminate). Implemented in `pkg/vnode` (`handler.go`, `node.go`, `status.go`).

#### Why one node per provider

VK's native model is one node with many pods. The alternative — a node per
instance — means building your own node-fleet/lease manager on top of VK
(registering and deregistering a node per workload, each with its own heartbeat).
We chose the static per-provider node: far simpler, VK-native, and the natural
unit for a provider that is really "a pool of capacity we rent from". The
accepted cost is a single failure domain per provider (if a provider's VK
process is down, that provider can't take new pods) — acceptable because
placement can route to other providers. Note the tradeoff of "VK owns
provisioning": because the happy-path teardown is `DeletePod →
provider.Terminate`, reclaiming an instance on that path depends on that
provider's VK being alive to process the Pod deletion. This gap is closed by the
**NodeClaim teardown backstop** (see [NodeClaim controller](#4-nodeclaim-controller)):
the cluster-scoped claim outlives the Pod and carries a finalizer, so if the Pod
is force-deleted while VK is down, the level-triggered claim controller still
reclaims the instance — found by claim name via `List()` — the next time it runs.

#### The node object

Each provider VK registers a Node with:

- name `nebula-<provider>` (e.g. `nebula-runpod`);
- label `nebula.inftyai.com/provider: <name>` — the key the placement
  controller writes into a Pod's nodeSelector to route it here;
- a **taint** `nebula.inftyai.com/provider=<name>:NoSchedule` so that *only*
  Pods Nebula has explicitly routed land on it. The mutating webhook adds a
  key-only `Exists` toleration for `nebula.inftyai.com/provider:NoSchedule` at
  admission (the provider is not chosen yet, so it tolerates any value); the
  placement controller then routes the Pod with a provider nodeSelector. Without
  the taint, any unscheduled Pod could drift onto a NeoCloud node.
- generous/opaque `capacity` (VK convention) — real capacity is enforced by the
  provider API and the optimizer's availability table, not by node allocatable.

#### The handler: VK events → the provider seam

VK asks us to implement `node.PodLifecycleHandler`. Each method is a thin
adapter onto the existing `provider.Provider` seam — **no provider-specific
logic lives in the VK layer**; it only translates between Pod events and the
seam:

VK asks us to implement `node.PodLifecycleHandler` (+ `PodNotifier`). Each method
is a thin adapter onto the existing `provider.Provider` seam — **no
provider-specific logic lives in the VK layer**; it only translates between Pod
events and the seam:

| `PodLifecycleHandler` method | Nebula action |
|---|---|
| `CreatePod(pod)` | Derive `ClaimName` from the Pod (namespace-name); read `CapacityType` from the `capacity-type` annotation; call `provider.Provision(pod, req)`; track the returned `instanceID` and set Pod status Pending. |
| `UpdatePod(pod)` | No-op for immutable workloads in v1 (a spec change is a new Pod); the tracked copy's metadata is refreshed. |
| `DeletePod(pod)` | Call `provider.Terminate(instanceID)` (idempotent), report a terminal status, and drop the Pod from tracking. This is the happy-path teardown; the NodeClaim finalizer is the backstop when VK never sees the delete. |
| `GetPod` / `GetPodStatus` | Return the tracked Pod / its status. |
| `GetPods` | Return the Pods this node is tracking. |
| `NotifyPods(cb)` | Start the poll loop; `provider.List()` on the provider's cadence (`Capabilities.PollInterval`, default 30s) matches instances to tracked Pods by claim name and pushes state changes (Running/Failed/Preempted) through `cb`. |

Because Provision is idempotent (keyed on `ClaimName`, derived deterministically
from the Pod), a `CreatePod` retry after a crash re-attaches to the existing
instance instead of double-provisioning.

The poll cadence is **per-provider** (`Capabilities.PollInterval`, default 30s),
because the trade-off differs by provider: the poll only exists to catch
transitions nothing pushes (`Pending→Running`, and preemption
`Running→Terminated`), so a spot-heavy backend where reclaims are common and
costly wants a short interval to notice them quickly, while an OnDemand-only
backend that never preempts (e.g. Modal) can poll lazily. The happy-path
transitions (provision start, teardown) are emitted synchronously by
`CreatePod`/`DeletePod` and do not wait on the poll. The cost of a faster
cadence is `List()` call volume — one API call per provider per tick regardless
of Pod count — so the real ceiling is the provider's rate limit, not ours.

The kubelet API surface (logs/exec/attach/stats/port-forward) is intentionally
unsupported — Nebula routes external GPU workloads, it does not proxy their
consoles — so those handler methods return `NotFound`. This also lets us wire the
lower-level `node` package directly rather than the `nodeutil` convenience
wrapper, whose kubelet HTTP/auth stack does not compile against the pinned k8s
0.33 line.

#### Where VK runs

VK is a long-running process that registers a node and maintains its lease. Two
options:

- **A (chosen):** run the per-provider VK nodes *inside* the existing manager,
  as `manager.Runnable`s (`vnode.Runner`) the manager starts. One binary, one
  deployment, simplest ops. They share the manager's lifecycle and, if enabled,
  leader election, so only the leader owns each node's lease.
- **B:** a separate Deployment per provider. Stronger isolation (a crash-looping
  Modal adapter can't take down the RunPod node) at the cost of N deployments
  and duplicated wiring.

**Implemented as A.** `setupVirtualNodes` in `cmd/main.go` adds one
`vnode.Runner` per registered provider. Revisit B if provider isolation becomes
a real operational need.

#### Interaction with capacity types

A provider VK node exists once, but a Pod's capacity tier (Spot/OnDemand) is a
per-instance decision the optimizer made. Because the VK provisions solely from
the Pod, the tier rides on the `nebula.inftyai.com/capacity-type` annotation
(written by the placement controller) — *not* on the node. `CreatePod` reads it
into the `ProvisionRequest`. This keeps "one node per provider" compatible with
"different pods on the same provider use different tiers".

### 4. NodeClaim controller

The VK owns provisioning and the *happy-path* teardown; the NodeClaim controller
is the **level-triggered backstop** that guarantees no paid instance leaks even
when the Pod → VK path never fires (Pod force-deleted while that provider's VK is
down). The claim is cluster-scoped and outlives the Pod, and carries the
`TerminateInstance` finalizer.

Reconcile logic:

- **Add the finalizer first.** A claim with no finalizer gets it (and nothing
  else) on the first reconcile, so teardown is guaranteed from the moment the
  claim exists.
- **Bound guard.** Once the served Pod (matched by UID) is observed, the claim is
  marked `Bound`. `Bound` is the durable signal "an instance was really serving
  this workload" — the guard the self-delete trusts.
- **Self-delete on real teardown.** A `Bound` claim whose Pod has since vanished
  is a real teardown: the claim deletes itself (the finalizer keeps it alive long
  enough to run the backstop).
- **Cache-lag guard.** A claim that was **never** `Bound` and whose Pod is absent
  might just be racing the Pod's cache appearance. It waits `placementGracePeriod`
  (2m) from creation before being reaped, so a freshly-placed claim isn't deleted
  in the window between create and the Pod showing up.
- **Deletion path (`reconcileDelete`).** Resolve the provider (unknown adapter →
  release the finalizer rather than wedge deletion forever), find the instance —
  preferring `status.InstanceID` if set, else matching the **claim name** against
  `provider.List()` (VK tracks the id in-memory and loses it on a crash, so `List`
  is the reliable recovery path) — call `Terminate` (idempotent), then release the
  finalizer. A transient `List` error does **not** release the finalizer: teardown
  retries so the instance is never abandoned.

On the happy path the backstop is a redundant idempotent no-op — VK already
terminated, so `List` finds nothing and `Terminate("")` does nothing. The claim
deliberately does **not** mirror the Pod's finer runtime status; the Pod is the
source of truth for that, and `kubectl get pods` shows it.

### 5. NodePool controller

The NodePool is **pure policy** (which providers, capacity-tier order, ranking
strategy, failover TTL) — no workload shape. The controller is its health &
observability loop:

- **validate** — the one *environmental* check: every referenced provider has a
  registered adapter. Surfaced as `Ready=False / UnknownProvider`, not an
  admission rejection, so a valid manifest stays appliable while a provider's
  creds are temporarily absent (self-heals on the next reconcile).
- **refreshPlaced** — recomputes `status.Placed` (count of `Bound` NodeClaims
  per provider for this pool — `Bound` being the claim-level signal that an
  instance is live for the workload) each reconcile; watches NodeClaims via
  `claimToPool` so the picture tracks instances coming and going.

*Static* spec validation (e.g. "Weighted strategy requires a weight on every
provider") is enforced at **admission** via a CEL `XValidation` rule on
`NodePoolSpec`, not in the controller — a spec property is knowable at CREATE and
belongs there. The split — static→admission, environmental→status condition — is
deliberate: coupling a fail-closed webhook to the runtime provider registry
would reject valid pools during creds/rollout windows.

### 6. Provider abstraction

The single narrow seam between the provider-agnostic control plane and the
heterogeneous cloud APIs. Key rules:

- **The Pod is the source of truth.** `Provision(pod, req)` reads
  image/command/env/ports/resources off the Pod, the accelerator type from its
  `accelerator-type` label and the count from the `nvidia.com/gpu` resource
  (parsed together by `util.AcceleratorRequest`). `ProvisionRequest` carries only
  the two things *not* on the Pod: `ClaimName` (identity) and `CapacityType` (the
  optimizer's tier choice).
- **Quirks are data, not branches.** `Capabilities{SupportsStop, SupportsSpot,
  NativeTags, PreemptionNotice}` lets the control plane behave generically.
- **Poll-based detection.** `List()` returns all Nebula-owned instances in as
  few calls as possible — the engine of the poll loop, since no NeoCloud pushes
  preemption.
- **Error classification.** `ClassifyProvisionError(err) → BlockScope` maps a
  failure to its blocklist granularity ({accel, tier} vs whole-provider).
- Shared catalog machinery (`catalog.Lookup`, embeddable `catalog.Base` for
  Name/Offerings/identity-MapAccelerator) lives in `pkg/provider/catalog`.

Adapters register into a process-wide registry (`pkg/provider/registry.go`) at
startup via `registerProviders` in `cmd/main.go`. A creds-absent provider is
logged and skipped, not fatal.

**Modal adapter** (`pkg/provider/modal/`): serverless Sandboxes, create/terminate
only (no stop/spot), native tags (claim id in a sandbox tag), OnDemand-only. SDK
sits behind a `Client` seam so the adapter is unit-testable without network. The
sandbox is built from the Pod as source of truth: image/command/env, accelerator
type+count (from the annotation), CPU/memory (from the container's resource
request), exposed ports (from `containerPorts`, surfaced as the endpoint tunnel),
and lifetime (from `activeDeadlineSeconds`, defaulting to 24h — Modal treats a
zero timeout as its 5-minute default, so the adapter always sets one). Credentials
come from a per-provider Secret (`nebula-modal-credentials`) injected via an
optional `envFrom` — the SDK reads `MODAL_TOKEN_ID`/`MODAL_TOKEN_SECRET` from the
environment; see `config/manager/modal-credentials.example.yaml`. Still open:
`env.valueFrom` (Secret/ConfigMap-backed env) is not yet projected, and there is
no live integration test.

### 7. Optimizer & poll loop (PLANNED)

- **Catalog table.** `{provider, accelerator, capacityType} → {price, available}`,
  built from provider `Offerings()` (static CSV catalog + live availability
  probe), cached and periodically refreshed.
- **Placement resolution — two orthogonal axes, fixed order.** Capacity type is
  the *outer* axis (hard tier): try Spot on *all* providers before *any*
  OnDemand. Strategy (LowestPrice/Ordered/Weighted) is the *inner* axis: it only
  ranks providers *within* the active tier, never across tiers.
- **Blocklist.** Derived, high-churn, in-memory (not in any CRD). Keyed on
  {provider, accelerator, capacityType} with wildcard-on-empty, so a failed H100
  request doesn't block A100 on the same provider. Entries expire after
  `FailoverPolicy.BlocklistTTL` (default 10m). Failover is always on.
- **Poll loop (DONE, in VK).** Each provider's virtual node calls `List()` on the
  provider's cadence (`Capabilities.PollInterval`, default 30s) and projects
  observed instance state onto **Pod** status — preemption/termination detected by
  an instance disappearing or changing state. It does **not** write NodeClaims (an
  earlier mirror-into-claim design was dropped — see
  [Status flow](#status-flow--two-independent-lanes)). What remains PLANNED here is
  the *optimizer's* offerings/availability refresh, not the pod-status poll.

---

## CRDs

Both are **cluster-scoped**, under `nebula.inftyai.com/v1alpha1`. Karpenter's
names are kept (not prefixed); disambiguation is via the API group and distinct
shortnames (`np`, `nc`). See `api/v1alpha1/`.

### NodePool (`np`) — policy

```yaml
spec:
  providers:            # ordered; order matters for the Ordered strategy
    - name: runpod
      weight: 3         # required iff strategy == Weighted (CEL-enforced)
    - name: modal
  capacityTypes:        # OUTER axis, fallback order. default {Reserved,OnDemand,Spot}
    - Spot
    - OnDemand
  strategy: Ordered     # INNER axis: LowestPrice | Ordered | Weighted
  failover:
    blocklistTTL: 10m
status:
  placed: {runpod: 2, modal: 1}   # running NodeClaims per provider
  conditions: [{type: Ready, ...}]
```

A pool is pure policy and applies to whatever Pods select it — an H100 and an
A100 Deployment can share one pool.

### NodeClaim (`nc`) — instance ledger

```yaml
spec:
  podRef: {namespace, name, uid}   # UID pins the exact Pod
  provider: runpod                 # immutable; a claim never migrates
  capacityType: Spot               # tier chosen by the optimizer, recorded for reporting
                                   # (the VK reads it from the Pod's capacity-type annotation)
  poolRef: gpu-pool
status:
  phase: Bound                     # Pending (Pod not yet observed) | Bound (Pod observed ≥ once)
  instanceID: <provider id>        # provider instance id, when VK recorded it (backstop hint)
```

Deliberately does **not** duplicate PodSpec/GPU/tier as workload data, nor mirror
the Pod's finer runtime status — the Pod is the source of truth for both. The
claim's own status is just the coarse `Bound` guard the teardown backstop relies
on. The claim is a durable teardown ledger + backstop, keyed on the Pod-derived
claim name; happy-path teardown is still the VK's `DeletePod → Terminate`.

---

## Failure domains & HA

Two components are on the fail-closed critical path and must run HA:

- **Webhook** (`failurePolicy=Fail`): if it's down, opted-in Pod creation is
  blocked. Run ≥2 replicas; the narrow objectSelector keeps non-Nebula Pods
  unaffected. A gated-Pod path that stays gated is preferable to an ungated Pod
  bypassing placement.
- **Placement controller**: if it never ungates, opted-in Pods sit
  `SchedulingGated` forever. Required safeguard: a **"gated too long" alert** and
  ideally a fallback (e.g. after a timeout, emit an event / optionally place on a
  default provider).

Non-critical by design:

- **VK node down**: that provider can't take *new* pods, and existing instances
  keep running untouched (the external NeoCloud does not care that VK is down).
  Placement routes new pods to healthy providers. Happy-path teardown of that
  provider's instances is deferred until its VK recovers — but a Pod force-deleted
  during the outage is still reclaimed by the NodeClaim finalizer backstop, so no
  instance leaks (see [NodeClaim controller](#4-nodeclaim-controller)).
- **Provider creds absent**: the adapter is skipped at registration (logged);
  pools referencing it surface `UnknownProvider` and self-heal when creds arrive.

Leader election (`LeaderElectionID: nebula.inftyai.com`) ensures a single active
manager owns reconciliation and the VK node leases (the VK nodes run inside the
manager as `Runnable`s — option A).

---
