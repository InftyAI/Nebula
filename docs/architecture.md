# Nebula Architecture

Nebula is a Kubernetes control plane for GPU-as-a-Service. It lets users submit
ordinary Pods or Deployments, then places each opted-in GPU workload onto an
external compute provider through Kubernetes-native scheduling gates, virtual
nodes, and durable placement records.

The current manager attempts to register the Modal and AWS providers, plus an
in-memory `fake` provider for e2e tests when explicitly enabled. The provider
seam is meant to support more backends, but this document describes the
implementation that is in the repository today. Planned or partial work is
called out in
[Current implementation status](#current-implementation-status). For deployment
and credential setup, see [docs/deploy.md](deploy.md); for how an instance's
lifecycle becomes Pod and NodeClaim status (including each provider's own status
mapping), see [docs/status.md](status.md).

- [Goals and Non-Goals](#goals-and-non-goals)
- [System Overview](#system-overview)
- [Placement Flow](#placement-flow)
  - [Status Flow](#status-flow)
- [Components](#components)
  - [Scheduling-Gate Webhook](#1-scheduling-gate-webhook)
  - [Placement Controller](#2-placement-controller)
  - [Virtual Kubelet Node](#3-virtual-kubelet-node)
  - [NodeClaim Controller](#4-nodeclaim-controller)
  - [NodePool Controller](#5-nodepool-controller)
  - [Provider Abstraction](#6-provider-abstraction)
  - [Placement Optimizer and Poll Loop](#7-placement-optimizer-and-poll-loop)
- [CRDs](#crds)
- [Observability](#observability)
- [Failure Domains and HA](#failure-domains-and-ha)
- [Current Implementation Status](#current-implementation-status)

---

## Goals and Non-Goals

**Goals**

- One Kubernetes API surface over external GPU clouds. Users describe workload
  shape with normal Pod fields; they do not call provider SDKs directly.
- Capacity/provider/region-aware placement with failover. The current placement
  walk tries capacity tiers first, then providers, then each provider's regions,
  skipping recently failed candidates through a TTL blocklist.
- No leaked paid instances. The virtual kubelet handles happy-path teardown and a
  cluster-scoped NodeClaim finalizer acts as the level-triggered backstop.
- Provider quirks stay behind a narrow seam: capabilities, offerings,
  accelerator mapping, provisioning, list, terminate, and error classification.

**Non-goals in the current implementation**

- Provider-neutral geography. `ProviderSpec.Regions` accepts shared *group* tokens
  (`us`, `eu`, `ap`), but the regions behind them are per-provider and the narrower
  names are each cloud's own vocabulary — there is no global region namespace. Which
  level a value is, is resolved by the provider (`ExpandRegions`); an omitted list
  means every region it serves.
- Price-ranked region choice. Within a capacity tier the expanded regions are walked
  in order, not ranked: the catalog carries no per-region prices, so a wide
  declaration cannot yet prefer the cheapest region. Modal is the sharper case — a
  pinned region there costs 1.5x (group) or 1.75x (narrow) over its unconstrained
  default, which the catalog does not model.
- Failover on a provider that *queues*. The blocklist has exactly one writer: the
  virtual kubelet's `Provision` error path. A provider that reports a capacity
  shortfall synchronously (AWS `CreateFleet`) therefore fails over across zone,
  region and tier, but one that ACCEPTS the request and queues for capacity returns
  an instance id and no error — so nothing is blocklisted and placement is never
  re-driven. Modal is that case: a `[modal, aws]` pool never advances to AWS on
  capacity, and the Pod waits in Modal's queue at `Initializing` (which, per
  [status](status.md#modal), is indistinguishable from booting). Modal also collapses
  its regions into a single candidate, so it has no intra-provider region failover
  either. Making failover live for such a provider needs a reserved-by deadline that
  synthesizes `ErrNoCapacity` — the classification side already handles it.
- Bin-packing multiple unrelated Pods onto one external instance. The current
  model is one workload Pod to one external instance.
- In-place migration. Recovery from reclaim, failure, or spec changes is
  delete-and-recreate.

---

## System Overview

Nebula combines three Kubernetes patterns:

- **Scheduling gates** hold opted-in Pods until Nebula chooses a placement.
- **Virtual Kubelet** exposes one virtual Node per provider. When the native
  scheduler binds a Pod to `nebula-<provider>`, the provider's virtual kubelet
  provisions an external instance instead of starting a local container runtime.
- **Karpenter-style CRDs** separate policy from the instance ledger:
  `NodePool` says where and how to place; `NodeClaim` records the durable
  identity of one external instance and owns the terminate finalizer.

```
kubectl apply Pod/Deployment
        |
        v
  +------------------+       +-----------------------+
  | mutating webhook | ----> | SchedulingGated Pod   |
  | adds gate/taint  |       | not yet schedulable   |
  +------------------+       +-----------------------+
                                      |
                                      v
                           +-----------------------+
                           | placement controller  |
                           | NodePool -> candidate |
                           | NodeClaim first       |
                           | nodeSelector + ungate |
                           +-----------------------+
                                      |
                                      v
                           +-----------------------+
                           | native scheduler      |
                           | binds to nebula-aws   |
                           | or nebula-modal       |
                           +-----------------------+
                                      |
                                      v
                           +-----------------------+
                           | provider VK handler   |
                           | CreatePod -> Provision|
                           | DeletePod -> Terminate|
                           | List poll -> PodStatus|
                           +-----------------------+
                                      |
                                      v
                           +-----------------------+
                           | Modal Sandbox / EC2   |
                           | instance              |
                           +-----------------------+
```

Everything runs in one controller-runtime manager binary (`cmd/main.go`) today.
Virtual nodes are started as manager runnables, one per registered provider. The
same manager also owns `Sandbox` and `SandboxSet` controllers, which synthesize
Nebula-managed Pods for long-lived interactive boxes.

---

## Placement Flow

Follow one GPU Pod from creation to teardown:

1. **Opt in.** A user creates a Pod, usually through a Deployment, with
   `nebula.inftyai.com/enabled: "true"` and a
   `nebula.inftyai.com/nodepool: <pool>` label. GPU type is requested with
   `nebula.inftyai.com/accelerator-type`; GPU count is the standard
   `nvidia.com/gpu` resource. The Pod remains the source of truth for image,
   command, env, ports, CPU, memory, accelerator type, and accelerator count.

2. **Gate at admission.** The mutating webhook adds the scheduling gate
   `nebula.inftyai.com/provider-selection` and a key-only `Exists` toleration for
   the virtual-node taint `nebula.inftyai.com/provider:NoSchedule`. The webhook
   is scoped by object and namespace selectors so only opted-in non-system Pods
   are affected.

3. **Select placement.** The placement controller watches opted-in Pods that
   still hold the gate and are not yet bound. It resolves the Pod's NodePool,
   parses the accelerator request, then walks candidates in this order:

   ```text
   for each capacityType in pool.spec.capacityTypes:     # outer axis
     for each provider in pool.spec.providers:           # listed order today
       for each region in ExpandRegions(provider.regions): # provider-local axis
         skip unregistered providers
         skip providers that do not offer the accelerator type/count
         skip providers that cannot serve the tier (Modal has no Spot)
         skip candidates blocked by failover blocklist
         choose the first remaining candidate
   ```

   The inner axis is whatever the provider's `ExpandRegions` returns, which is not
   one iteration per declared region: AWS expands a group token into many candidates,
   while Modal collapses every declared region into a single candidate carrying them
   all (so its inner loop always runs exactly once, and the chosen `region` may be a
   joined token rather than one region name). An empty expansion still yields one
   unconstrained `""` candidate so the walk runs.

   `Ordered` is the only strategy the API accepts, and the inner ranking is listed
   order. `LowestPrice` and `Weighted` exist as constants but are deliberately kept
   out of the enum until the ranking is implemented — admitting a strategy the walk
   ignores would let a pool claim a policy it does not get. The placement flow is
   already structured so price or weight ranking can replace the inner ordering
   without changing the rest of the controller; widening the enum is the switch.

4. **Create the ledger first.** Before the Pod can bind, the controller creates a
   deterministic NodeClaim named from the Pod namespace/name. The claim records
   the Pod UID, provider, capacity type, provider-local region, accelerator pool
   (`type:count`), and pool name. If a same-named claim exists for an older Pod
   UID, the new Pod stays gated until the NodeClaim backstop reaps the stale
   claim.

5. **Ungate atomically.** In one Pod update, the placement controller:

   - writes `spec.nodeSelector[nebula.inftyai.com/provider] = <provider>`;
   - writes `nebula.inftyai.com/capacity-type` when the chosen tier is explicit;
   - writes `nebula.inftyai.com/region` when the chosen region is explicit;
   - writes `nebula.inftyai.com/blocklist-ttl` when the pool sets one;
   - removes only Nebula's scheduling gate.

   If no candidate can serve the Pod, the Pod is left gated. If candidates are
   only blocked by failover TTLs, the controller requeues for the soonest expiry.

6. **Bind through Kubernetes.** Once the gate is gone, the native scheduler sees
   the provider nodeSelector and binds the Pod to `nebula-<provider>`.

7. **Provision in the virtual kubelet.** The provider's VK handler observes the
   bound Pod and calls `CreatePod`. It derives the claim name from the Pod, reads
   `CapacityType` and `Region` from annotations, builds
   `ProvisionRequest{ClaimName, CapacityType, Region}`, and calls
   `provider.Provision`. The placement controller never provisions directly.

8. **Observe by polling.** Each virtual node periodically calls `provider.List()`
   and matches instances back to tracked Pods by claim name. The default cadence
   is 15 seconds; providers can override it through `Capabilities.PollInterval`.
   Observed instance state is projected onto Pod status and endpoint metadata.

9. **Fail over on provision errors.** When `Provision` fails, the VK handler asks
   the provider to classify the error into a `BlockScope`, using the accelerator
   and region from the failed request. The shared blocklist records that scope for
   the Pod's blocklist TTL plus jitter. The Pod is marked Failed; the placement
   controller deletes terminal controller-owned Pods so their owner recreates a
   fresh gated Pod that skips the blocked candidate.

10. **Teardown.** The happy path is edge-triggered by VK `DeletePod`, which calls
    `provider.Terminate(instanceID)`. The backstop is level-triggered by the
    NodeClaim finalizer: once the served Pod is gone, the claim self-deletes, its
    finalizer resolves the provider, finds the instance by recorded instance ID
    or by claim name via `List()`, calls `Terminate`, and only then releases.

### Status Flow

Pod status and NodeClaim status come from different sources. The Pod is the
runtime surface users watch. The NodeClaim is the coarse ledger that protects
teardown.

```
Pod.status, written by pkg/vnode
  CreatePod success        -> Pending / Provisioning
  CreatePod error          -> Failed / ProvisionFailed
  List sees Pending        -> Pending / Initializing
  List sees Running        -> Running / Ready=True / endpoint annotation
  List sees Failed         -> Failed / Failed
  List misses instance     -> Failed / Terminated
  DeletePod success        -> Succeeded / Terminated

NodeClaim.status, written by NodeClaim controller
  present Pod, no instance yet       -> Provisioning
  present Pod, initializing instance -> Initializing
  present Running Pod                -> Bound
  present deleting Pod               -> Terminating
  present terminal Pod               -> Terminated
  absent after Bound/Terminating      -> delete self -> terminate finalizer
  absent before Bound                 -> wait placementGracePeriod, then delete
```

Important details:

- `Bound` is the teardown guard. Once a claim has seen a Running Pod, a later Pod
  disappearance is trusted as real teardown, not cache lag.
- `Provisioning` and `Initializing` do not earn the guard. If the Pod is absent
  before `Bound`, the controller waits `placementGracePeriod` (15 seconds) before
  deleting an orphaned claim.
- `NodeClaimStatus.InstanceID` is recorded on a best-effort basis. The finalizer
  prefers it when present, but can still recover by matching provider instances
  by claim name through `List()`.
- NodeClaim does not mirror logs, restarts, container state, or fine-grained
  runtime health. Those belong on the Pod.

The full mapping — Pod phase/reason and the claim phase each produces, plus each
provider's own status vocabulary and the limits of what is observable — lives in
[docs/status.md](status.md).

---

## Components

### 1. Scheduling-Gate Webhook

The webhook mutates external `core/v1` Pods on CREATE only. When a Pod carries
`nebula.inftyai.com/enabled: "true"` and is not already scheduled, it adds:

- scheduling gate `nebula.inftyai.com/provider-selection`;
- toleration for key `nebula.inftyai.com/provider` with operator `Exists` and
  effect `NoSchedule`.

The toleration must be present before native scheduling starts, while the
provider value is not known yet. That is why it is a key-only toleration from the
webhook rather than a provider-specific toleration from the placement controller.

The webhook uses `failurePolicy=Fail`, so selector scoping is load-bearing:
objectSelector limits it to opted-in Pods and namespaceSelector excludes system
namespaces.

### 2. Placement Controller

The placement controller is the consumer of Nebula's scheduling gate. It is a
no-op for ordinary Pods and for Pods that have already been bound.

Responsibilities:

- resolve the selected NodePool from the Pod's `nebula.inftyai.com/nodepool`
  label;
- parse the accelerator type/count from Pod label plus `nvidia.com/gpu`;
- select the first currently usable candidate across capacity tier, provider,
  and provider-local region;
- consult the shared failover blocklist before selecting a candidate;
- create or verify the Pod UID-pinned NodeClaim before ungating;
- stamp provider, capacity type, region, and blocklist TTL onto the Pod;
- remove Nebula's scheduling gate.

A NodePool edit re-enqueues still-gated Pods that reference that pool, so adding
a provider or region can unstick Pods without waiting for a full resync.

### 3. Virtual Kubelet Node

Nebula uses one static virtual Node per registered provider. The node name is
`nebula-<provider>` and it carries:

- label `nebula.inftyai.com/provider: <provider>`;
- taint `nebula.inftyai.com/provider=<provider>:NoSchedule`;
- opaque capacity large enough for scheduling, while real capacity is enforced by
  placement and provider provisioning.

`pkg/vnode` implements `node.PodLifecycleHandler` and `PodNotifier`.

| VK method | Nebula action |
| --- | --- |
| `CreatePod(pod)` | Build `ProvisionRequest{ClaimName, CapacityType, Region}`, call `provider.Provision`, track returned instance ID, mark Pod Pending/Provisioning. |
| `UpdatePod(pod)` | Treat workload shape as immutable; refresh tracked metadata/spec while preserving computed status. |
| `DeletePod(pod)` | Call `provider.Terminate(instanceID)`, mark Pod Succeeded/Terminated, drop local tracking. |
| `GetPod` | Return tracked Pod, or re-adopt a live provider instance by claim name after a VK restart. |
| `GetPodStatus` / `GetPods` | Return tracked status or tracked Pods. |
| `NotifyPods(cb)` | Start the provider `List()` poll loop and push status/endpoint changes through VK's callback. |

Of the kubelet API surfaces, only **container logs** is implemented. `kubectl logs` is
proxied by the API server to the node's kubelet endpoint, so `pkg/vnode/kubelet.go`
serves one HTTPS listener in the manager pod and every virtual node advertises it as its
`status.addresses` InternalIP plus `status.daemonEndpoints` port. It is a
`manager.Runnable`, hence leader-scoped — correct, because only the leader holds the
tracked Pods a log request resolves against. A provider opts in by implementing
`provider.LogStreamer`; one that does not answers `NotFound`. See
[kubelet-api.md](kubelet-api.md) for the transport, the trust model, and which kubectl
flags are honoured.

Exec, attach, stats, and port-forward are not implemented — those need an agent inside
the container, so Nebula places external workloads but does not proxy their consoles.

#### Interaction with Capacity Types

Capacity type is a per-instance decision, not a node property. The placement
controller writes `nebula.inftyai.com/capacity-type` onto the Pod, and
`CreatePod` passes it through `ProvisionRequest`. This lets multiple Pods on the
same provider virtual node use different purchase models.

#### Interaction with Regions

Region is also a per-instance decision. The placement controller writes
`nebula.inftyai.com/region` when the selected candidate has a region, and the VK
passes it through `ProvisionRequest.Region`.

Region-simple providers use an empty region candidate. AWS is region-aware and
must be configured with explicit regions in each `NodePool` provider entry. One
AWS provider registration spans all declared and already-provisioned regions by
building region clients lazily and sweeping the union during `List()` and
`Offerings()`.

### 4. NodeClaim Controller

The NodeClaim controller is the teardown backstop. A NodeClaim is cluster-scoped,
outlives the namespaced Pod, and carries the
`nebula.inftyai.com/terminate-instance` finalizer.

Reconcile behavior:

- add the terminate finalizer before doing anything else;
- fetch the served Pod by namespace/name and UID;
- set coarse phase from the served Pod: `Provisioning`, `Bound`, `Terminating`, or
  `Terminated`;
- best-effort record `status.instanceID` by matching the provider instance by
  claim name;
- when a previously observed Pod disappears, delete the claim so the finalizer
  runs teardown;
- when a never-bound claim's Pod is absent, wait 15 seconds to protect against
  informer cache lag, then delete the orphan claim.

Deletion behavior:

- resolve the provider from `spec.provider`;
- prefer `status.instanceID` when available;
- otherwise call `provider.List()` and find the instance whose `ClaimName`
  matches the Pod-derived claim name;
- call idempotent `provider.Terminate`;
- release the finalizer only after termination succeeds, or immediately if the
  provider is not registered and the controller cannot make progress.

### 5. NodePool Controller

NodePool is policy: allowed providers, allowed provider regions, capacity tier
order, placement strategy, and failover TTL. The controller supplies health and
observability rather than doing placement itself.

Responsibilities:

- validate environment-dependent provider availability and surface
  `Ready=False / UnknownProvider` when a referenced adapter is not registered;
- compute `status.placed` from Bound NodeClaims per provider;
- watch NodeClaims so placement counts update as instances come and go.

Static spec rules are admission-time CEL validations. Examples: `Weighted`
requires a weight on every provider entry, and AWS provider entries require at
least one region.

### 6. Provider Abstraction

`pkg/provider.Provider` is the only seam between provider-agnostic controllers
and provider-specific APIs.

Key contracts:

- `Provision(pod, req)` reads workload shape from the Pod and only receives the
  decisions that are not already on the Pod: claim identity, capacity type, and
  region.
- `Terminate(instanceID)` is idempotent, including empty or already-gone IDs.
- `List()` returns Nebula-owned instances for polling, re-adoption, and teardown
  recovery.
- `Offerings()` returns price/availability rows, including provider-specific
  region where applicable.
- `MapAccelerator(type, count)` maps a canonical accelerator request to provider
  capacity pools. Count is part of the key for providers such as AWS where
  different GPU counts map to different instance types.
- `ClassifyProvisionError(err, accelerator, region)` returns the provider-owned
  blocklist scope for failover.

**Modal adapter** (`pkg/provider/modal/`) provisions Modal Sandboxes. It is
OnDemand-only, supports native tags, and maps Pod image, command, env, ports,
CPU/memory, accelerator type/count, and lifetime into a Sandbox request.
`env.valueFrom` projection and live integration coverage are still incomplete.

**AWS adapter** (`pkg/provider/aws/`) provisions EC2 GPU instances. It uses the
NodePool-declared regions as the fan-out source, lazily builds per-region
clients, resolves region-local GPU AMIs, uses EC2 fleets for Spot or OnDemand,
records claim/capacity/region tags, and confines capacity/quota blocklist scopes
to the failing region when appropriate.

**Fake adapter** (`pkg/provider/fake/`) is an in-memory backend used by tests and
e2e overlays only. It is not registered in the default production deployment.

### 7. Placement Optimizer and Poll Loop

Implemented today:

- capacity type is the outer axis;
- provider list order is the current inner ranking;
- provider-local region is walked inside each provider entry;
- provider `Offerings()` and `MapAccelerator()` decide servability;
- failover blocklist skips recently failed provider/accelerator/tier/region
  candidates;
- VK poll loop observes provider instance state and updates Pod status.

Planned or partial:

- `LowestPrice` and `Weighted` ranking are API-visible but not implemented as
  richer inner ranking yet;
- no metrics-based scheduler feedback loop exists yet;
- no all-regions expansion exists yet;
- no warm pool or reservation controller exists yet;
- accelerator-family fallback beyond provider-local alternates is not a
  scheduler feature yet.

---

## CRDs

All CRDs are under `nebula.inftyai.com/v1alpha1`.

- `NodePool` (`np`) is cluster-scoped placement policy.
- `NodeClaim` (`nc`) is a cluster-scoped instance ledger.
- `Sandbox` is a namespaced workload-facing object for one interactive remote
  box.
- `SandboxSet` is a namespaced controller object that maintains N Sandboxes.

### NodePool (`np`)

```yaml
apiVersion: nebula.inftyai.com/v1alpha1
kind: NodePool
metadata:
  name: gpu-pool
spec:
  providers:
    - name: modal
      weight: 1
    - name: aws
      weight: 3
      regions:
        - us-east-1
        - us-west-2
  capacityTypes:
    - Spot
    - OnDemand
  strategy: Ordered
  failover:
    blocklistTTL: 30s
status:
  providers: modal,aws
  placed:
    modal: 2
    aws: 1
  conditions:
    - type: Ready
      status: "True"
      reason: Valid
```

When `capacityTypes` is omitted, the API defaults it to `{OnDemand, Spot}`. The
listed order is the fallback order the placement controller walks.

### NodeClaim (`nc`)

```yaml
apiVersion: nebula.inftyai.com/v1alpha1
kind: NodeClaim
metadata:
  name: default-train-a100
spec:
  podRef:
    namespace: default
    name: train-a100
    uid: 5c1c8e4a-...
  provider: aws
  capacityType: Spot
  region: us-east-1
  accelerator: A100-40GB:1
  poolRef: gpu-pool
status:
  phase: Bound
  instanceID: i-0123456789abcdef0
```

Valid phases are `Provisioning`, `Bound`, `Terminating`, and `Terminated`. The
claim deliberately does not duplicate PodSpec and does not mirror fine-grained
runtime status — `Bound` answers existence, not readiness.

### Sandbox and SandboxSet

`Sandbox` and `SandboxSet` are namespaced user-facing abstractions for
interactive boxes. The controllers synthesize Nebula-managed Pods, so they reuse
the same admission, placement, virtual-node provisioning, status, and teardown
paths as direct Pod/Deployment workloads.

---

## Observability

The instrumented surface is the path a Pod takes from admission to a running external
instance: **placement**, then **provisioning**. Those are the parts whose cost and
failure modes are otherwise invisible — placement can silently leave a Pod gated forever,
and provisioning runs against a third party, takes seconds to minutes, bills money, and
fails for reasons the Pod status flattens away. Everything else is covered elsewhere and
deliberately not duplicated: reconcile counts, queue depth and API latency by
controller-runtime's own collectors, Pod-population questions by kube-state-metrics.

Two design rules are worth knowing here, because they constrain the code above:

- Placement and provisioning metrics share **one label set** (`provider`, `region`,
  `capacity_type`, `accelerator`, `accelerator_count`), so a placement and the
  provisioning attempt it led to join in PromQL without label surgery. That is why
  `Handler.metricLabels` and `placementLabels` are field-for-field mirrors.
- Every label is bounded by **configuration** (NodePools, provider catalogs), never by
  workload. Nothing derived from a Pod name, UID, namespace or unresolved user-supplied
  pool label is ever a label value. Cost is no exception, which is why per-instance spend
  is a NodeClaim status field rather than a label on the counter. Cost *attribution* is the
  single opt-in breach of that rule — `--cost-labels` promotes chosen Pod labels onto the
  counter — so it ships off by default, and warns rather than capping when the series count
  says an operator chose badly: a billing metric that rewrote its own labels to defend
  itself would be wrong where nobody could see it.

See [metrics.md](metrics.md) for the full series list, label semantics, example queries
and the two known gaps.

## Failure Domains and HA

Components on the fail-closed path:

- **Webhook.** If the webhook is down, opted-in Pod creation is blocked because
  `failurePolicy=Fail`. Run it with multiple replicas; selector scoping keeps
  non-Nebula Pods out of the blast radius.
- **Placement controller.** If it cannot reconcile, opted-in Pods remain
  `SchedulingGated`. Operators should alert on gated Pods older than the
  expected placement window.

Components designed to degrade without leaks:

- **Provider virtual node down.** That provider cannot take new Pods through VK
  while down. Existing external instances keep running. Happy-path Pod deletion
  may be delayed, but the NodeClaim finalizer backstop can still reclaim
  force-deleted Pods by listing provider instances by claim name.
- **Provider credentials absent.** The adapter is skipped during registration.
  NodePools that reference it surface `UnknownProvider` and self-heal when the
  manager restarts with credentials available.
- **Provider List temporarily failing.** VK skips that poll tick and retries on
  the next cadence. The NodeClaim finalizer does not release on transient list
  errors, so teardown retries instead of abandoning the instance. A `List` failure
  during post-restart re-adoption is likewise treated as *unknown*, not *absent*:
  `Handler.GetPod` returns a non-nil Pod together with a non-NotFound error, which
  suppresses both a duplicate `CreatePod` and a premature `DeletePod` until the
  provider answers. Conflating the two would let one failed list mark a healthy Pod
  `Failed` for reaping while the paid instance kept running behind a zero instance id.
- **Provider unreachable during `Provision`.** Every provision failure fails the Pod, so
  placement can fail over rather than sit behind an attempt nothing re-enters. What the
  failure's *kind* still decides is the blocklist, and `provider.ClassifyError` is the
  single answer: a transport error or 503 files nothing, because it is not evidence about
  the candidate — the provider may well have accepted the request. An exhausted provision
  timeout is candidate-scoped; other candidate decisions (no capacity, quota, unsupported
  accelerator) also file an entry, and only auth widens it to the whole provider.
  `ClaimName`, so a re-provision adopts whatever the failed attempt created rather than
  doubling it.

Leader election (`LeaderElectionID: nebula.inftyai.com`) keeps a single active
manager reconciling controllers and owning virtual-node leases.

---

## Current Implementation Status

Implemented:

- Pod admission gate and provider taint toleration.
- Pod placement through NodePool, NodeClaim, provider nodeSelector, and ungate.
- Capacity-tier, provider, and provider-region candidate walk.
- Shared TTL failover blocklist keyed by provider, accelerator pool, capacity
  type, and region.
- Virtual Kubelet provider nodes with CreatePod/DeletePod/List polling.
- NodeClaim terminate finalizer and 15-second cache-lag guard.
- Modal, AWS, and e2e fake providers.
- Sandbox and SandboxSet controllers layered on the same Pod placement path.

Planned or partial:

- `LowestPrice` and `Weighted` ranking semantics.
- Metrics-based scheduling.
- Warm pools and reservation support.
- Arbitrary GPU-count fitting across larger provider instances.
- Richer accelerator-family fallback.
- Modal `env.valueFrom` projection and broader live integration tests.
