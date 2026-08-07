# Nebula Architecture

This is the design for where Nebula is going: one Kubernetes API surface over every
GPU workload class:

- sandboxes and agents, notebooks
- inference and services
- training and fine-tuning jobs

All running on remote instances across NeoClouds and hyperscalers.

Contents:

- [Goals and non-goals](#goals-and-non-goals)
- [Workload classes](#workload-classes)
- [Decisions](#decisions)
  - [1. Dial-out, not a mesh](#1-dial-out-not-a-mesh)
  - [2. The kubelet API lives on the virtual node](#2-the-kubelet-api-lives-on-the-virtual-node)
  - [3. SandD is PID 1](#3-sandd-is-pid-1)
  - [4. A Sandbox CRD, above the Pod](#4-a-sandbox-crd-above-the-pod)
  - [5. Inbound is a provider capability](#5-inbound-is-a-provider-capability)
- [Roadmap](#roadmap)

---

## Goals and non-goals

**Goals**

- Kubernetes native API surface for every workload class
- `kubectl logs` / `exec` / `attach` against a remote instance, natively.
  offer no native exec API and no inbound reachability.
- In-cluster clients can reach inference and services
- Per-provider code stays small: adding a provider must not mean re-implementing
  logs, exec, or stats.

**Non-goals (for now)**

- **Gang scheduling and intra-gang connectivity** — multi-instance workloads that
  provision all-or-nothing and can reach each other. Deferred, not abandoned: it is
  what training and fine-tuning need.

**Non-goals**

- Multi-container Pods, init containers, and image-level restart on remote
  instances. These need container-runtime access, which not every provider grants.
- An overlay network of any kind (see [Decision 1](#1-dial-out-not-a-mesh)).
- In-place migration of a running workload between providers.

## Workload classes

Three classes, three phases.

| Class | CRD | Inbound to instance | Group | Lifetime | Phase |
|---|---|---|---|---|---|
| sandbox, agent, shell | `Sandbox` | no | 1 | minutes–days | 1 |
| inference, services | Deployment + Service | **yes**, from arbitrary clients | N fungible | long | 2 |
| training, fine-tuning, jobs | Batch Jobs | **yes**, from peers only | N ordinal, all-or-nothing | bounded | deferred |

---

## Decisions

### 1. Dial-out, not a mesh

**Decision.** The in-container SandD opens **one long-lived WSS connection per
instance** to a stable public endpoint, authenticated by a short-lived JWT whose
audience is the workload identity. No overlay network, for any class.

```
   instance (any cloud, behind NAT)          your cluster
   ┌─────────────────────────────┐
   │ container                    │
   │  SandD (PID 1) ──────────────┼──outbound TLS 443──▶ LB ──SNI──▶ WS server
   │   └─ workload as child       │                                  map[claim]conn
   └─────────────────────────────┘
```

**Why the connection must be persistent.** The SandD daemon should already have an established connection when a request arrives, ensuring synchronous operations like `kubectl exec` work seamlessly.

Cost: one idle TLS connection per instance, carrying zero traffic until someone
runs a command. 10k of them is an ordinary WebSocket server in a single process.

**Why not a mesh (headscale/Tailscale).** A mesh solves the same reachability
problem and needs less development work since coordinator already exists, but headscale has a performance issue when reaching around 500 nodes, see [headscale#1656](https://github.com/juanfont/headscale/issues/1656).

![tailnet](./tailnet.png)

**Revisit if** dial-in becomes a product feature: operator SSH into any box, a
pull-based metrics scraper, or instance-to-instance traffic *across* providers.

### 2. The kubelet API lives on the virtual node

**Decision.** Implement the kubelet HTTP surface on the virtual node and serve
logs/exec/attach from it, forwarding over the instance's WebSocket.

```
kubectl exec pod-x
   └─▶ apiserver ──HTTPS──▶ virtual-node kubelet endpoint (Nebula manager)
                               └─▶ map[claim]conn ──▶ SandD ──▶ container
```

**What it buys.** Every consumer works unmodified — `kubectl`, `client-go`, other
languages' clients, k9s/Lens/ArgoCD, our own controllers — and authorization is
RBAC on `pods/exec` and `pods/log`, namespace-scoped, so a tenant's workload can only exec into its own Pods.

Each subresource is a separate handler:

| Subresource | Handler | Phase |
|---|---|---|
| `exec`, `attach` | `RunInContainer`, `AttachToContainer` | 1 |
| `logs` | `GetContainerLogs` | 1 |
| `port-forward` | `PortForward` | later |
| `top` | `GetStatsSummary` | later |

### 3. SandD is PID 1

**Decision.** SandD runs as PID 1 in the workload container and spawns the
workload as its **child**, owning its stdout/stderr pipes.

### 4. A Sandbox CRD, above the Pod

**Decision.** Add one namespaced `Sandbox` CRD with `spec.replicas`. It **creates N
Pods** with stable ordinal identity — StatefulSet-shaped, not Deployment-shaped — and
does not replace or bypass the Pod. No class object, no `type` discriminator. Other
shapes (Notebook, and later a training Job) become their own CRDs when they need
their own spec, each reusing the same Pod path underneath.

```
Sandbox (replicas: 3)
  ├── creates Pod pool-0, pool-1, pool-2  (ownerRef'd, restartPolicy: Never, NO command)
  │      └── gate → placement → nodeSelector → virtual node
  │             └── VK CreatePod → NodeClaim → instance
  └── owns lifecycle: scale, stop/start, TTL, idle-cull, per-replica endpoints
```

**Why replicas.** Two needs, one field: a **warm pool**, because an instance takes
minutes to become Ready while an agent-exec call wants sub-second; and **fan-out**,
one task across 20 boxes without 20 objects. Implement `/scale` so `kubectl scale`
and HPA/KEDA work.

**Ordinal, not fungible.** Each replica is `<sandbox>-<ordinal>` with its own claim
and sessions, so `kubectl exec pool-3` is repeatable. That is why a Deployment is
still wrong despite having replicas: its replicas are interchangeable, and a rolling
update would swap a user's box out mid-session.

**Why a Pod underneath.** The Pod is the carrier of everything already built:
placement, `pkg/failover`'s blocklist, and — critically — the NodeClaim finalizer
that guarantees a paid instance is never leaked. It is also what ResourceQuota
counts, which is how one tenant is stopped from provisioning fifty H100s. Bypassing
it would mean reimplementing all of that.

```yaml
kind: Sandbox
metadata: {name: pool, namespace: team-ml}
spec:
  replicas: 3
  nodePoolRef: gpu                    # existing placement policy object
  template:                           # a PodSpec — the shape of one replica
    metadata:
      labels:
        nebula.inftyai.com/accelerator-type: a100-40gb
    spec:
      containers:
      - name: sandbox
        image: ubuntu:24.04
        # no command: SandD is PID 1
        resources:
          requests: {cpu: "8", memory: 64Gi}
          limits:   {nvidia.com/gpu: "1"}
status:
  replicas: 3
  readyReplicas: 2
  instances:                          # per replica — the addressable unit
  - name: pool-0
    phase: Ready
    endpoints: {exec: ..., pty: ...}
    lastActivityTime: "2026-08-07T09:12:00Z"
  - name: pool-1
    phase: Ready
    lastActivityTime: "2026-08-07T09:40:00Z"
  - name: pool-2
    phase: Provisioning
```

**A PodSpec template, not flat fields.** The controller synthesizes Pods, so what it
accepts must eventually *be* a PodSpec — flat fields would mean re-inventing
`resources`, `env`, `volumeMounts`, and `securityContext` one at a time. Resources
matter most: the GPU count is a standard `nvidia.com/gpu` limit, which is already
where placement and the scheduler's fit check read it from, so a parallel
`accelerator` count field would fork the source of truth. The accelerator *type*
stays a label, exactly as on a hand-written Nebula Pod, so both go through identical
placement.

Nebula fills in on the synthesized Pod: the opt-in labels, the scheduling gate, the
virtual-node toleration, `restartPolicy: Never`, and the container command (SandD). A
webhook rejects a template that sets `command` rather than silently dropping it.

**Open question — scale-in must not be ordinal.** StatefulSet removes the highest
ordinal; here `pool-4` may hold a live session while `pool-1` has been idle an hour.
Removal must pick the least recently active replica, with `status.instances` as the
input. So ordinals are stable names but not a removal order, culling leaves ordinal
gaps, and an in-flight exec must either block scale-in briefly or fail clearly.

---

## Roadmap

Each phase builds the seam the next one needs. Nothing is built twice.

### Phase 1 — Sandbox, agent, shell

*Goal: `kubectl exec`/`logs` work against a remote box, driven by a CRD.*

### Phase 2 — Inference and services

*Goal: `my-llm.default.svc` resolves to an off-cluster instance.*

1. Named addresses + `Reachability` (if not pulled into Phase 1).
2. `Capabilities` access flags; per-workload SandD opt-in.
3. **Real readiness probing** gating the Ready condition — a correctness fix.
4. EndpointSlice bridge: selector-less Service + Nebula-managed endpoints.
5. Per-provider inbound: Modal native (nearly free), AWS security groups.

### Phase 3 — Training, fine-tuning, jobs

*Goal: multi-instance, all-or-nothing, mutually reachable.*

1. **Gang scheduling** — N instances all-or-nothing, so a partially provisioned
   8-node job does not bill while waiting. `NodeClaim.spec.podRef` is 1:1
   (`api/v1alpha1/nodeclaim_types.go:18`), so this needs a group concept. It is the
   one genuinely new primitive in this document.
2. **Intra-gang L3** — shared subnet, placement group, EFA/RDMA, allow-from-self.
   Provider-native, never an overlay.
3. **Rank injection** — `RANK`, `WORLD_SIZE`, `MASTER_ADDR`, stable ordinals.
4. **Job semantics** — exit code → terminal `Succeeded`/`Failed`. Mostly free from
   Phase 1's PID-1 SandD, which already reaps the child; `applyState` needs to
   represent it.
5. **Shared storage** for checkpoints — the same missing provider field as notebook
   persistence. Nothing in the AWS adapter wires block devices today.

Logs and exec come free from Phase 1: same SandD, same connection.

---
