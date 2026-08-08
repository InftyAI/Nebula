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
- [Alternatives](#alternatives)
  - [1. Dial-out, not a mesh](#1-dial-out-not-a-mesh)
- [Roadmap](#roadmap)

---

## Goals and non-goals

**Goals**

- Support AWS, Modal as starting providers, with more to come.
- `kubectl logs` / `exec` / `attach` against a remote instance, natively.
  offer no native exec API and no inbound reachability.
- In-cluster clients can reach inference and services
- Per-provider code stays small: adding a provider must not mean re-implementing
  logs, exec, or stats.

**Non-goals (for now)**

- Gang scheduling and intra-gang connectivity — multi-instance workloads that
  provision all-or-nothing and can reach each other. Deferred, not abandoned: it is
  what training and fine-tuning need.
- More providers than AWS and Modal. The architecture should be provider-agnostic, but
  the implementation will be provider-specific.

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

## Implementation Details

### Authentication

### SandD

### Virtual Kubelet

## Alternatives

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

---

## Roadmap

Each phase builds the seam the next one needs. Nothing is built twice.

### Phase 1 — Sandbox, agent, shell

*Goal: `kubectl exec`/`logs` work against a remote box, driven by a CRD.* Support AWS as the first provider.

### Phase 2 — Inference and services

*Goal: `my-llm.default.svc` resolves to an off-cluster instance.* Support Modal as the first provider.

### Phase 3 — Training, fine-tuning, jobs

*Goal: multi-instance, all-or-nothing, mutually reachable.*
