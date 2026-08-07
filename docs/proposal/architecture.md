# Nebula Architecture

This is the design for where Nebula is going: one Kubernetes API surface over every
GPU workload class — sandboxes and agents, notebooks and shells, inference and
services, training and fine-tuning jobs — all running on remote instances across
NeoClouds and hyperscalers.

Nebula turns external GPU capacity into ordinary Pods. Today that stops at
*provisioning*: a Pod is placed, an instance is launched, and status flows back —
but the workload is unreachable. `kubectl logs` and `kubectl exec` return
`NotFound` (`pkg/vnode/handler.go:686`), nothing in the cluster can call a served
model, and there is no way to run several instances as one coordinated group. So
only one of the four classes is really served, and the pieces the other three need
are also the pieces that make the first one good.

This document covers that whole target state and the order to build it in. It is a
proposal: nothing here is built yet. For the architecture as it exists today, see
[docs/architecture.md](../architecture.md) — as each phase ships, its content moves
there and this stays as the record of *why*.

- [Requirement](#requirement)
- [Goals and non-goals](#goals-and-non-goals)
- [Workload classes](#workload-classes)
- [Decisions](#decisions)
  - [1. Dial-out, not a mesh](#1-dial-out-not-a-mesh)
  - [2. The kubelet API lives on the virtual node](#2-the-kubelet-api-lives-on-the-virtual-node)
  - [3. The agent is PID 1](#3-the-agent-is-pid-1)
  - [4. A Sandbox CRD, above the Pod](#4-a-sandbox-crd-above-the-pod)
  - [5. Inbound is a provider capability](#5-inbound-is-a-provider-capability)
- [Provider seam changes](#provider-seam-changes)
- [Roadmap](#roadmap)
- [Risks](#risks)

---

## Requirement

> The experience must be exactly that of creating a Pod in Kubernetes — including
> `kubectl logs` and `kubectl exec` — while the workload actually runs on a remote
> instance in another account, VPC, or cloud.

That sentence drives most of what follows. It rules out a Nebula-specific client
API as the primary surface: consumers must use the standard `pods/log` and
`pods/exec` subresources, with RBAC as the authorization model, so that `kubectl`,
`client-go`, k9s, Lens, ArgoCD, and every language's Kubernetes client work
unmodified.

## Goals and non-goals

**Goals**

- `kubectl logs` / `exec` / `attach` against a remote instance, natively.
- One control-plane mechanism that works on **every** provider, including ones that
  offer no native exec API and no inbound reachability.
- In-cluster clients reach a served model by ordinary Service DNS.
- Multi-instance workloads provision all-or-nothing and can reach each other.
- Per-provider code stays small: adding a provider must not mean re-implementing
  logs, exec, or stats.

**Non-goals**

- Multi-container Pods, init containers, and image-level restart on remote
  instances. These need container-runtime access, which not every provider grants.
- Serving high-volume data traffic through the Nebula control plane.
- An overlay network of any kind (see [Decision 1](#1-dial-out-not-a-mesh)).
- In-place migration of a running workload between providers.

## Workload classes

Four classes, three phases. They differ along two axes only: whether anything must
**dial into** the instance, and whether the instances form a **group**.

| Class | Inbound to instance | Group | Lifetime | Phase |
|---|---|---|---|---|
| sandbox, agent, agent-exec | no | 1 | minutes–days | 1 |
| notebook, shell | no (consumer terminates in-cluster) | 1 | hours–days | 1 |
| inference, services | **yes**, from arbitrary clients | N fungible | long | 2 |
| training, fine-tuning, jobs | **yes**, from peers only | N ordinal, all-or-nothing | bounded | 3 |

Two facts fall out and shape every decision below:

1. **Outbound reachability is the only thing every provider guarantees.** So the
   control plane must be outbound-only, everywhere.
2. **Inbound is needed by exactly two classes, for unrelated reasons** —
   open-ended consumers (inference) versus a closed peer set (training). They get
   different mechanisms; neither gets an overlay.

Note that sandbox is the **highest-count, highest-churn** class, not the lowest.
Scale arguments that assume otherwise get the answer backwards.

---

## Decisions

### 1. Dial-out, not a mesh

**Decision.** The in-container agent opens **one long-lived WSS connection per
instance** to a stable public endpoint, authenticated by a short-lived JWT whose
audience is the workload identity. No overlay network, for any class.

```
   instance (any cloud, behind NAT)          your cluster
   ┌─────────────────────────────┐
   │ container                    │
   │  agent (PID 1) ──────────────┼──outbound TLS 443──▶ LB ──SNI──▶ WS server
   │   └─ workload as child       │                                  map[claim]conn
   └─────────────────────────────┘
```

**Why the connection must be persistent.** A real kubelet needs no such thing: the
apiserver *dials it*, because it has a routable address. A Nebula instance sits
behind NAT in another cloud, so the apiserver cannot dial it and the direction must
flip. Once flipped, the connection has to *already exist* when a request arrives —
`kubectl exec` is synchronous, and there is no other channel over which to ask the
instance to dial in. That is the entire reason, and it is forced by "no inbound",
not chosen. The alternative, polling, would add seconds of latency to every exec
and burn requests from every idle instance.

Cost: one idle TLS connection per instance, carrying zero traffic until someone
runs a command. 10k of them is an ordinary WebSocket server in a single process.

**Why not a mesh (headscale/Tailscale).** A mesh solves the same reachability
problem and charges much more for it:

- a stateful control plane to operate (headscale: SQLite, single-writer, no HA) and
  one failure domain for every instance at once;
- per-workload pre-auth key minting, TTLs, ephemeral-node reaping, and MagicDNS
  name-reclamation races;
- netmap distribution on every join/leave — worst precisely for sandboxes, the
  highest-churn class;
- `tailscaled` inside every workload container, in userspace-networking mode, on a
  rented GPU box.

And the one thing it uniquely provides — a stable address something can *dial
into* — has **no consumer**: the agent only ever initiates, and identity comes from
the JWT audience, not an address. Buying commercial Tailscale would fix the
operational half (HA control plane, working ACLs, an operated DERP fleet) but not
that: it would be paying a vendor for a property no code path uses, plus per-device
billing against extreme churn. The comparison is not "buy Tailscale" versus
"operate headscale properly" — it is "buy Tailscale" versus "a TLS listener and JWT
verification", which is a problem we can delete instead of solve.

For the other classes a mesh is not merely unnecessary but unworkable. Inference
consumers are arbitrary in-cluster Pods and external callers that will never be
mesh members, so mesh IPs in an EndpointSlice are unroutable — a structural break,
not a scale limit. Training's rank-to-rank traffic must stay provider-native,
because userspace encapsulation and reduced MTU land directly on allreduce
bandwidth.

**Revisit if** dial-in becomes a product feature: operator SSH into any box, a
pull-based metrics scraper, or instance-to-instance traffic *across* providers.
Those are the cases an overlay genuinely wins. Even then it is additive — the
JWT-authorized channel is still what authorizes a request, since a mesh
authenticates a *device*, not an action.

**One public front door, SNI-routed.** The agent dials a deterministic name derived
from its own identity (`sbx-<uid>.example.com`). The router proxies on SNI
**without terminating TLS**, so it never sees plaintext or credentials and needs no
per-workload configuration — wildcard DNS plus a wildcard certificate. The WS
server terminates TLS and verifies the JWT audience itself. If connection counts
ever justify sharding, consistent-hashing the uid is a router change, not an
architecture change, and the agent's configuration never moves.

Note this replaces one public endpoint with another: the mesh design already
required an internet-facing NLB for headscale. This is not new exposure.

### 2. The kubelet API lives on the virtual node

**Decision.** Implement the kubelet HTTP surface on the virtual node and serve
logs/exec/attach from it, forwarding over the instance's WebSocket.

```
kubectl exec pod-x
   └─▶ apiserver ──HTTPS──▶ virtual-node kubelet endpoint (Nebula manager)
                               └─▶ map[claim]conn ──▶ agent ──▶ container
```

This **reverses an earlier decision** to expose logs/exec only as a controller-side
`/v1/*` API reached in-cluster. That decision judged `kubectl`-native routing to be
"pure ergonomics", not worth the serving infrastructure. Given the
[requirement](#requirement), the ergonomics *are* the product.

**What it buys.** Every consumer works unmodified — `kubectl`, `client-go`, other
languages' clients, k9s/Lens/ArgoCD, our own controllers — and authorization is
RBAC on `pods/exec` and `pods/log`, namespace-scoped, so a tenant's workload can
only exec into its own Pods. No client library to publish, no permission system to
design. This also answers "how does another in-cluster workload run a command on
the instance": it uses `remotecommand.NewSPDYExecutor` exactly as it would against
a real Pod, and needs to know nothing about Nebula.

**What it costs.** The infrastructure previously skipped:

1. Mount `vkapi.PodHandler` on an HTTPS server. `node/api` is **already imported**
   (`pkg/vnode/handler.go:30`), so this does not need the `nodeutil` wrapper whose
   apiserver dependency does not compile against the pinned k8s 0.33 line
   (`pkg/vnode/node.go:78`). That compile block was never the real blocker.
2. Advertise the endpoint: node `status.addresses` plus
   `daemonEndpoints.kubeletEndpoint.port`.
3. A serving certificate the apiserver trusts.
4. Authn/authz: TokenReview + SubjectAccessReview on `nodes/proxy`. Without this,
   anything that can reach the port can exec into any tenant's container.

Each subresource is a separate handler, and all of them are stubs returning
`errdefs.NotFound` today (`pkg/vnode/handler.go:686`–`709`). They can ship
incrementally — an unimplemented one returns a clean error rather than failing
strangely:

| Subresource | Handler | Phase |
|---|---|---|
| `exec`, `attach` | `RunInContainer`, `AttachToContainer` | 1 |
| `logs` | `GetContainerLogs` | 1 (depends on [Decision 3](#3-the-agent-is-pid-1)) |
| `port-forward` | `PortForward` | 1.5 (notebook) |
| `top` | `GetStatsSummary` | later |

**Latency.** A caller reaches the agent in three hops (caller → apiserver → VK →
agent), and each `exec` is a fresh stream upgrade, because that is the shape of the
Kubernetes exec API — it cannot reuse a connection. The VK→instance leg *is*
connection-reused (a multiplexed stream over the existing WebSocket, sub-millisecond
to open), so the per-call cost is the apiserver upgrade: tens of milliseconds. Fine
for debugging, notebooks, and moderate agent use.

If agent-exec becomes thousands of calls per second, add a second door directly on
the WS server — the caller holds one long-lived connection and multiplexes many
execs over it, one hop instead of three. That is additive and shares the same
instance connection, but it owns its own authorization, which is its real cost.
Build the apiserver path first; it is the compatibility story.

### 3. The agent is PID 1

**Decision.** The agent runs as PID 1 in the workload container and spawns the
workload as its **child**, owning its stdout/stderr pipes. The image entrypoint is
resolved from the **registry** at provision time, not required from the user.

**Why.** `kubectl logs` is impossible otherwise. Today the shim does `exec "$@"`,
making the workload PID 1, so its output goes to the host's Docker log driver —
outside the container and unreachable from within it. (`/tmp/sandd.log` is the
*agent's* log, not the workload's.) Owning the child's pipes is what makes logs
real, and three things come with it:

- **exit codes** — the agent reaps the child, so `Succeeded`/`Failed` become
  representable. That is exactly what Job and training semantics need, and
  `applyState` (`pkg/vnode/status.go`) cannot express it today.
- **`kubectl top`** with no provider API: `/sys/fs/cgroup` plus `nvidia-smi`, both
  readable from inside.
- **restarting the workload** without reprovisioning the instance.

It also *deletes* code. The shim's `while :` supervisor loop exists only because the
agent could not be PID 1; with the agent as PID 1, its death is the container's
death — a visible failure instead of the documented silent-dead-daemon class
(InftyAI/Nebula#20).

**The entrypoint problem, solved generally.** Overriding `ENTRYPOINT` means the
agent must know what to run, which is why the current design rejects commandless
Pods (`errSanddNeedsCommand`, `pkg/provider/aws/translate.go`). That constraint
would block most real inference and training images, which rely on baked-in
entrypoints. The fix is not per-provider: pull the **OCI image config** at provision
time, read `Entrypoint`/`Cmd`, and apply kubelet override semantics — Pod `command`
overrides entrypoint, `args` overrides cmd, the same mapping `buildUserData` already
documents. One cached HTTP call per digest, identical on every provider, and
`errSanddNeedsCommand` disappears for good.

**Why in-container rather than on the host.** Where we own the host (AWS), a
host-side agent would be closer to a real kubelet: `docker logs` and `docker exec`
are literally what a kubelet uses, with zero image requirements, plus container exit
codes and restart-without-reprovision for free. It is genuinely the better
implementation *there*. But it is unavailable on providers that hand us only a
container, so choosing it as the primary path means writing logs/exec twice and
having no answer for a provider that offers neither host access nor a native exec
API.

The in-container PID-1 model is the **general** solution: it needs only a container
and outbound network, which is the universal contract. It gives up multi-container
Pods, init containers, and image-level restart — declared non-goals above. A native
or host-side implementation stays available as an **optimization** behind the
`PodAccess` interface ([below](#provider-seam-changes)), never a requirement. This
is the property that matters for adding providers: an unsupported provider degrades
to the general path instead of being unsupported.

### 4. A Sandbox CRD, above the Pod

**Decision.** Add a `Sandbox` CRD (namespaced) plus a `SandboxClass` (cluster-scoped)
for policy. A Sandbox **creates one Pod**; it does not replace or bypass it.

```
Sandbox
  ├── creates Pod (1, ownerRef'd, restartPolicy: Never, NO command)
  │      └── gate → placement → nodeSelector → virtual node
  │             └── VK CreatePod → NodeClaim → instance
  └── owns lifecycle: stop/start, TTL, idle-cull, endpoints
```

**Why a Pod underneath.** The Pod is the carrier of everything already built:
placement, `pkg/failover`'s blocklist, and — critically — the NodeClaim finalizer
that guarantees a paid instance is never leaked. It is also what ResourceQuota
counts, which is how one tenant is stopped from provisioning fifty H100s. Bypassing
it would mean reimplementing all of that.

**Why not a Deployment.** Replicas are *fungible*; a sandbox has identity — its
disk, its claim name, its live sessions. A rolling update would silently swap a
user's box out from under them, and the Deployment controller's recreate-on-exit
fights the `Stopped` state below.

**Why a CRD at all**, rather than just a labeled Pod. Three things a Pod cannot
express:

1. **Identity that outlives the instance** — the disk and claim name survive a
   reprovision.
2. **`desiredState: Running | Stopped`** — release a $3/hr GPU while keeping the
   volume. A large economic lever for notebooks, and meaningless on a Pod.
3. **`status.endpoints` + `lastActivityTime`** — a stable address and the idle
   signal that drives culling.

**No command, by design.** The agent is PID 1 and there is no user program. For a
notebook, the process that must run (`start-notebook.sh`) is started as a
**post-ready exec through the control channel**, not as the container command. That
is strictly better: restart Jupyter without recreating the instance, a crash does
not kill the box or the session, and a second process (TensorBoard) can be added to
the same sandbox later. If a field is wanted for it, it is `spec.startup` — a hook,
semantically distinct from a container command.

**The classes are policy, not a discriminator.** Five product surfaces come from one
CRD by varying a `SandboxClass`, the way StorageClass and RuntimeClass work:

```yaml
kind: SandboxClass          # cluster-scoped policy
metadata: {name: notebook-a100}
spec:
  nodePoolRef: gpu
  image: jupyter/tensorflow-notebook:latest
  startup: ["start-notebook.sh"]      # post-ready exec, NOT a container command
  ports: [{name: http, port: 8888, expose: Proxied}]
  interfaces: [Exec, PTY, Proxy]
  idleTimeout: 30m
  storage: {size: 100Gi, retainOnStop: true}
```

```yaml
kind: Sandbox               # namespaced instance
metadata: {name: alice-nb-1, namespace: team-ml}
spec:
  classRef: notebook-a100
  desiredState: Running
  accelerator: nvidia-a100            # per-instance override
  ttl: 8h
status:
  phase: Ready
  endpoints:
    http: https://nebula-proxy.nebula-system/s/alice-nb-1/
  lastActivityTime: "2026-08-07T09:12:00Z"
  nodeClaimRef: {name: team-ml-alice-nb-1}
```

So: *agent-exec* = short TTL, no storage, Exec only. *agent* = long-lived, storage,
no idle cull. *notebook* = as above. *shell* = PTY-primary, short idle. Five
surfaces, one CRD, zero mutually-exclusive fields.

**The WS server is the activity oracle.** It sees every exec, session, and proxied
request, so `lastActivityTime` flows agent → server → `Sandbox.status`, and the
Sandbox controller culls on it. Nothing else can know this — the provider only knows
the instance is running.

**Open question — cold start for agent-exec.** A NeoCloud GPU instance takes minutes
to become Ready; a code-interpreter call wants sub-second. Three options: (a) 1:1
Sandbox↔instance and accept the latency; (b) a warm standby pool, still 1:1, paying
for idle GPUs; (c) N Sandboxes multiplexed onto one warm instance. Option (c) needs
a per-instance runtime that can launch containers on demand — a different component
from a single-tenant agent — and breaks `NodeClaim.spec.podRef`'s 1:1 relationship
(`api/v1alpha1/nodeclaim_types.go:18`). Start at (a); do not deepen the 1:1
assumption elsewhere, so (b) or (c) stays reachable.

### 5. Inbound is a provider capability

**Decision.** Inference reachability is resolved per provider in three tiers, all
converging on one consumer-facing abstraction: a **selector-less Service** plus an
EndpointSlice that Nebula populates.

| Tier | Mechanism | Providers |
|---|---|---|
| **A. Native endpoint** | the provider hands us a URL | Modal (tunnels — already implemented, `pkg/provider/modal/client.go:270`) |
| **B. Direct L3** | security group opens the port; clients hit the instance IP | AWS |
| **C. Gateway** | instance dials out; an in-cluster gateway accepts consumers | providers with no inbound path at all |

Prefer A, then B. C is a fallback taken knowingly, per provider, because it puts a
hop in the token path — it must not be the default, which is exactly what a
mesh-everywhere design would have locked in.

**Why the Service must have no selector.** A selector-based Service would have its
EndpointSlices auto-populated with the *virtual node's* notion of pod IP, which is
useless. The real address lives in the endpoint annotation
(`persistEndpoint`, `pkg/vnode/handler.go:545`), so Nebula writes the EndpointSlice
itself with `endpointslice.kubernetes.io/managed-by: nebula`. Then kube-proxy
load-balances to off-cluster IPs and every existing client — `my-llm.default.svc` —
works unmodified.

**Readiness is a correctness prerequisite, not a feature.** `applyState` sets
`Ready=True` as soon as the provider reports *running*
(`pkg/vnode/status.go:74`), but vLLM needs minutes more to load weights. Routing on
that blackholes traffic on every rollout. An endpoint must not become `ready` until a
real probe passes. Modal already enforces the Pod's probe internally
(`SandboxSpec.ReadinessProbe`); for AWS this means probing in the VK poll loop
(`reconcileOnce`) and gating the Ready condition on the result.

**Never through the control channel.** One process holding 10k connections must not
also carry token streams: it would become a throughput bottleneck and a single
failure point for serving. Control plane and data plane stay separate. The one
exception is `kubectl port-forward` over the existing WebSocket — correct for
debugging, a notebook, or an admin API, and explicitly not for model serving.

**Training's inbound is different.** Rank-to-rank connectivity is inbound, but from a
*closed, known* peer set — so it is a provider-native rule scoped to the placement
group (same subnet, allow-from-self security group, EFA/RDMA where available), not
exposure to anything outside. It is a provisioning-time property, which is why it is
coupled to the gang primitive rather than to this tier system.

---

## Provider seam changes

Four changes to `pkg/provider`, all small, all shared by every phase.

**1. One interface for pod access, agent-backed by default.**

```go
type PodAccess interface {
	Logs(ctx context.Context, claim string, opts LogOptions) (io.ReadCloser, error)
	Exec(ctx context.Context, claim string, cmd []string, io AttachIO) error
	Stats(ctx context.Context, claim string) (Stats, error)
}
```

The agent-backed implementation works everywhere. A provider *may* supply a native
one (Modal's SDK exec) as a pure optimization; nothing breaks if it does not. This is
what keeps "add a provider" from meaning "reimplement logs and exec".

**2. One per-provider primitive for getting the agent in.** This is the only place
provider difference is irreducible:

```go
InjectFiles(spec *InstanceSpec, files []File) error
```

AWS already has this in effect — host fetch plus a read-only bind mount
(`sanddHostFetchScript`). Modal uses its own file/image mechanism. Anything else
falls back to a `/bin/sh` bootstrap that curls the binary and execs it. One small
function per provider, and the whole kubelet surface follows.

**3. `Instance.Endpoint string` becomes named, typed addresses.** Modal is the
forcing case, not a hypothetical: it creates one tunnel *per port* and `observe`
collapses them into a single string (`pkg/provider/provider.go:192`).

```go
type Address struct {
	Name      string       // "http", "grpc" — matches the containerPort name
	Host      string
	Port      int32
	Reachable Reachability // Native | Direct | Gateway
}
```

`Reachable` is what tells the EndpointSlice bridge whether to write the address
directly or point at a gateway. Do this **before** two consumers hardcode the
string: sandbox wants a control channel, inference wants `http`.

**4. `Capabilities` gains the access questions** (`pkg/provider/provider.go:151`):

```go
NativeExec     bool         // Modal: true → no agent needed for exec
NativeEndpoint bool         // Modal: true (tunnels)
InboundMode    Reachability // which tier this provider uses
```

Also: SandD injection moves from **per-provider** (`KeyMinter != nil`, which injects
into every workload on that provider) to **per-workload**, carried on
`InstanceSpec`. Required regardless — inference and training Pods must be able to opt
out.

---

## Roadmap

Each phase builds the seam the next one needs. Nothing is built twice.

### Phase 1 — Sandbox, agent, notebook, shell

*Goal: `kubectl exec`/`logs` work against a remote box, driven by a CRD.*

Agent side (gating — these block everything else):

1. Plain-TLS dial-out with a bearer JWT, alongside the existing `--tunnel` mode.
2. Agent as PID 1, spawning the workload as a child and owning its pipes.
3. Registry-based entrypoint resolution, so no `command` is required.

Nebula side:

4. `SanddConfig` → dial-out: drop `ControlServer`, `AuthKey` → `Token`. The
   keybroker becomes a JWT signer — same call site at Provision, same env-injection
   seam in `writeSanddEntrypoint`; it loses its headscale admin coupling rather than
   gaining anything.
5. WS server holding `map[claim]conn`. **In the manager if the agent has a Go
   server implementation; otherwise one shared gateway Deployment** that the VK
   calls over HTTP. Not one per workload — see [Risks](#risks).
6. Kubelet serving stack on the VK: `vkapi.PodHandler`, address advertisement,
   serving cert, TokenReview/SubjectAccessReview.
7. Implement `RunInContainer`, `AttachToContainer`, `GetContainerLogs`.
8. `Sandbox` + `SandboxClass` CRDs and their controller.
9. Delete: `errSanddNeedsCommand`, the shim supervisor loop, the Tailscale fetch,
   the headscale half of `config/sandd`, and the `SANDD_TUNNEL_SERVER` substitution
   dance in `Makefile`/`hack/deploy.sh` (a headscale constraint, not ours).

Pull forward from Phase 2 if convenient: the `Instance.Endpoint` widening (seam
change 3). Cheap now, annoying once two consumers depend on the string.

Notebook additionally needs `PortForward` or a proxy for its HTTP/WS surface —
scope it as 1.5 if Phase 1 is getting long.

### Phase 2 — Inference and services

*Goal: `my-llm.default.svc` resolves to an off-cluster instance.*

1. Named addresses + `Reachability` (if not pulled into Phase 1).
2. `Capabilities` access flags; per-workload agent opt-in.
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
   Phase 1's PID-1 agent, which already reaps the child; `applyState` needs to
   represent it.
5. **Shared storage** for checkpoints — the same missing provider field as notebook
   persistence. Nothing in the AWS adapter wires block devices today.

Logs and exec come free from Phase 1: same agent, same connection.

---

## Risks

**Apiserver → manager reachability.** The apiserver dials the address the virtual
node advertises, so the manager pod's IP must be routable from the control plane.
Fine on VPC-CNI (EKS). **Not** guaranteed on overlay CNIs, and konnectivity or
egress-selector clusters need explicit configuration. This is the one item that
could invalidate Decision 2, and it is cheap to test — **validate it in a real
cluster before building the rest of Phase 1.**

**Cross-repo dependency on the agent.** Items 1–3 of Phase 1 are SandD-side. In
particular, whether the agent has (or will have) a **Go** server implementation
decides whether the WS server lives in the manager or in a separate gateway
Deployment. Confirm before starting item 5.

**Connection state is not reconstructible.** Restarting the connection holder kills
live `exec` sessions and reconnects every agent at once. Real kubelets behave the
same way on restart, so it is a familiar failure mode — but it means backoff with
jitter belongs in the agent (not optional), and log streaming should be resumable by
offset rather than assuming a durable stream.

**The per-workload controller is dropped.** The earlier design gave each workload its
own controller Deployment so consumers could dial it directly with per-tenant
isolation. With the apiserver as the front door, isolation comes from
SubjectAccessReview instead, and a per-workload controller would be 10k Deployments
and an extra hop for no benefit. One shared holder, keyed by claim name.

**Token lifetime versus workload lifetime.** A 24h token on an 8h notebook is fine;
a long-lived agent outliving its token needs refresh. Simplest for Phase 1: issue a
token that outlives the class's max TTL and bound exposure with ownerRef GC. Decide
now — retrofitting refresh into the agent is worse than designing for it.
