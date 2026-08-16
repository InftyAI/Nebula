# Status

How an external instance's lifecycle becomes Kubernetes status. There are three
layers, and each one deliberately knows less than the one below it:

```
provider API (EC2 states, Modal exit codes, ...)
        |  each adapter's toState
        v
provider.InstanceState        Pending | Running | Terminated | Failed
        |  pkg/vnode/status.go applyState
        v
Pod.status                    phase + reason + Ready condition
        |  internal/controller desiredPhase
        v
NodeClaim.status.phase        Provisioning | Bound | Terminating | Terminated
```

The narrowing is the point. `InstanceState` is a closed four-value set so the
control plane never branches on a provider name, and the NodeClaim reads only the
Pod — never the provider — so there is exactly one place where provider vocabulary
enters the system.

- [Pod and NodeClaim status](#pod-and-nodeclaim-status)
- [Provider mappings](#provider-mappings)
  - [AWS](#aws)
  - [Modal](#modal)
  - [fake](#fake)
- [Logs](#logs)
- [What is not observable](#what-is-not-observable)

---

## Pod and NodeClaim status

Pod status and NodeClaim status come from different sources. The Pod is the
runtime surface users watch. The NodeClaim is the coarse ledger that protects
teardown.

| Pod phase | reason | writer | instance exists? | claim phase |
|---|---|---|---|---|
| `Pending` | `Provisioning` | `CreatePod`, before `Provision` | no | `Provisioning` (grace applies) |
| `Pending` | `Provisioning` | held, after an UNRESERVED `Provision` | yes, unreserved | `Provisioning` (grace applies) |
| `Pending` | `Initializing` | `CreatePod`, after a RESERVED `Provision` | yes | `Bound` |
| `Pending` | `Initializing` | `applyState` ← `InstancePending` | yes | `Bound` |
| `Running` | `Running` | `applyState` ← `InstanceRunning` | yes | `Bound` |
| `Failed` | `ProvisionFailed` | `CreatePod` | no | `Terminated` (via `isTerminal`) |
| `Failed` | `Failed` | `applyState` ← `InstanceFailed` | yes | `Terminated` |
| `Failed` | `Terminated` | `applyState` ← `InstanceTerminated` | gone | `Terminated` |
| `Succeeded` | `Terminated` | `DeletePod` | gone | `Terminated` |

`Running` also sets `Ready=True`. A Pod carrying a `DeletionTimestamp` maps to
`Terminating` from any non-terminal phase. When the served Pod is ABSENT: after
`Bound`/`Terminating`, the claim deletes itself and the terminate finalizer runs;
before `Bound`, it waits `placementGracePeriod` first.

The endpoint annotation is written on whichever path first knows the address, which
differs by provider and is independent of phase. A provider that mints a connect URL
at create time (Modal) publishes it from `CreatePod`, alongside the Secret holding
the matching bearer token — so the endpoint is on the Pod before it is `Running`. A
provider whose address only exists once the instance boots (AWS's public DNS name)
reports it through `List()`, so the poll loop publishes it. Nothing ever clears the
annotation: once written it stays for the Pod's life, which is what lets the two
paths coexist and what makes the address survive a manager restart.

Note the two writers. `CreatePod` writes the rows around the `Provision` call;
every other non-terminal row comes from `applyState`, driven by the poll loop, and
is therefore only reachable for an instance the provider returned from `List()`.

`Provisioning` is written *before* `Provision` is called. That call can run for
minutes (AWS sweeps a region's zones on a capacity error, bounded at 2), and until
it returns this is the only explanation the Pod carries.

Emitting it early is safe; *tracking* it early is not. The poll loop maps a tracked
Pod absent from `List()` to `Terminated`, which is unrecoverable (Pod phases are
terminal-sticky and the claim reclaims on that phase), so a tick landing mid-
provision would tear down the instance the call is about to return. `CreatePod`
therefore emits without storing, and stores only once `Provision` returns.

`Provision` returns `(id, reserved, error)`. `reserved` means the provider committed
capacity, not merely accepted the request: AWS always does (`CreateFleet` with
`FleetTypeInstant` is synchronous), a fresh Modal sandbox never does (the GPU may
still be queued). Only a reserved instance advances to `Initializing`; an unreserved
id holds at `Provisioning`, which is still exactly true — the id is real and must be
reclaimed, but nothing is allocated. Either way the Pod is now tracked: `reserved`
constrains what the status may claim, not what is owed, and says nothing about
readiness — that is `applyState`'s job one tick later.

That hold is short-lived by design. One poll tick later the same queued sandbox
reads `Initializing`, because a provider that queues cannot tell queued from booting
(see below). For that window the claim is `Provisioning` rather than `Bound`, so an
orphan waits the grace period instead of being reclaimed at once — teardown still
resolves the instance from `List()`, so nothing leaks.

Important details:

- `Bound` means an instance EXISTS at the provider, not that the workload is
  ready. It is the teardown guard: a later Pod disappearance is trusted as real
  teardown, not cache lag, and is reclaimed with no grace period.
- A booting instance is `Bound` too — just as real and just as billable as a
  serving one. There is no `Initializing` claim phase; readiness lives on the Pod
  alone. Note `Pending` carries two reasons: the phase alone cannot separate
  "nothing exists yet" from "exists and booting", so the claim keys off
  `status.reason`, which is why the reasons are declared once as `PodReason*` in
  `api/v1alpha1`.
- Only `Provisioning` fails to earn the guard. If the Pod is absent then, the
  controller waits `placementGracePeriod` (15 seconds) before deleting an orphaned
  claim. An unreserved instance reads `Provisioning` even though it exists, so it
  gets the grace window rather than immediate reclaim — safe either way, because
  teardown resolves the instance from `List()`, not from the reason.
- `NodeClaimStatus.InstanceID` is recorded on a best-effort basis. The finalizer
  prefers it when present, but can still recover by matching provider instances
  by claim name through `List()`.
- NodeClaim does not mirror logs, restarts, container state, or fine-grained
  runtime health. Those belong on the Pod.

---

## Provider mappings

Every adapter narrows its own vocabulary to `InstanceState` in a `toState`
function. Two rules hold across all of them:

- **`Running` means reachable, not merely started.** A provider reports
  `InstanceRunning` only once the instance has passed whatever readiness bar it
  can observe, because `InstanceRunning` becomes Pod `Running` + `Ready=True`,
  which is what Kubernetes counts toward a Deployment's ready replicas. Advancing
  early would report a Deployment as serving while none of its boxes can be
  reached.
- **Unknown states map to `Pending`, never to a terminal state.** A status string
  the adapter does not recognize means the poll loop keeps watching. Guessing
  `Terminated` would strand a live, billing instance.

### AWS

Two independent axes: the EC2 instance-state name from `DescribeInstances`, and
the 2/2 reachability checks, which need a separate `DescribeInstanceStatus` call
folded in by `List` (`StatusChecksPassed`).

| `ec2State` | checks | `InstanceState` | Pod |
|---|---|---|---|
| `pending` | — | `Pending` | `Pending` / `Initializing` |
| `running` | not ok | `Pending` | `Pending` / `Initializing` |
| `running` | 2/2 ok | `Running` | `Running` / `Ready=True` |
| `shutting-down`, `terminated` | — | `Terminated` | `Failed` / `Terminated` |
| `stopping`, `stopped` | — | `Terminated` | `Failed` / `Terminated` |
| anything else | — | `Pending` | `Pending` / `Initializing` |

- EC2 flips an instance to `running` a minute or two before its checks clear, so
  `running` alone is not reachable — hence the second call. If
  `DescribeInstanceStatus` fails (commonly a missing `ec2:DescribeInstanceStatus`
  IAM grant, which is a *separate* permission from `DescribeInstances`), `List`
  still returns the instances with `StatusChecksPassed` false, so they hold at
  `Pending`. That failure is logged precisely because it is otherwise invisible:
  every healthy instance stays stuck at `Pending` forever.
- `stopped` is treated as gone, not as a distinct state: a stopped instance is not
  serving the workload, and the ledger's recovery model is delete-and-recreate.
- **There is no queueing.** Provisioning uses a `CreateFleet` *instant* request,
  which is synchronous — the response carries either an instance id or the
  reason it could not launch. A capacity shortfall is an error
  (`ErrNoCapacity`/`ErrSpotCapacity`) that drives AZ/region/tier failover, not a
  pending instance. So an AWS instance that exists is always allocated; `pending`
  vs `running`-without-checks are both "booting", and both are `Bound`. This is why
  `Provision` always returns `reserved` — the Pod can go straight to `Initializing`.
- **No `Failed` case.** Impaired status checks and `StateReason` are not consumed,
  so an instance that failed to boot currently reads as `Terminated` (looks like a
  clean teardown) or holds at `Pending`. See
  [What is not observable](#what-is-not-observable).

### Modal

One sandbox per NodeClaim. Modal exposes far less than EC2, so the adapter has
only two signals and has to record a third fact itself.

| signal | `InstanceState` | Pod |
|---|---|---|
| `Poll` → nil (live), readiness confirmed | `Running` | `Running` / `Ready=True` |
| `Poll` → nil (live), readiness not confirmed | `Pending` | `Pending` / `Initializing` |
| `Poll` → exit `0`, `137`, `124` | `Terminated` | `Failed` / `Terminated` |
| `Poll` → any other exit code | `Failed` | `Failed` / `Failed` |
| absent from `List` | `Terminated` | `Failed` / `Terminated` |

- `Poll` answers exactly one question: **has the process exited?** It returns
  `nil` for a sandbox that is queued, pulling its image, attaching GPUs, booting,
  or up-but-not-ready — all of it. Readiness cannot come from `Poll`.
- Readiness comes from `WaitUntilReady`, which **blocks** and returns early only
  to say "ready" — never to say "not ready". Its timeout is a budget, not a hint,
  and the cost is dominated by per-call setup (task-id lookup plus a fresh TLS
  gRPC dial to the task's own router), measured at ~16s cold versus ~100ms warm.
  It therefore cannot be called on the read path: `observe` runs once per sandbox
  per poll tick, so a truthful budget would serialize ~16s per sandbox, and a
  short budget returns a deadline regardless of the truth. Instead one background
  waiter per sandbox does the blocking call and latches the result; `observe`
  reads the latch without blocking. The latch is set only on a CONFIRMED answer
  (`err == nil`, or `FailedPrecondition` meaning "no readiness probe configured"),
  so an ambiguous error can never promote a sandbox that is still coming up.
- The latch does **not demote**: a probe that passes and later starts failing
  leaves the sandbox `Running`. `Poll` still catches process exit, so death is
  observed; sickness is not.
- Readiness only exists if the sandbox was created with a probe, and Modal's
  control plane does not expose that fact back through the Go SDK — so the adapter
  records it at create time in the `ProbeTagKey` tag. The tag tracks whether Modal
  actually RECEIVED a probe, not whether the Pod declared one: a Pod probe with a
  named port or an unsupported handler cannot be expressed as a Modal probe and is
  dropped, and tagging those would claim a readiness signal that does not exist. A
  sandbox with no probe is reported ready as soon as it is live, which is the
  honest answer — Modal has no readiness concept without one.
- Pod probes map onto Modal's exec and TCP probes only. `httpGet` degrades to a
  TCP probe on its port; `periodSeconds` becomes the probe interval.
- **Logs are readable through `kubectl logs`.** Modal implements the optional
  `provider.LogStreamer` (`SandboxLogs` merges the sandbox's stdout and stderr into
  one stream), so the sandbox console reaches kubectl over the kubelet log route.
  Two Modal-specific limits: the stream always replays from the sandbox's FIRST byte
  (there is no seek, which is why `--tail` is served by buffering — see
  [Logs](#logs)), and v2 sandboxes report stdio as unsupported, which surfaces as an
  error rather than an empty stream.
- **Exit codes are lossy.** The control plane's result carries eight statuses
  (`SUCCESS`, `FAILURE`, `INIT_FAILURE`, `INTERNAL_FAILURE`, `TERMINATED`,
  `TIMEOUT`, `IDLE_TIMEOUT`, unspecified) and the SDK collapses all of them into
  one int before the adapter sees it. The split above is therefore inference, and
  deliberately conservative: only a clean exit plus the two codes Modal
  substitutes for a non-exit outcome (`137` terminated, `124` timeout) count as
  gone; any other nonzero exit is a failure. A workload that genuinely exits `137`
  is indistinguishable from a Modal termination, so this can understate a failure
  but never invent one — and it cannot affect teardown, which reclaims by asking
  the provider what exists.
- **Modal DOES queue**, unlike AWS: `Sandboxes.Create` returns an id immediately
  and the sandbox then waits for capacity, potentially for minutes on a large GPU
  shape. It is `Bound` and billing throughout. So `Provision` returns
  `reserved=false` for a fresh sandbox and the Pod holds at `Provisioning` until the
  first poll tick. An *adopted* sandbox has been observed, so a `running` one is
  known to be reserved. See below for why queued is not reported distinctly.

### fake

The in-memory e2e provider reports `InstanceRunning` as soon as an instance is
created, and so always reserves. It exists to exercise the placement and teardown
paths without a real backend, so it has no boot or readiness phase to model.

---

## Logs and exec

`kubectl logs` (`-f` included) and `kubectl exec` (`-it` included) both work against a
Nebula Pod. It is worth spelling out how, because none of the usual kubelet machinery is
present.

**The transport.** Neither is a control-plane read: the API server proxies both to the
kubelet of the node the Pod is on, dialing the address in the Node's
`status.addresses` and the port in `status.daemonEndpoints`. A virtual node has no
kubelet, so the manager serves those routes itself
(`pkg/vnode/kubelet.go`) and every virtual node advertises the manager's Pod IP and
that port. Consequences worth knowing:

- The endpoint is **leader-scoped and dialed by Pod IP**, not through a Service. The
  tracked Pods live in one process's memory, so a Service balancing across replicas
  would send requests to a replica that answers `NotFound`.
- It serves TLS with a self-signed, in-memory certificate — what the API server
  expects of a kubelet, which does not verify it unless
  `--kubelet-certificate-authority` is set. Client certificates are **not** verified
  by default, because which CA signs the API server's kubelet client cert is not
  portable across distributions. Anything that can reach the port can therefore read the
  logs of, and **run commands in**, any Pod on these virtual nodes, with no RBAC check:
  keep it closed with a NetworkPolicy, or pass `--kubelet-client-ca` to require mTLS.
- No POD_IP (running the manager off-cluster) means no endpoint. Logs and exec degrade
  to unsupported; nothing else is affected.

**The provider seam.** Both are optional: a provider opts in by implementing
`provider.LogStreamer` and `provider.Executor`, and one that does not answers `NotFound`
rather than carrying a stub. Modal implements both; AWS implements neither. Each seam is
deliberately minimal — logs are one stream from the instance's first byte, stdout and
stderr merged; exec only STARTS the command and hands back its streams — so every kubectl
option is honoured once, for all providers, in `pkg/vnode/logs.go` and
`pkg/vnode/exec.go`.

**What logs honour, and the one heuristic.** A provider stream has no EOF while the
instance lives and no marker for "you have now caught up", which the real kubelet gets
for free from a file on disk. So:

| flag | behaviour |
|---|---|
| (none) | ends at the first silent gap (1s) or a 30s ceiling, whichever comes first |
| `--follow` | runs until the instance exits, the client disconnects, or the manager shuts down; a silent gap does NOT end it |
| `--tail=N` | the backlog is buffered into a ring of the last N lines and only those are emitted, then `--follow` continues from there |
| `--limit-bytes` | hard cap on bytes handed to the client, applied last |
| `--timestamps`, `--previous`, `--since`, `--since-time` | accepted and **ignored** |

The idle gap is the heuristic, and it is unavoidable: the alternative for a one-shot
read is hanging until the workload exits, which for a long-running server is forever.
The ceiling covers the opposite case, a workload chatty enough that the stream never
goes idle — it truncates rather than failing, and `--follow` has no ceiling.

`--tail` costs what buffering costs: the full backlog still crosses the provider's API,
because there is no seek. Only the delivery is trimmed, bounded by N lines.

The ignored flags cannot be served from this seam rather than merely being unfinished.
`--timestamps` would have to invent a receive-time stamp, attributing the backlog's
whole history to the moment it was fetched. `--previous` and `--since` need a
per-container restart history and time-indexed storage that no provider exposes. They
are ignored rather than rejected so a habitual `--since=1h` prints the full stream
instead of an error.

**Containers are not addressable.** `-c` is accepted and ignored, for logs and exec
alike: a Nebula Pod maps to exactly one external instance with one console, so there is
no per-container stream to select or shell to enter. Honouring the name would mean
rejecting `kubectl logs pod` (which sends no container) or lying about a second
container's output.

**Exec needs no agent in the image.** The provider's own worker starts the command, so
`kubectl exec -it pod -- bash` works against an unmodified user image — including the
`sleep infinity` placeholder a Sandbox runs. What the exec does need is a **running**
instance: a sandbox still queued for capacity has no container to run in, and the attempt
fails rather than waiting.

Nebula only pumps bytes: stdin is forwarded and closed at EOF (so `exec -i -- cat < f`
ends), stdout and stderr are copied back, and a non-zero exit reaches kubectl as
`command terminated with exit code N` rather than a server error. Two gaps, both from the
provider side:

| behaviour | why |
|---|---|
| terminal **resize** is ignored | no provider exposes a window-size call, so `-it` opens at the remote default and stays there |
| under `-t`, **stderr is merged into stdout** | a PTY is one stream; kubectl forbids asking for both anyway |

## What is not observable

Documented so the coarseness is not mistaken for a bug.

**Modal: queued vs. booting.** A sandbox waiting for capacity and one actively
starting up are both reported `Pending` / `Initializing`: the Pod reads
`Provisioning` until the first poll tick (≤15s), then `Initializing` for the rest of
both, so that one reason covers queueing and booting alike. Every public signal was
measured and none carries the boundary:

| signal | distinguishes queued from booting? |
|---|---|
| `Poll` | No — both are the unspecified status → `nil` |
| `WaitUntilReady` | No — one bit, and only ever the positive one |
| `SandboxGetTaskId` | No — a task id is assigned ~400ms after create, long before boot |

Modal's control plane does know (`SandboxInfo` carries the task state and a
`ReadyAt` stamp) but the Go SDK builds its sandbox objects from ids alone and
keeps the control-plane client private. If a future SDK exposes `SandboxInfo`,
both this gap and `ProbeTagKey` and the readiness latch all become unnecessary.

The mislabel is cosmetic today: the only consumer of the reason is the claim's
teardown guard, and both `Provisioning` and `Initializing` reclaim correctly —
`Provisioning` after the grace window, `Initializing` immediately — because
teardown resolves the instance from `List()`, not from the reason.

**AWS: failed vs. gone.** `toState` has no `InstanceFailed` case, so an instance
that failed to boot is reported as `Terminated` or holds at `Pending`. Unlike the
Modal exit-code inference, the signal here is available and authoritative:
`DescribeInstanceStatus` reports `impaired` for both status checks, and the
instance carries a `StateReason` that separates "we stopped it" from "it died".

**Neither provider reports preemption.** `InstanceState` has no `Preempted`
value, and an instance's disappearance says only that it is gone, not why — so
`Terminated` is the neutral, accurate answer rather than a claim about a provider
reclaim. This is also why `NodeClaimPhase` has no `Preempted`.

**Readiness has no intermediate rung.** `applyState` welds phase, the `Ready`
condition, and container readiness into one atomic write, so in Nebula
`PodRunning` implies `Ready=True` implies all containers ready. The readiness bar
lives entirely in each adapter's `toState`; a Pod is never `Running` but
not-ready.
