# Log shipping

Copies an externally-run instance's stdout and stderr to CloudWatch Logs while it runs, so the output
survives the provider's retention window.

Modal is the one provider implemented, and the sections below about it describe that implementation,
not the component: reading a backend is a port (`internal/provider`), and which backend an instance
needs comes off its Pod. See [How it works](#how-it-works).

**Status.** The reader, the pipeline, the sink, the supervisor, the Pod watch and the wiring between
them are implemented and tested, so one process finds the fleet itself and ships all of it. What is
missing is durability and scale-out: no cursor checkpoint, so a restart replays; no drain finalizer,
so a deletion can cut the tail; no lease, so there can only be one replica. See
[What is left](#what-is-left).

**It does not talk to AWS, and it writes no files.** A record goes to logship's own stdout and the
cluster's existing log agent collects it exactly as it collects every other pod's. No AWS
credentials, no IAM grant, no hostPath — see [The handoff](#the-handoff) for what that costs.

- [Why](#why)
- [What Modal gives us](#what-modal-gives-us)
- [How it works](#how-it-works)
- [The handoff](#the-handoff)
- [Event format](#event-format)
- [Duplicates](#duplicates)
- [Scale](#scale)
- [Failure modes](#failure-modes)
- [What is left](#what-is-left)
- [Permissions](#permissions)
- [Open decisions](#open-decisions)
- [Known gaps](#known-gaps)

## Why

Modal deletes logs on a clock: **1 day on Starter, 30 days on Team**, custom on Enterprise. Once
the window passes, a training run's output is gone and nothing in this repo can recover it.

So this is durability, not convenience. `kubectl logs` still reads live from Modal and is
unaffected; what changes is whether last week's run can still be explained.

## What Modal gives us

`SandboxGetLogs` streams batches, each carrying a cursor — `entry_id`, formatted `<millis>-<seq>`.
Passing one back as `LastEntryId` returns strictly what followed it, so resuming is exact.
Verified against a live sandbox:

| cursor | returned |
| --- | --- |
| `"0-0"` | the whole history |
| first line's `entry_id` | only the lines after it |
| last line's `entry_id` | nothing but `eof` |
| unrecognized (`"1-0"`) | the whole history, **no error** |

Four things about that stream shape the code:

- **The SDK throws the cursor away.** `sb.Stdout`/`sb.Stderr` bottom out in `outputStreamSb`,
  which tracks `entry_id` for its own 55-second re-open loop and then writes only `item.GetData()`
  into a pipe. The entry ID, per-item timestamp and file descriptor are gone before a caller sees
  a byte. Hence the direct RPC: not for the call, but to keep what the call already returns.
- **stdout and stderr have independent ID spaces.** Two lines in the same millisecond got
  `1788477379229-0` and `1788477379282-0`. Everything downstream is per-descriptor.
- **An item is a chunk, not a line.** `data` can hold several newlines or half of one, so line
  assembly is ours. Items also arrive with a `task_state` or `task_progress` and no data; those
  are not output, and `PutLogEvents` rejects an empty message, so the reader drops them while
  still advancing the cursor past them.
- **Prefer `timestamp_ns` over `timestamp`.** The float64 seconds field cannot represent every
  millisecond exactly, so a millisecond-resolution sink reorders same-millisecond lines without
  it.

Two stream-level signals are easy to confuse: `io.EOF` means the 55-second window closed and
should be re-opened, while `batch.GetEof()` means the sandbox finished. The `eof` batch's
`entry_id` is empty, so it must not reach the cursor — an empty cursor replays everything.

## How it works

Six packages, one direction of dependency. `internal/ship` declares the ports and knows nothing
about a provider or about where a line lands; `cmd` is where they meet.

| package | role |
| --- | --- |
| `internal/provider` | port: which streams an instance has, a `Source` for one, capacity ahead of them, and the set of registered backends |
| `internal/modal` | one backend: cursor-aware `Follow`, the re-open loop, a `Source` per descriptor, a connection `Pool` |
| `internal/ship` | mechanism: `Assembler` (chunks → lines), `Record` (the format), `Batcher`, `Pipeline` |
| `internal/emit` | sink: one line per record on stdout, shared by every stream, plus the `Limits` that keep it collectable |
| `internal/supervise` | lifetimes: a changing set of instances → a running set of pipelines |
| `cmd` | wiring: an instance → its provider, a Pod's labels → a `Record`, one sink for the process. No flags — there is nothing to configure |

**The provider is the Pod's, not a flag's.** `nebula.inftyai.com/provider` on the Pod's nodeSelector
is what Nebula placed the instance with, and it travels through `supervise.Instance` to the backend
that can read that instance's id — the same field holds a Modal sandbox id or an EC2 instance id, so
handing one to the wrong reader fails every attempt and spends the whole restart budget doing it. A
configured provider would instead have to be kept in agreement with the cluster's node pools, and
being wrong about it looks exactly like an idle cluster. A backend opens when the first Pod on it
arrives, so its credentials are only required if something is actually running there.

**A pipeline is one stream.** `Pipeline.Run` reads from the source inline and ships from a second
goroutine. Two goroutines rather than one because the source's callback is synchronous — a stalled
callback stalls the cursor — and the interval flush needs a `select`. It flushes every 5 seconds
or when the batch reaches 64 KiB, and calls `Shipped(cursor)` after each successful put. Retrying is
not configured: a write to stdout has no rate to back off from, and the hop that does is the
agent's.

**One sink, not one per stream.** There is a single stdout, so `emit.Sink` is shared by all 1,000
pipelines, and that shapes its contract: `Put` must be concurrency-safe, must write
a batch in one `Write` — two interleaved batches are indistinguishable afterwards — and must never be
closed, since one stream ending says nothing about the descriptor the rest are still using. `Put`
also ignores its context on purpose: cancellation is how shutdown reaches the pipelines, and the last
batch of a stopping process is exactly the one that still has to be printed.

**Backpressure drops, it does not block.** The queue is bounded at 64 KiB and drops the newest
lines on overflow, counting them. Blocking the reader would stall the cursor, and a stalled cursor
risks Modal aging the logs out underneath us — trading a visible loss for a permanent one.

**A supervisor owns the goroutines.** `Ensure` starts one pipeline per stream the instance's
provider declares, and is idempotent on the instance ID, so a watch re-delivering the same Pod does not start a second
replay. A stream that ends is never restarted; one that fails restarts with capped backoff up to
five times and is then abandoned with a log line, because every restart replays from the last
durable cursor.

**Its own Go module**, at `components/logship`, so the manager cannot depend on it even by
accident. Own `Makefile`, `Dockerfile` and CI job; the dependency runs one way, via a `replace` on the
repo root for `api/v1alpha1`.

**Its own gRPC connection to Modal.** `proto/modal_proto` is public, so the request types are ours
to call, and a breaking change there can only reach us when we bump the SDK. The SDK's own
connection cannot be reused (`Client.cpClient` is unexported), and is cheap to replace here: a
`SandboxGetLogs` stream authenticates with five static metadata headers and needs none of the
rotating-JWT machinery the unary interceptors provide. Three consequences:

- Credentials are env-only (`MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`, `MODAL_SERVER_URL`), because
  the SDK's profile loader is unexported. A local run against `~/.modal.toml` diverges.
- The SDK is **pinned twice**, so `kubectl logs` and the shipper can compile against different
  proto revisions. Harmless with one server, but not obvious.
- The five headers include a client type and version a server-side minimum-version check would
  read, so they are reproduced verbatim rather than rebranded. A test asserts the version const
  matches the module pin, which is what a dependency bump forgets.

`SandboxLogs` stays on the SDK rather than being rebuilt on this reader. They are different shapes
— a byte stream for a terminal against an entry stream for a sink — and collapsing them would put
`kubectl logs` behind the raw gRPC surface for no gain.

**Its own Deployment**, one active replica, the lease still to come. Provisioning is critical and log
shipping is best-effort, so the manager's memory must not become a function of how much a tenant
prints, and shipping a log fix must not bounce the control plane. Two replicas reading one sandbox
would duplicate everything, and no cursor helps when both are live.

**Draining will be enforced by the shipper's own finalizer**, `logship.nebula.inftyai.com/drain`, added
to the NodeClaim and removed once the sandbox has shipped to `eof`. Finalizers compose, so this
needs no protocol with the manager: each drops its own, and the object survives until both are
gone. Without it every run loses its last lines, which is the part people read. If the shipper is
dead the manager still tears the instance down, so **nothing keeps billing** and only the API
object lingers — but it still needs a bounded escape so a dead shipper does not accumulate objects
forever.

## The handoff

The sink prints each record as one line on logship's own stdout. Nothing else happens here. kubelet
writes that line to `/var/log/containers/logship-*.log`, the cluster's Fluent Bit DaemonSet already
tails that glob, and the record reaches
`/aws/containerinsights/<cluster>/application` because logship is a pod on the node — not because
anything was configured for it.

Two earlier designs were dropped, and both reasons are worth keeping. Direct `PutLogEvents`: the
agent already runs on every node, already holds the credentials and already does batching, retry and
throttle backoff, so handing it the last hop deleted an AWS SDK dependency tree, the throttle-retry
path and the entire IAM grant. A spool under `/var/log/logship/`: **it needed a config change in a
shared, `eks`-managed DaemonSet.** Fluent Bit's `tail` inputs take no recursive `**`, so a spool is
addressed by no default input and needs an `[INPUT]` added to `fluent-bit-config` — a ConfigMap
server-side-applied by the add-on, so `kubectl edit` is not durable and one syntax error stops
collection on every node. Stdout needs neither.

**The cost is the envelope**, and it is the one thing the handoff is not free of. Our record travels
*through* the agent's `[FILTER] kubernetes` rather than past it, and that filter does two things to
it:

- It stamps the **emitting** pod's metadata — logship's, not the sandbox's. So the outer
  `kubernetes` block is useless for identity, and always was going to be.
- `Merge_Log On` with `Merge_Log_Key log_processed` parses our line as JSON and places the result
  under `log_processed` as real nested JSON, not a string.

So the record arrives intact but one level down: what the consumer used to read at
`$.kubernetes.labels.app` is now at `$.log_processed.kubernetes.labels.app`, while the node's own
logs stay at the top level. Same field names, two depths, and the consumer has to ask for both.

Four things about that were checked against the live group rather than reasoned about:

- `Merge_Log` **is** on, and so is `Keep_Log` — a record carries both `log` (the raw string) and
  `log_processed` (the parsed object).
- **Filter-pattern wildcards work at depth.** `{ $.log_processed.message = "*build*" }` matches, which
  is what keeps the pod-name clause expressible; a three-level path with no matches returns zero
  events rather than an error.
- Insights reads the same fields as dotted names with no `$`, so `coalesce(log_processed.…, …)`
  covers both roots in one query. Run against the real group, it returns the ordered sandbox pod names
  the consumer's discovery query needs.
- **Today's sandbox records already have a `log_processed`** — their own envelope, parsed, with no
  `kubernetes` block, because the pods print structured JSON. So a consumer that descended into
  `log_processed` unconditionally would break every sandbox log that works now. It has to descend only
  when the nested record carries an identity of its own.

**The handoff is still unacknowledged.** Nothing tells us the agent read a byte, so
`Pipeline.Shipped(cursor)` means "written to stdout", not "in CloudWatch", and the drain finalizer
releases on that weaker signal. What holds unread lines is kubelet's rotation of the container log
(typically 10Mi × 5), which is not ours to size.

## Event format

Not ours to choose. The consumer already reads sandbox logs from this group
(`internal/experimentservice/cwlogclient.go`), so the schema is the one it parses: Fluent Bit's
Kubernetes-filter output, which the observability add-on already writes for every node in the
cluster.

```json
{"time":"<RFC3339Nano>","log":"<json>","kubernetes":{"pod_name":"...","labels":{...}}}
```

`log` is itself JSON, holding `level`, `category`, `message` and our `id`. **That nesting is load
bearing.** `buildLine` defaults a `log` it cannot parse to category `private`, and `private` is
hidden from every caller without the developer-view role — so a line shipped as bare text lands
durably and is *invisible* in the UI. A wrong-but-valid envelope fails the same way, silently.

**A line that is already an envelope is adopted, not nested.** `buildLine` unwraps `log` exactly
once, so wrapping a workload's own `{level, category, message}` in ours made ours the only one read:
every sandbox line arrived as `INFO`/`user` with the real envelope stranded inside `message` as text —
missing from a `category=system` query, and rendered in the UI as raw JSON. So `id` is spliced into
the workload's object instead, with `category` added only when it has none, and `level` never, since
the consumer defaults that itself. The bar for adopting is the consumer's own decode rather than
validity: an object it cannot decode into three strings, or one carrying no `message`, is wrapped as
before — adopting it would land it in `private` and hide a line the wrap shows.

That is what logship writes. What arrives in CloudWatch is that object nested under `log_processed`
inside one of the agent's own, per [The handoff](#the-handoff) — so the same JSON, read one level
deeper. The double encoding survives: `$.log_processed.log` is still a *string* of JSON, because
`Merge_Log` parses only the one level it was handed.

Identity is in every event, never in a stream name, because the consumer filters on the labels and the pod
name — and because a deleted Pod leaves nothing to join a stream name against anyway. The labels cost
~250 bytes a line and are captured at ship time or not at all.

Every one of the Pod's labels is copied, not a curated few: that is what Fluent Bit's filter emits for
the node logs already in this group, and it means nothing here has to know a label key. The tenant
triple (the consumer's org, team and experiment IDs) is read only by the consumer, off the
record, so nothing here is configured with a label key at all.

**All of logship's output shares one CloudWatch stream**, named for logship's own pod. Nothing of
the consumer regresses on that: it already reads with a whole-group `FilterLogEvents` plus an Insights
query for discovery and never used a stream-name prefix. One consequence is worth naming:
`PutLogEvents` allows 5 requests/sec **per stream** and that quota is not raisable, so the whole
fleet's output funnels through one stream's share of it. The agent batches, so this is a throughput
ceiling to watch rather than a known problem.

Three rules of the format rather than of the API. A split falls on a rune boundary, since half a rune
reaches CloudWatch as a replacement character and corrupts the durable copy; the split is of the
*data*, never of the formatted message, which would cut an envelope in half, and each piece repeats the
`id` so concatenating same-`id` messages reassembles the line. And **a record contains no raw
newline** — one line is one record, so an unescaped `\n` would become two half-records that parse as
neither. `Assembler` produces a `Line` by splitting on newlines in the first place, and
`Record.Formatter` escapes any that a line's data still holds; the two together are what
`emit.Sink.Put` relies on.

Labels are encoded in sorted order so a stream is byte-identical run to run.

## Duplicates

At-least-once, bounded. There is no exactly-once path into CloudWatch: `PutLogEvents` has no
idempotency key and no content de-duplication.

| situation | duplicates |
| --- | --- |
| running normally | none — the cursor advances in memory |
| clean shutdown | none — the drain ships to `eof` |
| crash, or any restart | full replay of whatever Modal still holds for each live sandbox |
| unrecognized cursor | the same replay, silently — the server returns no error |
| the **agent** restarting without its tail `DB` | re-reads whatever kubelet still holds of our container log |

The order is read → put → record. Crash between the put and the record and a line ships twice;
reverse it and the line is lost instead.

Today those duplicates are user-visible: the consumer de-duplicates on CloudWatch's `EventId`, so a
replay of the same text arrives as new IDs and shows up as duplicated lines. The `id` in each
record is what a reader *could* collapse on. Until something does, the cursor checkpoint is what
bounds a restart's blast radius — and the cursor currently lives only in memory, so a restart
replays from `"0-0"`. `Pipeline.Shipped` is the seam a checkpoint plugs into; nothing calls it
yet.

## Scale

The target is **500 concurrent instances**, which is **1,000 streams**. Five numbers follow from
it. It is a design target and nothing more: no measurement supports it, and it is deliberately not a
configured limit anywhere — a provider makes room as instances arrive (`modal.Pool` widens), so
exceeding 500 costs connections rather than dropped instances. What has not been established is where the fleet-wide path
actually saturates, and the pool is unlikely to be it; the single mutex-guarded descriptor below and
the one container log the agent tails are the better suspects.

- **One goroutine per stream, no worker pool.** A pool smaller than the stream count does not
  slow the fleet, it starves part of it — and an unread stream is one whose logs age out of Modal.
  2,000 goroutines is ~16 MiB of stacks. Bytes are worth bounding here; goroutines are not.
- **A pool of gRPC connections, not one.** HTTP/2 caps concurrent streams per connection and
  gRPC-go *queues* RPCs past the cap rather than failing them, so 1,000 streams on one
  `ClientConn` would stall most of them with no error anywhere. 100 streams per connection.
- **The batch threshold is 64 KiB, not the API's 1 MiB.** Per-stream bounds multiply: 1 MiB would
  be 1 GiB of pending events across the fleet. Same reason the assembler's `maxFragment` is 64 KiB
  — two such bounds per stream puts the fleet near 128 MiB, which is a Deployment that can be
  sized.
- **200 write syscalls per second** at a 5-second interval, one per stream per flush, all of them on
  one mutex-guarded descriptor. The interval is a latency-versus-syscalls choice, not a fleet-size
  one; the mutex is held only for the `Write`, with the batch formatted outside it.
- **No files and no disk bound at all.** kubelet rotates our
  container log and the node was already sized for that; a chatty fleet costs pipe backpressure,
  which the queue's 64 KiB bound turns into counted drops rather than a stalled reader.

The other cost is the envelope: ~250 bytes per line plus CloudWatch's own 26, so ingest is
dominated by metadata for short lines — a 40-byte print costs ~320 bytes. Worth measuring against
real verbosity before quoting a monthly number.

**The per-event cap is now 16 KiB, not CloudWatch's 256.** The binding constraint moved: the
container runtime reads a container's stdout in 16 KiB pieces and writes each as its own partial-
flagged line, leaving the agent's `cri` multiline parser to glue them back. Staying inside one chunk
means that rejoin never runs — and a rejoin that failed would deliver two halves of a JSON object,
which the consumer drops without reporting. Splitting here is cheap; getting that wrong is silent. The
**26 bytes of per-event overhead** are still reserved, both because CloudWatch still charges them and
because they double as headroom under the chunk for the runtime's own line prefix.

With no request to size, `MaxBytes` is only a memory-and-syscall bound. The batcher clamps a backwards
timestamp rather than sorting, which keeps the output in the order the lines were printed.

## Failure modes

| failure | consequence |
| --- | --- |
| shipper down longer than Modal retention | logs permanently lost; the reason this exists |
| unrecognized cursor | silent full replay — the server returns no error |
| an empty `eof` entry ID reaching the cursor | replays the whole sandbox — guarded in the reader |
| sink saturated | dropped events plus a counter, never a stalled reader |
| finalizer removed before drain | the tail of that run is lost |
| a stream failing five times | abandoned and logged; that instance's remaining logs are not copied |
| a diagnostic printed to stdout instead of stderr | it ships as a record; unparseable, so it lands nowhere the consumer looks |
| `Merge_Log` turned off in the agent's config | our record becomes a *string* under `log`; every consumer clause misses |
| the consumer's filter losing the `log_processed` depth | **nothing matches, and nothing errors** |
| a record over 16 KiB reaching the runtime unsplit | split into partial chunks; correct only if the `cri` multiline parser rejoins them |
| node dies with lines kubelet has not been read out of | those lines are lost; the finalizer already released on the write |
| stdout pipe full | the queue drops newest lines and counts them; the reader keeps draining the provider |
| a Pod on a provider this build has no backend for | logged and skipped, once per event; nothing of that instance ships |
| a Pod re-provisioned onto a new instance | the replaced instance is forgotten on the update that rewrote the annotation, losing its unshipped tail; leaving it running would ship two instances under one pod name |

## What is left

1. **`internal/drain`** — add and remove the finalizer, with the bounded escape and a startup
   sweep for stale ones. The first thing here to need `api/v1alpha1`.
2. **Leader election and metrics** — the Deployment and its RBAC exist; the lease does not. At
   `replicas: 1` with `Recreate` a single reader is still enforced by the shape, which now means the
   one replica is the whole fleet's collection. Metrics over `supervise.Stats` are still nothing but
   a line on stderr at shutdown. No volumes and no agent-side prerequisite: being a pod is the whole
   integration.
3. **The end-to-end check.** Nothing in the repo proves a record survives the agent's filter chain,
   and every way it can fail is silent. It was confirmed by hand against a live cluster, which is not
   a check anything re-runs. Whatever replaces it has to assert the pre-fix query returns *nothing*
   as well: a pattern selecting the wrong path is otherwise indistinguishable from an empty window.
4. **The cursor checkpoint** — the one item here that costs money rather than confidence. Both halves
   of the seam exist and neither is wired: `Pipeline.Shipped` is called with each accepted batch's
   last cursor and nothing listens; `fleet.cursor` is a `func(inst, stream) string` nothing assigns,
   so every stream resumes from `"0-0"`. Persisting per *(instance, stream)* — never per instance,
   since the two streams' IDs are independent and one handed to the other replays it silently — takes
   a restart from re-shipping whatever the provider still holds down to what was printed since the
   last flush. Two things must hold: the order stays read → put → record (reversed, a crash loses
   lines instead of repeating them), and the final flush on SIGTERM cannot use the signal's own
   context, which is already cancelled by the time `shutdown` runs — it would fail instantly while
   the clean path looked fine.

## Permissions

**Kubernetes, and nothing else.** Its own ServiceAccount and ClusterRole, narrower than the manager's:
`get`/`list`/`watch` on Pods, read-only, no status writes, no CRDs — cluster-scoped only because
instance Pods live one namespace per org. The drain finalizer will add NodeClaims and
`patch` on `nodeclaims/finalizers`, leader election `coordination.k8s.io` leases, and the checkpoint
whatever it stores cursors in; none of the three exists yet, so none is granted.

**AWS and the node: none, structurally.** Writing to your own stdout is the least privilege a
container has: no credentials to mount, no hostPath a path bug could point at another tenant's
collector, and no CloudWatch client at all — so a compromised shipper cannot read back what it has
written. That is worth keeping in mind before adding a read of the destination, which is the one thing
that would undo it (see [Open decisions](#open-decisions)).

## Open decisions

Two things are settled and not by us: the log group is
`/aws/containerinsights/<cluster>/application`, the one the consumer is already configured with,
because a second group would be a second query no consumer makes; and retention is Terraform's, at
365 days.

- **Where the resume point comes from.** Deferred, with the options costed. A local checkpoint is the
  cheap one: a single ConfigMap in logship's own namespace holding the whole fleet's cursors is one
  write per flush regardless of fleet size, needs `get`/`update` on one named object, and works in
  kind. Per-Pod or per-NodeClaim annotations are the same idea at 500× the writes, each bumping a
  `resourceVersion` that Nebula's own controllers watch, and the Pod variant also turns a read-only
  watcher into something that patches tenant objects.

  Reading it back out of CloudWatch instead is more attractive than it sounds, because **the cursor is
  already in every durable record**: `Record.Formatter` ships `Line.Cursor` as the `id` inside `log`,
  so one Insights query (`stats latest(...) by pod`, descending — `FilterLogEvents` only pages
  forward) reconstructs the whole fleet's resume table with nothing stored anywhere. It is also the
  only option that notices the loss above: a local checkpoint records what we *printed*, so lines
  kubelet never handed over are gone silently, while the destination's own last line re-ships them.

  What it costs is why it is not the default. The record carries no stream marker — `fleet.build` sets
  no `Level` — so `latest` by pod alone cannot tell stdout from stderr, and guessing wrong is the
  silent full replay. It reintroduces an AWS client, IRSA and the group name into a component whose
  whole permission story is *none* (see [Permissions](#permissions)), which also means every start in
  a cluster without that group replays. The query has to regex into `log_processed.log`, still a
  *string* of JSON, whose escaping is the consumer's to change. And it needs a time window: too narrow
  after a long outage returns no rows, which reads as no prior state. If it is ever built, the shape
  that keeps the properties without the coupling is a verification step outside the Deployment —
  "did the agent deliver everything we printed?", which nothing can answer today.
- **The 16 KiB per-event cap.** Chosen to stay inside one CRI chunk, which is a mechanism rather
  than a measurement: the agent's `cri` multiline parser probably does rejoin a longer record
  correctly, and if a probe shows it does, this can go back up toward CloudWatch's 256 KiB and split
  fewer long lines. Erring small is the direction where being wrong is visible.
- **A slash-free identity field in the record.** the consumer filters the experiment out of the pod name
  with a wildcard because a CloudWatch filter pattern cannot select a key containing `/` — confirmed
  against the live group, so the experiment ID label is only usable client-side. A top-level
  `experiment_id` would make that narrowing server-side and decouple the query from pod naming. Still
  a two-repo change, and now a deeper one: it would sit at `$.log_processed.experiment_id`.
- **Whether to collapse progress-bar frames by default.** Implemented either way; the assembler
  takes it as a flag. Two arguments point the same way: a `tqdm` bar emits a carriage-returned
  frame per update and ingest is billed per GB, and since a bar emits no newline until it
  finishes, an un-collapsed one is a pending line that grows for the length of the run. That
  second half is what forced `maxFragment`, now load-bearing for any source that never terminates
  a line. What collapsing costs is the timing of a run's progress.

## Known gaps

**Modal retention is not discoverable at runtime.** `TaskLogsBatch.ttl_days` exists in the proto
and came back `0` on every batch — unset for sandbox logs, and read by no client, Go or Python.
Retention has to be configuration we are told. Which plan the workspace is on therefore matters: 1
day makes an outage lossy in hours.

**Backfill is capped at 14 days.** `PutLogEvents` rejects events older than that, so on a 30-day
Team plan the older half of Modal's retention cannot be copied after the fact by this path. Live
shipping is unaffected. The rejection happens inside the agent, whose counters are not ours to read,
so a backfill that silently drops its older half has nowhere here to show up.
