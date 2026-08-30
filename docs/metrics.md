# Metrics

What Nebula instruments, and what each series is for.

The instrumented surface is the path a Pod takes from admission to a running external
instance — **placement**, then **provisioning**:

```
Pod created (gated)
        |  nebula_placement_wait_duration_seconds
        |  nebula_placement_deferrals_total       <- why it is still waiting
        |  nebula_placement_candidate_skips_total <- why a candidate was passed over
        v
placed (gate removed)         nebula_placement_decisions_total
        |  nebula_provision_duration_seconds     <- the provider API call alone
        |  nebula_provision_attempts_total
        |  nebula_provision_failures_total
        v
instance accepted
        |  nebula_instance_ready_duration_seconds
        v
Running
```

Those are the parts whose cost and failure modes are otherwise invisible: placement can
silently leave a Pod gated forever, and provisioning runs against a third party, takes
seconds to minutes, bills money, and fails for reasons the Pod status flattens away.
Everything else is already covered elsewhere and deliberately not duplicated here —
reconcile counts, queue depth and API latency by controller-runtime's own collectors, and
Pod-population questions ("how many Pods are gated right now?") by kube-state-metrics.

- [Where they are served](#where-they-are-served)
- [Placement](#placement)
- [Provisioning](#provisioning)
- [Label semantics](#label-semantics)
- [Example queries](#example-queries)
- [Known gaps](#known-gaps)

## Where they are served

Every collector registers into controller-runtime's registry (see `pkg/metrics`; the
`init` in each file is what registers them, so importing the package is the only wiring),
which means they are served on the manager's existing `--metrics-bind-address` endpoint
alongside the standard controller and workqueue metrics. In the default overlay that is
`:8443` with authn/authz, so a scrape needs a bearer token whose subject is bound to the
`nebula-metrics-reader` ClusterRole (`config/rbac/metrics_reader_role.yaml`, which grants
`get` on `/metrics`).

## Placement

Where Pods land, and why they don't.

| Metric | Type | What it answers |
| --- | --- | --- |
| `nebula_placement_decisions_total` | counter | Where Pods actually land. The `capacity_type` breakdown is the cost question: a fleet sliding from Spot to OnDemand is a regression with no error anywhere. |
| `nebula_placement_wait_duration_seconds` | histogram | How long a Pod sat gated, from Pod creation to the gate being removed. |
| `nebula_placement_deferrals_total{pool,reason}` | counter | *Why* a reconcile placed nothing. |
| `nebula_placement_candidate_skips_total{provider,capacity_type,region,reason}` | counter | Why the walk passed over one candidate. The only view into failover actually working. |

The deferral `reason` is a closed set, and each value points at a **different owner** —
which is the whole reason for splitting them:

| `reason` | Means | Clears when |
| --- | --- | --- |
| `no_pool` | The Pod names a NodePool that does not exist, or carries no pool label. | A human fixes the Pod (or the workload generating it). |
| `invalid_request` | The accelerator request is malformed — e.g. `nvidia.com/gpu` with no accelerator-type label. It is *not* treated as CPU-only. | A human fixes the Pod spec. |
| `all_blocked` | A servable candidate exists, but failover is holding every one of them off. | By itself — the Pod is already requeued for the block's expiry. |
| `no_candidate` | No provider in the pool can serve this request at all. | An operator adds a provider, or a provider registers. |
| `stale_claim` | A NodeClaim from a prior same-named Pod has not been reaped yet. | By itself, in seconds. A sustained rate means the NodeClaim backstop is stuck. |

The skip `reason` is likewise closed: `provider_unregistered`,
`capacity_type_unsupported`, `accelerator_unsupported`, `blocked`. Only `blocked` clears
on its own. One reconcile can file several skips — the walk visits every candidate before
giving up.

`nebula_placement_deferrals_total` counts **deferrals, not Pods**. A gated Pod is
reconciled again on every requeue and resync, so one Pod stuck for an hour contributes
many increments. The rate is therefore a measure of placement pressure, not a population:
for "how many Pods are stuck right now" read the SchedulingGated Pod count from
kube-state-metrics, and use this series to explain *why*.

## Provisioning

What the external call cost, and how it failed.

| Metric | Type | What it answers |
| --- | --- | --- |
| `nebula_provision_attempts_total{result}` | counter | Provisioning volume and error rate, per candidate. |
| `nebula_provision_failures_total{reason}` | counter | *Why* provisioning fails. |
| `nebula_provision_duration_seconds{result}` | histogram | Latency of the `Provision` call alone. What lands here is provider-specific: AWS sweeps a region's availability zones inside the call, so a capacity shortage shows up as latency *here*; Modal builds the image inside it, so a build-cache miss does. Bucketed to 600s, the largest `ProvisionTimeout` — so `+Inf` means a call outran its own deadline. |
| `nebula_instance_ready_duration_seconds` | histogram | The whole user-visible wait, from `CreatePod` to the first poll tick reporting `Running` — including provider-side queueing, image pull, GPU attach and up to one poll interval of detection lag. |

`nebula_provision_failures_total` deliberately overlaps
`nebula_provision_attempts_total{result="failure"}` rather than adding a `reason` label
there: `reason` is only meaningful on failure, and carrying it on the attempts counter
would multiply the success series by a label that is constant for them.

The failure `reason` is a coarse, closed set, mapped from the shared sentinels in
`pkg/provider` — *not* from message text, which is what keeps the label bounded:

| `reason` | Means |
| --- | --- |
| `capacity` | `ErrNoCapacity` — the provider has no capacity for this shape. |
| `quota` | `ErrQuota` — our account limit, not the provider's supply. |
| `auth` | `ErrAuth` — credentials or permissions. |
| `unsupported_accelerator` | `ErrUnsupportedAccelerator` — the request cannot be honoured here at all. |
| `timeout` | The `Provision` call hit its own deadline without a capacity cause. |
| `other` | No sentinel, so the category was unavailable — either the adapter returned a raw API error without wrapping one, or the provider never told us what it decided at all. |

Fine-grained detail is deliberately *not* here: it stays where it is already available
(the Pod's `Failed` status message and the `vnode-handler` error log). These labels exist
to answer "are we losing capacity, or are our credentials broken?" at a glance.

## Label semantics

Every series except the `{pool,reason}` and `{provider,capacity_type,region,reason}`
diagnostics carries the same label set:

```
provider  region  capacity_type  accelerator  accelerator_count
```

That is on purpose: a placement and the provisioning attempt it led to carry **identical
label values**, so the two join in PromQL without label surgery — "placed on Spot but
never provisioned" is one query. For the same reason the two duration histograms measure
adjacent legs of one journey: `placement_wait` ends exactly where `instance_ready`
begins, so together they cover `kubectl apply` to `Running`.

Five label values are load-bearing:

- **`none`** is the placeholder for a label the request genuinely did not carry: no region
  (unconstrained), no capacity tier (the provider's default), no accelerator (a CPU-only
  Pod). An explicit token beats an empty string, which in PromQL is indistinguishable from
  a label that was never set and silently matches `{region=""}` selectors nobody meant to
  write.
- **`pool`** on the deferral counter is only ever the name of a NodePool that *exists*, or
  `none`. The pool a Pod asks for is a Pod label — user-controlled and unbounded — so the
  `no_pool` deferral files `none` rather than the unresolved string; a mislabeled workload
  must not be able to mint a time series per typo.
- **`accelerator` and `accelerator_count`** are two labels, not the joined `H100:8` pool
  identity the failover blocklist uses as its key. The two want opposite things from the
  same pair: a blocklist needs one opaque key so an `H100:8` shortage never excludes
  `H100:1`, while a metric needs two dimensions so `sum by (accelerator)` spans every size
  and "all 8-GPU requests, whatever the type" is expressible at all. The pool key stays
  recoverable as `accelerator + ":" + accelerator_count` when correlating a counted
  failure with an excluded candidate. A CPU-only Pod renders both as `none` — not `0`,
  which would land it in the numeric series read as real counts.
- **`region`** is the provider's own token, not necessarily one region. For a provider that
  collapses every declared region into a single candidate (Modal) it is the joined form —
  the same value `NodeClaimSpec.Region` carries.
- **`reason="other"`** covers both an adapter that did not wrap its errors and a provider
  that never told us what it decided (a transport failure, a 503). Neither blocklists the
  candidate on its own, and the two are separated in the `vnode-handler` error log rather
  than in the label. A spike here alongside flat `capacity`/`auth` series is the shape of a
  network or provider outage.

Cardinality is bounded by *configuration*, not by workload: providers x regions x tiers x
accelerator pools, all of which come from NodePools and provider catalogs. Nothing
derived from a Pod name, UID or namespace is ever a label.

## Example queries

```promql
# Provisioning error rate, by candidate.
sum by (provider, region, capacity_type) (rate(nebula_provision_attempts_total{result="failure"}[15m]))
  / sum by (provider, region, capacity_type) (rate(nebula_provision_attempts_total[15m]))

# Cost regression: what fraction of placements are falling back off Spot?
sum(rate(nebula_placement_decisions_total{capacity_type="OnDemand"}[1h]))
  / sum(rate(nebula_placement_decisions_total[1h]))

# ...and whether failover explains it.
sum by (provider, region) (rate(nebula_placement_candidate_skips_total{capacity_type="Spot",reason="blocked"}[1h]))

# Placements that never led to a provisioning attempt (stuck between the two halves).
sum by (provider, region, capacity_type) (increase(nebula_placement_decisions_total[1h]))
  - sum by (provider, region, capacity_type) (increase(nebula_provision_attempts_total[1h]))

# End-to-end p95, apply to Running: the two legs summed.
histogram_quantile(0.95, sum by (le) (rate(nebula_placement_wait_duration_seconds_bucket[1h])))
  + histogram_quantile(0.95, sum by (le) (rate(nebula_instance_ready_duration_seconds_bucket[1h])))

# Adapters that are not wrapping their errors (a to-do, not an incident).
sum by (provider) (rate(nebula_provision_failures_total{reason="other"}[6h]))

# Requests nobody is retrying their way out of: a human has to fix these.
sum by (reason) (rate(nebula_placement_deferrals_total{reason=~"no_pool|invalid_request"}[1h]))
```

## Known gaps

Both are deliberate, and both bias toward looking *better* than reality — worth knowing
before trusting a dashboard.

- **Counters reset on restart.** Every collector is in-process. This is ordinary Prometheus
  semantics — `rate()` and `increase()` detect resets — but no cumulative history survives
  a redeploy; the scrape backend owns durability.
- **`instance_ready_duration` under-samples slow boots.** The start timestamp lives only in
  the virtual node's in-memory tracking map, so a provision still in flight when the
  manager restarts is re-adopted without one and is never observed. A missing sample beats
  a wrong one — measuring from re-adoption would report a wait of minutes as microseconds —
  but the bias runs toward looking fast. Closing it needs the start time persisted durably
  (a Pod annotation or NodeClaim status field), which is a write on the provisioning path.

`placement_wait_duration` has no equivalent gap: its start timestamp is the Pod's own
`creationTimestamp`, so a placement that happens after a manager restart still reports the
true total wait.
