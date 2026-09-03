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
Running                       nebula_cost_usd_total      <- dollars, while it bills
```

Those are the parts whose cost and failure modes are otherwise invisible: placement can
silently leave a Pod gated forever, provisioning runs against a third party, takes
seconds to minutes, bills money, and fails for reasons the Pod status flattens away, and
the instance it produces keeps charging whether or not anything is using it.
Everything else is already covered elsewhere and deliberately not duplicated here —
reconcile counts, queue depth and API latency by controller-runtime's own collectors, and
Pod-population questions ("how many Pods are gated right now?") by kube-state-metrics.

- [Where they are served](#where-they-are-served)
- [Placement](#placement)
- [Provisioning](#provisioning)
- [Cost](#cost)
- [Attribution](#attribution)
- [Label semantics](#label-semantics)
- [Example queries](#example-queries)
- [Known gaps](#known-gaps)

## Where they are served

Every collector registers into controller-runtime's registry (see `pkg/metrics`; the
`init` in each file is what registers them, so importing the package is the only wiring —
except the cost counter, whose labels are not known until `--cost-labels` is parsed),
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

## Cost

What the fleet has actually spent.

| Metric | Type | What it answers |
| --- | --- | --- |
| `nebula_cost_usd_total{phase,...}` | counter | Dollars the fleet has run up, added one accrual window at a time. Also carries whatever `--cost-labels` names (see [Attribution](#attribution)). |

```promql
# Dollars spent yesterday, by provider.
sum by (provider) (increase(nebula_cost_usd_total[1d]))

# Recent spend, fleet-wide. Keep the range well above BOTH the accrual interval (1m) and the
# scrape interval: increase() needs two samples inside the range, so [1m] against a 60s scrape
# usually returns nothing at all.
sum(increase(nebula_cost_usd_total[5m]))

# Burn rate in USD/hour.
sum(rate(nebula_cost_usd_total[30m])) * 3600
```

**Per shape, not per instance.** One series per candidate — the labels in [Label
semantics](#label-semantics), plus `phase` — with no claim identity on it. That is what makes the
metric *billable* rather than merely observable: `increase(...[w])` is a pure function of the
window `w`, so a billing service replaying an old window re-derives the same dollars and can
upsert them idempotently. A per-claim counter would give up exactly that — differencing a
cumulative per-claim series needs a sample at each window boundary, and an instance that lived
and died between two boundaries has neither, so its spend would be unbillable. Per-claim spend
lives on the claim instead (below).

Sharing series across claims narrows that problem rather than removing it, because `increase()`
still needs a sample from *before* the charge and a series' first sample has none. So the moment a
claim becomes chargeable, Nebula publishes its label set at `0` under all three billing phases —
a baseline for the first window each will ever book. What is left is [one scrape
interval](#known-gaps): an instance born and gone inside a single scrape is invisible to
`increase()` however the series are shaped.

**Instance-level** cost — infrastructure spend, blind to the workload on top, charging each
claim's whole rate.

**Every number here is an estimate.** Both the counter and the field are Nebula's own arithmetic
over a hand-maintained list price — cost *incurred*, which is what "accrued" means, not cost any
provider has confirmed. Nothing below has been invoiced.

**One arithmetic, two views.** A leader-elected loop closes each claim's window every minute:
it charges `priceUSDPerHour × (now − status.lastAccruedAt)`, adds it to
`NodeClaim.status.estimatedCostUSD` (the `EST_COST` column) and re-anchors, then books the *same*
window on the counter. So the counter is the fleet's stream of charges and the field is the
per-claim rollup of them, and "what did this one instance cost" is a `kubectl get` rather than a
query:

```
NAME               PROVIDER   ACCELERATOR   PRICE/HR   EST_COST       PHASE
nc-train-0         modal      H100:8        23.7000    148.12500000   Bound
```

`EST_COST` carries eight decimals to the rate's four because each checkpoint measures its window
from the field it wrote last time, so digits dropped there are money dropped, not display noise: at
four decimals a claim under $0.003/hour rounded back to its previous value every minute and never
accrued at all. Round it when you show it.

The counter is advanced only *after* the field's patch lands. A counter has no idempotency key, so
a window booked before its write was durable would be charged twice: the anchor would not have
moved, and the next tick would re-derive the same window. The persisted anchor is also why no time
is lost to a restart — the first tick back charges the whole gap, downtime included — and why the
window is measured from `status`, not from how long the loop actually slept.

**Teardown closes the last window too.** No checkpoint can close it — the object is going away, so
there is nothing left to re-anchor — but it is still booked on the counter, gated by the same rule:
strictly after the finalizer removal is accepted. That `Update` is a compare-and-swap, so it lands
at most once and only for a claim whose ledger is current, which is the same exactly-once guarantee
the anchor gives the checkpoint path. Without it the metric would miss far more than one window: an
instance that died before its first accrual tick would be billed **nothing at all**, making short
workloads look free while long ones stayed accurate.

`EST_COST` does *not* get that last window, so it understates a reclaimed instance's lifetime by up
to one interval while the counter has the whole of it. The lifetime figure also goes to the log
(`claim finalized`), the only record that outlives both the object and Prometheus retention.

**Where the number comes from.** `NodeClaim.status.priceUSDPerHour`, resolved from the
provider's catalog (`pkg/provider/catalog/data/*.csv`) against the served Pod's shape and
written once, on the claim's first reconcile — pinned so a catalog edit cannot retroactively
reprice a running instance. These are hand-maintained list prices: no committed-use discounts,
no private pricing, no gap between a Spot quote and the actual charge. Reconcile against the
provider's billing export before anyone gets invoiced.

**Which claims are charged.** Only `Bound` and `Terminating` hold an instance (see
`NodeClaimPhase`), so only they accrue; a `Terminated` claim's `EST_COST` is its frozen final total.
`Provisioning` is excluded, undercounting by about one poll interval per instance.

An instance that ends on its own — a preemption, a crashed sandbox — reaches `Terminated` without
passing through `Terminating`, and the loop will not touch it again. Teardown books the time since
its last checkpoint anyway, under `phase="Terminated"`, capped at one accrual interval: the claim
then waits in that phase until its Pod is deleted, and charging the wait would bill idle hours at GPU
rates. So that series is a real one, accurate to ±1 interval, and it separates spend that ended by
itself from spend we ended.

`Terminating` still bills until teardown finishes, which is what makes the `phase` label worth
having:

```promql
# Dollars burned on instances whose workload was already gone. Rising steadily means the
# teardown backstop is stuck.
sum(increase(nebula_cost_usd_total{phase="Terminating"}[1d]))
```

A claim with no usable price (no `Pricer`, or no catalog row) accrues **nothing**, never `0` — a
zero would be summed and averaged as a real "this costs nothing". Fleet totals therefore
under-report by whatever cannot be priced; an empty `PRICE/HR` in `kubectl get nc` finds those
claims.

## Attribution

Who to charge. Off by default; `--cost-labels` turns it on:

```
--cost-labels=example.com/org-id,example.com/team-id
```

Each entry is a **Pod label key**, written exactly as the Pod carries it. The metric label is
*derived* from it: the whole key, with `/`, `-` and `.` folded to `_`.

| `--cost-labels` entry | Pod label read | PromQL label |
| --- | --- | --- |
| `example.com/org-id` | `example.com/org-id` | `example_com_org_id` |
| `team.id` | `team.id` | `team_id` |
| `org_id` | `org_id` | `org_id` |

So a key is configured the way Kubernetes spells it and emitted under a name Prometheus accepts,
with nothing thrown away in between — the same convention kube-state-metrics
(`label_app_kubernetes_io_name`) and Prometheus service discovery
(`__meta_kubernetes_pod_label_…`) follow. Keeping the prefix is what makes `a.com/org-id` and
`b.com/org-id` two distinct series rather than a collision, and what keeps a qualified key from
quietly shadowing a label the metric already carries.

Rejected at startup: a key Kubernetes would not accept, one whose derived name Prometheus would not
(anything starting with a digit — a bare `2team`, or a domain like `4paradigm.com/org-id`; use an
unqualified Pod label there), the same key twice, two keys folding to the same name (`org-id` and
`org.id` still meet), and a bare key that shadows a label the metric already carries (`provider`,
`region`, `phase`, …). Every one of those fails the process at boot instead of silently reporting
every tenant as `none`.

Two names nothing rejects but you should still avoid: **`job` and `instance`**. Prometheus attaches
its own at scrape time, and with the default `honor_labels: false` it renames yours to
`exported_job` / `exported_instance` — so `sum by (job)` would quietly report the scrape job instead
of the tenant, with no error anywhere. `job_id` or `workload` if that is the breakdown you want.

The values are read off the served Pod once — when the claim first becomes chargeable, in the same
status patch that opens its billing window — and pinned on `NodeClaim.status.costLabels`, keyed by
the **Pod** key rather than the derived one, so the record reads like the Pod it came from:

```yaml
status:
  costLabels:
    example.com/org-id: acme
    example.com/team-id: ml
```

Three things follow from pinning them there rather than reading the Pod at accrual time:

- **Spend cannot be re-attributed.** Relabelling a Pod does not move dollars already reported
  under another tenant, exactly as a catalog edit cannot reprice a running instance.
- **The last window is attributable.** A `Terminating` claim's Pod is often already gone, and that
  window is [booked at teardown](#cost) — from `status`, which is still there.
- **The breakdown is auditable per claim.** `kubectl get nodeclaim -o yaml` shows who a given
  instance was charged to, which no aggregate metric can answer.

A claim serves exactly one Pod, UID-pinned (`spec.podRef`), so its whole rate belongs to that one
workload: nothing is split, and nothing is counted twice.

```promql
# Yesterday's bill, per tenant.
sum by (example_com_org_id) (increase(nebula_cost_usd_total[1d]))

# One team's burn rate, in USD/hour.
sum(rate(nebula_cost_usd_total{example_com_org_id="acme",example_com_team_id="ml"}[30m])) * 3600
```

**Values are tenant-controlled, which is a cardinality risk** — and the only one on this endpoint,
since every other label is bounded by configuration. A counter never releases a series, so a
workload generator emitting a fresh `org_id` per Pod leaks one series per Pod for the life of the
process. **Nothing caps this.** Past 5000 cost series the manager logs a warning once:

```
WARNING: the cost metric has passed its expected series budget, which usually means an
attribution label is carrying a per-Pod value. Nothing is dropped or merged, so the dollars
stay correct, but memory and every scrape grow until this process restarts.
```

Warning rather than enforcing is deliberate. Merging tenants into an `overflow` bucket, or dropping
the window, each corrupts a metric an external billing service reads as truth — and silently, in
the one direction its consumer cannot detect. A metric that is too *big* is an operator's problem
with an obvious fix (constrain the values at admission — a webhook or a policy engine); a metric
that quietly rewrote its own labels is nobody's problem until invoicing.

Prometheus knows the real number, so alert on it there rather than trusting the log line to be seen:

```promql
count(nebula_cost_usd_total) > 5000
```

A Pod that carries none of the configured labels reports `none`, the same placeholder every other
absent label uses. A Pod that carries the label with an *empty* value reports `none` too: an empty
tenant id names no payer, so it is folded into the unattributed pool rather than splitting it.

**Changing `--cost-labels` changes the identity of every cost series.** Adding a name resets each
one to zero and starts a new set, which `increase()` reads as a reset and handles, but no
historical series will carry the new label. Roll it out at a boundary you are happy to see in a
dashboard.

## Label semantics

Every series except the `{pool,reason}` and `{provider,capacity_type,region,reason}`
diagnostics carries the same label set:

```
provider  region  capacity_type  accelerator  accelerator_count
```

That is on purpose: a placement, the provisioning attempt it led to, and the cost of the
instance it produced carry **identical label values**, so they join in PromQL without label
surgery — "placed on Spot but never provisioned" is one query, and so is "what did our
Spot fallbacks cost us". For the same reason the two duration histograms measure
adjacent legs of one journey: `placement_wait` ends exactly where `instance_ready`
begins, so together they cover `kubectl apply` to `Running`.

Five label values are load-bearing:

- **`none`** is the placeholder for a label the request genuinely did not carry: no region
  (unconstrained), no capacity tier (the provider's default), no accelerator (a CPU-only
  Pod). An explicit token beats an empty string, which in PromQL is indistinguishable from
  a label that was never set and silently matches `{region=""}` selectors nobody meant to
  write. It is a plain word rather than an unforgeable token, which a real value can therefore
  collide with — see [Known gaps](#known-gaps).
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

Cardinality is bounded by *configuration*, not by workload, on every label above: providers x
regions x tiers x accelerator pools, all of which come from NodePools and provider catalogs. That
is what makes it safe that they are in-process counters whose series live until the process exits —
including cost, which is why it carries no claim identity.

The one exception is [attribution](#attribution), whose values come from Pod labels. It is off by
default, and warns rather than caps when on — so if you enable it, the bound is whatever your
admission policy puts on those label values.

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

# What the Spot fallback actually cost over the last week: the same candidate labels, so this
# joins the placement decision to the bill.
sum by (capacity_type) (increase(nebula_cost_usd_total[7d]))

# Dollars per placement, by candidate. Rising without a price change means instances are
# being held longer per workload.
sum by (provider, accelerator) (increase(nebula_cost_usd_total[1d]))
  / sum by (provider, accelerator) (increase(nebula_placement_decisions_total[1d]))

# Current burn rate in USD/hour, by accelerator.
sum by (accelerator) (rate(nebula_cost_usd_total[30m])) * 3600
```

## Known gaps

All of these are deliberate, and all bias toward looking *better* than reality — worth
knowing before trusting a dashboard.

- **Counters reset on restart, and lose their unscraped tail.** Every counter and histogram here is
  in-process, cost included: a redeploy or a leader handoff starts it at zero. `rate()` and
  `increase()` detect the reset, but the increments booked *since the last scrape* were never
  observed by anyone, and for cost they cannot be re-booked — the next leader measures from
  `status.lastAccruedAt`, which has already advanced past them. So an unclean exit drops up to one
  scrape interval of spend from the counter, and a crash landing between a checkpoint's write and its
  booking drops that window too. **`status.estimatedCostUSD` is what survives this** — cross-check
  against it after a redeploy rather than treating the counter as durable. Aggregate as
  `sum(increase(...))`, never `increase(sum(...))`: only the former sees the per-series reset.
- **An instance shorter than one scrape interval is invisible to `increase()`.** A counter's first
  sample carries no information — `increase()` recovers a *rise* between two samples — so dollars
  that arrive on a series' first sample are in `nebula_cost_usd_total` but in no `increase()` or
  `rate()` query over it. Nebula heads this off by publishing every chargeable claim's label set at
  `0` when the window opens (see [Cost](#cost)), which covers any claim that survives a scrape; a
  claim whose baseline and whole spend land between two scrapes is not covered, and with
  `--cost-labels` on — where the series is one tenant's — that tenant's entire bill can read as zero.
  Cross-check against `status.estimatedCostUSD` and the `claim finalized` log, which are the durable
  records. Closing it properly means a durable per-window event stream, not a counter.
- **`EST_COST` understates a reclaimed instance; the metric does not.** The field is not written on
  the deletion path (the object is going away), so it misses the window still open at teardown —
  which the counter *does* book (see [Cost](#cost)). The two therefore disagree by up to one
  interval on any claim that has been reclaimed, the counter being the complete figure and the log
  line agreeing with it. `EST_COST` also lags live spend by up to one interval while a claim is
  running — a freshness limit, not an error, since a window not charged now is charged in full next
  tick.
- **The last window of a self-terminated instance is capped, not measured.** A `Terminated` claim's
  anchor froze when the instance died, and nothing since distinguishes "died a minute ago" from
  "died while the manager was down three hours ago", so teardown charges one interval either way
  (see [Cost](#cost)). The cap is the safe direction — it cannot bill idle hours — but a preemption
  during an outage is undercharged by the whole outage. Nothing is booked at all if the Pod object
  never goes away, since teardown is what triggers it.
- **A crash between the teardown write and the booking loses that window.** The final window is
  booked in-process right after the finalizer removal lands, and unlike a checkpoint it has no
  anchor to fall back on, so a process that dies in between charges it nowhere. Exactly-once in the
  direction that matters — it cannot double-charge — but it can drop one partial window per
  unlucky teardown.
- **Cost is a list price, and misses two windows** — `Provisioning` claims, and any claim the
  provider cannot price. See [Cost](#cost); reconcile against the billing export before
  invoicing anyone.
- **Attribution is only as good as the Pod's labels, and is frozen.** A Pod that was missing its
  `org_id` when the claim became chargeable is booked to `none` for its whole life; labelling it
  afterwards does not backfill, by design (see [Attribution](#attribution)). Enforce the labels at
  admission — a webhook or a policy engine — rather than trusting the metric to notice.
- **`none` is a placeholder a real value can forge.** A Pod whose attribution label reads literally
  `none` shares a series with every Pod that carries no such label, merging that tenant's spend into
  the unattributed pool. The alternatives were each worse in a way that mattered more: an unforgeable
  token (`<none>`) is unreadable and vanishes wherever a label value reaches HTML, and the empty
  string is what PromQL uses for "label absent", so `{org_id=""}` could no longer select the
  unattributed bucket on its own. Pick attribution keys whose values your admission policy
  constrains.
- **Attribution cardinality is unbounded.** Enabling `--cost-labels` promotes tenant-controlled
  values onto a counter that never releases a series, and nothing caps it: a bad label choice grows
  manager memory and the scrape payload until a restart. The 5000-series warning is a smoke alarm,
  not a limit — see [Attribution](#attribution) for why it does not enforce, and alert on
  `count(nebula_cost_usd_total)`. Budget three series per distinct label set, not one: the baselines
  above are published for all three billing phases, and a claim only ever spends under two of them.
- **`instance_ready_duration` under-samples slow boots.** The start timestamp lives only in
  the virtual node's in-memory tracking map, so a provision still in flight when the
  manager restarts is re-adopted without one and is never observed. A missing sample beats
  a wrong one — measuring from re-adoption would report a wait of minutes as microseconds —
  but the bias runs toward looking fast. Closing it needs the start time persisted durably
  (a Pod annotation or NodeClaim status field), which is a write on the provisioning path.

`placement_wait_duration` has no equivalent gap: its start timestamp is the Pod's own
`creationTimestamp`, so a placement that happens after a manager restart still reports the
true total wait.
