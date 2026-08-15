# Adding a provider

A provider adapter teaches Nebula how to provision on one backend (a NeoCloud like
RunPod, or a hyperscaler like GCP). The control plane is provider-agnostic: it
drives everything through the `provider.Provider` interface and a price/availability
catalog, so a new provider is an adapter package plus a little wiring — no changes
to the placement controller, virtual kubelet, or NodeClaim controller.

Use `pkg/provider/modal` (NeoCloud, coarse regions, all optional) and
`pkg/provider/aws` (hyperscaler, a region is mandatory on every call) as
references.

## 1. Implement the adapter

Create `pkg/provider/<name>/` and implement `provider.Provider`
(`pkg/provider/provider.go`):

| Method | Responsibility |
| --- | --- |
| `Name()` | Stable identifier, e.g. `"runpod"`. Must match the provider name used in a NodePool and on the virtual node. |
| `Capabilities()` | Declare quirks (poll interval, preemption notice) so the control plane behaves generically instead of branching on name. |
| `Provision(ctx, pod, req)` | Create exactly one instance from the Pod (image/command/env/ports/cpu/memory + accelerator). Idempotent on `req.ClaimName`. Returns the instance id. |
| `Terminate(ctx, id)` | Destroy by id. **Must be idempotent** — terminating an already-gone instance returns nil (the NodeClaim finalizer relies on this). |
| `Get(ctx, id)` | Current state of one instance, or `(nil, nil)` if gone. |
| `List(ctx)` | Every Nebula-owned instance, in as few API calls as possible. This drives the poll loop — preemption is detected by an instance disappearing here. |
| `Offerings(ctx)` | Price/availability rows for the optimizer (see the catalog below). |
| `MapAccelerator(canonical, count)` | Translate a canonical accelerator (type + count) to the provider's own id; `ok=false` if unsupported. |
| `ClassifyProvisionError(err, accel, region)` | Map a Provision failure to the `BlockScope` failover should blocklist. Only an **auth** error widens to the whole provider (`DenyAll`); capacity, quota, and unrecognized errors are all scoped to that {accel, tier, region} so failover can route around one candidate instead of fencing off the provider. Delegate to `provider.ClassifyError` for the shared part and decorate only what is provider-specific (e.g. the region axis). |
| `ExpandRegions(declared)` | Turn a pool's `regions` into the region candidates placement will walk. `catalog.Base` passes them through unchanged — one candidate per declared region, token used verbatim. Override for **either** of two independent reasons: the tokens are not callable (`pkg/provider/aws` expands the group `us` into every US EC2 region via a static table, since `us` is not a region you can call), or the provider's create **cannot fail over**, in which case splitting shrinks the capacity pool instead of widening it (`pkg/provider/modal` collapses every declared region into ONE candidate). Note Modal's own names already include the group tokens, so it overrides for the *second* reason alone — the two axes are orthogonal. |

The Pod is the single source of truth for the workload shape; `ProvisionRequest`
carries only what the Pod cannot express (the optimizer's capacity tier and the
claim identity). Do not duplicate Pod fields onto the request.

### Wrap the errors your `Provision` returns

`ClassifyProvisionError` decides *how widely* to blocklist, but a separate predicate —
`provider.IsRejection` — decides *whether to blocklist at all*, and whether the Pod is
failed. It answers: did the provider make a decision about this request ("no capacity",
"over quota", "bad credentials"), or did we merely fail to find out what it would have
decided (a transport error, a timeout, a 503)?

Only a **decision** is acted on. An unattributable failure leaves the Pod
non-terminal at `Provisioning` for the pod controller to retry, and records nothing —
because failing a Pod there would stamp a terminal verdict on a request the provider may
well have accepted, reaping the Pod out from under a paid instance whose id was never
returned.

What this asks of an adapter: **wrap every error your `Provision` path returns with the
matching sentinel** (`fmt.Errorf("...: %w", provider.ErrNoCapacity)`). A wrapped sentinel
always outranks the message text, so it is the only way to be certain of the outcome.
Unwrapped errors fall back to a string heuristic that recognizes the obvious API
phrasings and treats transport markers (`rpc error`, `connection refused`, `503`, `EOF`)
as unattributable — a reasonable default, but not one to rely on for a condition you can
classify yourself.

The metrics say when an adapter has skipped this: an unwrapped rejection lands on
`nebula_provision_failures_total{reason="other"}`, so a sustained rate on that series for
your provider is a to-do list of conditions still to wrap (see
[metrics.md](metrics.md)).

Most adapters embed `catalog.Base` for the generic `Name`, `Offerings`, and the
identity `MapAccelerator`, overriding only what the provider does differently (see
how `pkg/provider/modal` embeds it).

## 2. Add the price/availability catalog

Add `pkg/provider/catalog/data/<name>.csv` (embedded at build time — see
`pkg/provider/catalog/catalog.go`). The `accelerator_type` column is the canonical
Nebula type a workload requests via the `nebula.inftyai.com/accelerator-type`
label (matched case-insensitively). Copy the header and column semantics from
`aws.csv`.

## 3. Register the provider name

Add a constant to the `const` block in `pkg/provider/registry.go`:

```go
ProviderRunPod = "runpod"
```

## 4. Wire it into the manager

In `registerProviders` (`cmd/main.go`), build the adapter and register it. A
provider whose credentials are absent must be **logged and skipped, not fatal** —
follow the existing Modal/AWS pattern:

```go
if p, err := runpod.NewSDKClient(ctx); err != nil {
    setupLog.Info("skipping RunPod provider registration", "reason", err.Error())
} else {
    provider.Register(p)
    setupLog.Info("registered provider", "provider", p.Name())
}
```

## 5. Wire its credentials

Credentials live in **one Kubernetes Secret per provider**, mounted via an optional
`envFrom.secretRef`. Two edits:

1. **`hack/deploy.sh`** — add a row to `PROVIDER_SECRETS`
   (`<secret-name>|<REQUIRED_KEYS>|<OPTIONAL_KEYS>`):

   ```bash
   PROVIDER_SECRETS=(
     "nebula-modal-credentials|MODAL_TOKEN_ID MODAL_TOKEN_SECRET|"
     "nebula-runpod-credentials|RUNPOD_API_KEY|"
   )
   ```

2. **`config/manager/manager.yaml`** — add an optional `secretRef` under `envFrom`:

   ```yaml
   - secretRef:
       name: nebula-runpod-credentials
       optional: true
   ```

Then add the new keys to `.env.example`. The deploy script creates the Secret from
`.env` and skips it when the keys are blank (the provider is then skipped at
registration — see [How credentials are handled](deploy.md#how-credentials-are-handled)).

## 6. Verify

Register the provider and reference it from a NodePool. A pool that names an
unregistered provider surfaces `Ready=False / UnknownProvider`, which self-heals
once the adapter registers:

```bash
kubectl -n nebula-system logs deploy/nebula-controller-manager | grep -i provider
kubectl get nodes -l nebula.inftyai.com/provider
```
