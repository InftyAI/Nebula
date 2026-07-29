# Adding a provider

A provider adapter teaches Nebula how to provision on one backend (a NeoCloud like
RunPod, or a hyperscaler like GCP). The control plane is provider-agnostic: it
drives everything through the `provider.Provider` interface and a price/availability
catalog, so a new provider is an adapter package plus a little wiring — no changes
to the placement controller, virtual kubelet, or NodeClaim controller.

Use `pkg/provider/modal` (region-simple NeoCloud) and `pkg/provider/aws`
(region-aware hyperscaler) as references.

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
| `ClassifyProvisionError(err, accel, region)` | Map a Provision failure to the `BlockScope` failover should blocklist (a capacity error → that {accel, tier, region}; an auth/quota error → the whole provider). |

The Pod is the single source of truth for the workload shape; `ProvisionRequest`
carries only what the Pod cannot express (the optimizer's capacity tier and the
claim identity). Do not duplicate Pod fields onto the request.

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
