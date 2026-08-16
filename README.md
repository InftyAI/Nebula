<div align="center">

# Nebula

**The Control Plane for GPUaaS**

[![Discord](https://img.shields.io/badge/Discord-Join%20us-5865F2?logo=discord&logoColor=white)](https://discord.gg/7WTUuFqyS6)
![Go Version](https://img.shields.io/badge/go-1.24-00ADD8?logo=go&logoColor=white)
[![Go Reference](https://pkg.go.dev/badge/github.com/InftyAI/Nebula.svg)](https://pkg.go.dev/github.com/InftyAI/Nebula)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Run GPU workloads on any NeoCloud or hyperscaler or your own infrastructure through one Kubernetes API.

</div>

Nebula turns external GPU capacity into ordinary Pods: ask for an accelerator, and
it picks a provider, provisions the instance, and reclaims it when you're done — no
per-cloud glue. The API follows a Karpenter-style split:

- **NodePool** — policy: which providers are allowed, how to pick between them
  (cost / availability), failover behaviour, and the GPU shape.
- **NodeClaim** — one provisioned instance and its lifecycle, held by a finalizer
  so a paid instance is never leaked.

<p align="center">
  <img src="site/images/arch.png" alt="Nebula architecture: the operator's webhook, virtual kubelet and placement controller drive NodePools and NodeClaims onto hyperscalers, NeoClouds, Kubernetes clusters and on-prem servers" width="90%">
</p>

## How it works

1. **Opt in.** You label a Pod to request an accelerator.
2. **Gate.** A mutating webhook injects a scheduling gate (and a toleration for the
   virtual node) at Pod CREATE, so the Pod sits `SchedulingGated`.
3. **Place.** The placement controller picks a provider from the matching NodePool,
   stamps its `nodeSelector`, and lifts the gate — or leaves the Pod gated if no
   provider can serve it.
4. **Provision.** The Pod binds to that provider's virtual node; a per-provider
   virtual kubelet spins up the real instance and reports status (phase, endpoint)
   back onto the Pod.
5. **Reclaim.** A NodeClaim tracks the instance and guarantees teardown via a
   finalizer — even if the Pod is force-deleted while the virtual kubelet is down.

## Defining a NodePool

A NodePool declares which providers may serve a workload and how to pick between
them. Workloads target it by name via the `nebula.inftyai.com/nodepool` label. A
single pool can span multiple providers, so a workload fails over across clouds:

```yaml
apiVersion: nebula.inftyai.com/v1alpha1
kind: NodePool
metadata:
  name: gpu
spec:
  providers:
  - name: modal            # NeoCloud; regions omitted = place anywhere (cheapest)
  - name: aws              # hyperscaler; "us" expands to every US region
    regions:
    - us
    - eu-west-1            # or name one region exactly
  capacityTypes:           # prefer cheap Spot, fall back to OnDemand
  - Spot
  - OnDemand
  strategy: Ordered        # try providers in listed order (or LowestPrice)
  failover:
    blocklistTTL: 10m      # how long a failed placement is skipped
```

## Opting a workload in

Three labels on the **Pod template** (not the Deployment metadata) are all it
takes; Nebula fills in the rest:

```yaml
metadata:
  labels:
    nebula.inftyai.com/enabled: "true"          # opt in
    nebula.inftyai.com/nodepool: gpu            # which NodePool to place against
    nebula.inftyai.com/accelerator-type: h100   # GPU type (case-insensitive)
spec:
  containers:
  - name: workload
    image: nvidia/cuda:12.4.1-base-ubuntu22.04
    resources:
      limits:
        nvidia.com/gpu: "8"                     # GPU count
```

The accelerator **type** rides on the label and is matched case-insensitively
against the provider catalog (`pkg/provider/catalog/data`); the **count** rides on
the standard `nvidia.com/gpu` resource limit, so scheduling and provisioning read
the same number. Do not set `nodeName` or a provider `nodeSelector` yourself — the
placement controller owns those.

> `kubectl logs` and `kubectl exec` both work on Modal, `-f`/`--tail` and `-it`
> included: the manager serves the two kubelet routes the API server proxies.
> `--timestamps`/`--previous`/`--since` and `-c` are ignored, and a terminal resize is
> not forwarded. On providers that do not support them yet, both answer NotFound.

## Getting started

- See [docs/deploy.md](docs/deploy.md) to install
- See [config/samples](config/samples) for example NodePools and a runnable workload.
- See [docs/add-a-provider.md](docs/add-a-provider.md) to add a provider backend.
- See [docs/architecture.md](docs/architecture.md) for design details.
- See [docs/status.md](docs/status.md) for how instance lifecycle becomes Pod and
  NodeClaim status, per provider.
- See [docs/metrics.md](docs/metrics.md) for what is instrumented and how to query it.

## License

Apache-2.0 — see [LICENSE](LICENSE).
