<div align="center">

# Nebula

**The control plane for GPUaaS**

[![Go Reference](https://pkg.go.dev/badge/github.com/InftyAI/Nebula.svg)](https://pkg.go.dev/github.com/InftyAI/Nebula)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
![Go Version](https://img.shields.io/badge/go-1.24-00ADD8?logo=go&logoColor=white)

Run GPU workloads on any NeoCloud or hyperscaler — AWS, Modal, and more — through one Kubernetes API.

</div>

Nebula turns external GPU capacity into ordinary Pods: ask for an accelerator, and
it picks a provider, provisions the instance, and reclaims it when you're done — no
per-cloud glue. The API follows a Karpenter-style split:

- **NodePool** — policy: which providers are allowed, how to pick between them
  (cost / availability), failover behaviour, and the GPU shape.
- **NodeClaim** — one provisioned instance and its lifecycle, held by a finalizer
  so a paid instance is never leaked.

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

## Opting a workload in

Three labels on the **Pod template** (not the Deployment metadata) are all it
takes; Nebula fills in the rest:

```yaml
metadata:
  labels:
    nebula.inftyai.com/enabled: "true"          # opt in
    nebula.inftyai.com/nodepool: aws            # which NodePool to place against
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

> `kubectl logs`/`exec` do not work against a virtual node (it doesn't serve the
> kubelet API); read workload output on the provider side.

## Getting started

See [docs/deploy.md](docs/deploy.md) to install and [config/samples](config/samples)
for example NodePools and a runnable workload. Design details live in
[docs/architecture.md](docs/architecture.md).
