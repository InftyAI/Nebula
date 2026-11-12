# Multi-cluster: the kubernetes provider

The `kubernetes` provider places GPU workloads into other EKS clusters. Placement picks a
cluster the way it picks an AWS region; the provider creates the workload's **real pod** in
that cluster, where the cluster's own Karpenter launches and packs the nodes. The placeholder
in the host cluster mirrors the real pod's status and carries its IP (`status.podIP`).

```
host cluster (runs Nebula)                     target cluster (EKS + Karpenter)
┌──────────────────────────────┐               ┌─────────────────────────────────┐
│ placeholder pod  ──placement─┼──Provision───▶│ real pod (same namespace, named │
│  (nebula-kubernetes vnode)   │               │  after the claim) ──▶ Karpenter │
│ Nebula manager ──────────────┼──list/events─▶│ EKS API                         │
└──────────────────────────────┘               └─────────────────────────────────┘
```

A cluster listed in the table may be the host itself; it is then a target like any other.

## How a workload runs

Each cluster in the table is one region of the `kubernetes` provider, so placement picks a
cluster as it picks a region, from the pool's `regions` and `strategy` ([NodePools](#nodepools)).

1. **Provision** works in the chosen cluster, through its EKS API: it creates the Pod's
   namespace if missing, writes the [isolation](#namespace-isolation) policies when the table
   has `cidrs`, and creates the real pod. That is the placeholder's spec, named after the
   claim (`<namespace>-<pod>`), with Nebula's scheduling replaced by a node selection for the
   GPU's instance families and the capacity type. Secret-sourced env vars go into a Secret the
   real pod owns.
2. **That cluster's Karpenter** launches a node, and the real pod starts there.
3. **Status flows back.** Every 5 seconds Nebula lists the real pods in each cluster; the
   placeholder mirrors the real pod's phase and readiness, and carries its IP.
4. **A refusal fails over.** Karpenter's `FailedScheduling` events on the real pod say why it
   cannot launch:
   - *no capacity*: that cluster is blocked for the GPU for a while;
   - *the pod's own spec refused* (an untolerated taint, say): it is blocked for this workload
     alone, for 10 minutes;
   - *no answer*: a pod still unbound after 5 minutes counts as no capacity.

   The placeholder fails, and its controller's replacement goes to the next cluster that is
   not blocked.
5. **Deleting** the placeholder deletes the real pod; that cluster's Karpenter removes the
   empty node.

## What each cluster needs

**Every target cluster**

| Need | Why |
|---|---|
| EKS, with **Karpenter v1** and a GPU NodePool | Karpenter launches the nodes; Nebula checks its NodeClaim API at startup. The NodePool must allow the instance families of the GPUs you request (multi-GPU types too, or nothing packs). |
| The NVIDIA device plugin | Without it no node advertises `nvidia.com/gpu`. |
| An **access entry** mapping the host's Nebula IAM role to the Kubernetes group `nebula` | How the target recognises Nebula (STS-signed token; no kubeconfig or secret). |
| [`config/kubernetes-provider/remote-rbac.yaml`](../config/kubernetes-provider/remote-rbac.yaml) applied | What group `nebula` may do: pods, secrets, namespaces, events, network policies. |
| Its EKS API reachable from the Nebula manager | Every poll lists the real pods there. |
| *Isolation only:* AWS VPC CNI's network policy support (`networking.k8s.aws` CRDs) | Enforces the policies in [Namespace isolation](#namespace-isolation). |

**The host cluster**

- An IAM identity for the manager (EKS Pod Identity or IRSA), allowed `eks:DescribeCluster`
  on every cluster in the table and `ec2:DescribeInstanceTypeOfferings`.
- The provider enabled: `--providers` including `kubernetes`, and `--kubernetes-clusters`
  pointing at the mounted table (both already set in `config/manager/manager.yaml`).

> **The access entry goes stale if the IAM role is recreated.** EKS binds it to the role's
> internal ID, not its name: after a recreation every target answers `401 Unauthorized`
> until its access entry is recreated too.

## The cluster table

Read once at manager startup from `/etc/nebula/kubernetes/clusters.yaml` (ConfigMap
`kubernetes-clusters`). **Restart the manager after changing it.**

```yaml
clusters:
  - name: hive-b18270575c-dev   # the EKS cluster's own name
    provider: eks
    region: eu-north-1
    geography: eu               # a NodePool's regions match this or region
    host: true                  # the cluster Nebula runs in
    cidrs: [10.20.0.0/16]       # isolation only: its pod IP ranges (VPC CNI: the VPC CIDRs)
  - name: gpu-usw2
    provider: eks
    region: us-west-2
    geography: us
    cidrs: [10.21.0.0/16]
```

Rules, enforced at load (a bad table leaves the provider unregistered):

- names are unique, `provider` is `eks`, and **one cluster per region**;
- `geography` is one of Nebula's geographies (`us`, `eu`, `uk`, `ap`, …);
- `cidrs` are on **every** cluster or on none, never overlap, and have no host bits set;
- at most one cluster is `host`, and exactly one once `cidrs` are set; the manager's own pod IP
  must fall in the host's `cidrs`.

## Installing

Everything lives in [`config/kubernetes-provider/`](../config/kubernetes-provider/), a kustomize
component that `config/default` includes on request:

| File | What it is | Applied |
|---|---|---|
| `clusters.yaml` | the cluster table | rendered into the `kubernetes-clusters` ConfigMap |
| `cluster-network-policy.yaml` | the host-wide default deny, for isolation | only once enabled (step 4) |
| `remote-rbac.yaml` | what group `nebula` may do in a target | by hand, in each target (step 3) |

1. Fill in [`config/kubernetes-provider/clusters.yaml`](../config/kubernetes-provider/clusters.yaml).
2. Uncomment `- ../kubernetes-provider` in `config/default/kustomization.yaml`, then deploy:
   ```bash
   make deploy IMG=<registry>/nebula:<tag>
   ```
3. In **each target** cluster:
   ```bash
   kubectl apply -f config/kubernetes-provider/remote-rbac.yaml
   ```
   and map Nebula's IAM role to the group `nebula` with an access entry.
4. **If the table sets `cidrs`**, enable the default deny: uncomment `resources:` and
   `- cluster-network-policy.yaml` in `config/kubernetes-provider/kustomization.yaml`, set its
   `networks` to every non-host cluster's `cidrs`, and deploy again. Kustomize cannot derive
   them from the table, so update both files together whenever a cluster is added.

   > **Skipping step 4 leaves isolation without its boundary**: host namespaces that have
   > never placed a sandbox can reach every remote one, since targets admit the whole host
   > range.

After any change to `clusters.yaml`, deploy and restart the manager (it reads the table only
at startup):

```bash
kubectl -n nebula-system rollout restart deploy/nebula-controller-manager
```

## NodePools

A pool uses the provider like any other:

```yaml
providers:
  - name: kubernetes            # no regions: every cluster in the table, in table order
  - name: modal
```

`regions` narrows or reorders the clusters: a value matches a cluster's `geography` or exact
`region` (`[eu]`, `[us-east-1]`, `[us, eu]`). With `strategy: Ordered` a pod goes to the first
cluster that is not blocklisted and offers its GPU, so a burst fills one cluster and spills to
the next on a capacity refusal, rather than spreading. A cluster whose region lacks the GPU's
instance family is skipped at placement.

The provider enforces only `egress: Open` (it has no egress policy of its own); a pool with
`Blocked` or `Allowlist` never places on it.

## Namespace isolation

On whenever the table has `cidrs`. A host pod in namespace X then reaches only X's remote
pods, and a remote pod in X admits only X and the host.

| Where | Policy | Written by | Effect |
|---|---|---|---|
| host | ClusterNetworkPolicy `nebula-isolation-host-<X>`, **Admin, priority 500, `Pass`** | Provision | X's traffic to its own Nebula pods (whose placeholders carry the remote IPs) is left to X's own NetworkPolicies, exactly as for its local pods |
| host | ClusterNetworkPolicy `nebula-isolation-host-cluster`, **Admin, priority 1000** | `cluster-network-policy.yaml` (step 4) | every other namespace, and X's other traffic, is denied the targets' ranges |
| target | ClusterNetworkPolicy `nebula-isolation-target-<X>`, **Admin, priority 500** | Provision | no other namespace in the target reaches X's Nebula pods (`Deny`), whatever X's own NetworkPolicies allow; X's own traffic is left to them (`Pass`) |
| target | NetworkPolicy `nebula-isolation-target` in X | Provision | from outside the cluster, Nebula's pods admit only the host's range: the ClusterNetworkPolicy above cannot match CIDRs |

The ClusterNetworkPolicies are Admin-tier so that no namespace's own NetworkPolicy can override
the boundary. Keep the deny's `priority` above 500, so it runs after the per-namespace passes;
at or below 500 it would drop each namespace's traffic to its own remote pods too.
`nebula-system` is exempt, as the manager must reach the targets' API endpoints; add any other
namespace that legitimately needs the targets to the deny's `values` list in
`cluster-network-policy.yaml`.

One host per set of targets: each target's `nebula-isolation-target` admits exactly one host's
range, and two Nebula hosts placing into one target would overwrite it in turn, and put two
tenants of the same namespace name into one target namespace.

Pod NetworkPolicy does not apply to a host-network Pod, so admission refuses opted-in Pods
with `spec.hostNetwork: true`; one would otherwise run outside the fence in its target.

## Checking it works

```bash
kubectl -n nebula-system logs deploy/nebula-controller-manager | grep -E "registered provider|skipping provider"
kubectl get node nebula-kubernetes          # Ready once the provider is registered
```

Then run a small GPU workload pinned to one cluster (a NodePool with `regions: [<region>]`):
the placeholder should go Running with the real pod's IP, and deleting it should remove the
real pod, after which that cluster's Karpenter removes the empty node.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| `skipping provider registration` at startup | a bad table (the reason is logged), or **any one** cluster unreachable or refusing auth: the provider checks every cluster before registering |
| No `nebula-kubernetes` node | the provider is not registered (above); a node left from an earlier run goes NotReady instead |
| A cluster missing from placement after a change | the manager started before the new table: restart it |
| `401 Unauthorized` from a target | its access entry predates the IAM role: recreate the entry |
| `403 … User "…" cannot …` | `remote-rbac.yaml` missing or outdated in that target |
| `i/o timeout` to a target's API | network: the target's endpoint does not accept the manager's source address |
| Pods stay `SchedulingGated` | every serving cluster refused for capacity and is blocklisted; one probe is retried as each block lapses (a 4- or 8-GPU request is the usual cause) |
| Pods fail with `provider: pod spec refused: …` and move to the next cluster | that cluster's Karpenter rejects the pod's own spec, usually a taint it does not tolerate (the message is Karpenter's). The cluster is skipped for that workload for 10 minutes; a fixed Deployment template is a new workload and tries it straight away |
| A real pod pending for minutes, then the placeholder fails | no capacity reported by Karpenter, but the pod stayed unbound past the 5-minute bind timeout |

Known gap: `kubectl logs` against the placeholder returns `NotFound`; read the real pod's logs
in its cluster.
