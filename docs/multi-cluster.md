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
   namespace if missing and creates the real pod. That is the placeholder's spec, named after
   the claim (`<namespace>-<pod>`), with Nebula's scheduling replaced by a node selection for the
   GPU's instance families and the capacity type. Its env holds the values resolved from the
   placeholder's references at placement. Its imagePullSecrets stay as written: that cluster's
   kubelet pulls with the [reflected](#pull-secrets) copies, and one that isn't reflected fails
   the pod there, as on any node.
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
| [`config/kubernetes-provider/remote-rbac.yaml`](../config/kubernetes-provider/remote-rbac.yaml) applied | What group `nebula` may do: pods, secrets, configmaps, namespaces, events. |
| Its EKS API reachable from the Nebula manager | Every poll lists the real pods there. |
| Its log agent forwarding Nebula's pods to the host's application log group | The host's log readers query only that group. Its streams there are prefixed `<cluster>-`. |

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
  - name: my-cluster            # the EKS cluster's own name
    provider: eks
    region: eu-north-1
    geography: eu               # a NodePool's regions match this or region
  - name: gpu-usw2
    provider: eks
    region: us-west-2
    geography: us
```

Rules, enforced at load (a bad table leaves the provider unregistered):

- names are unique, `provider` is `eks`, and **one cluster per region**;
- `geography` is one of Nebula's geographies (`us`, `eu`, `uk`, `ap`, …);
- no other field: the table is parsed strictly, so a misspelt key fails the load.

## Installing

Everything lives in [`config/kubernetes-provider/`](../config/kubernetes-provider/), a kustomize
component that `config/default` includes on request:

| File | What it is | Applied |
|---|---|---|
| `clusters.yaml` | the cluster table | rendered into the `kubernetes-clusters` ConfigMap |
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

## Labels on the real pod

The real pod carries only an allowlist of its placeholder's labels: the `--cost-labels` keys, plus
any keys listed in `--kubernetes-propagate-labels` (empty by default), e.g. `app` for a log pipeline
that selects workloads by it. Values are copied unchanged, and Nebula's own labels
(`nebula.inftyai.com/*`, `app.kubernetes.io/managed-by`) cannot be listed.

It is an allowlist because, in the host cluster, the real pod shares the placeholder's namespace:
a controller label such as `pod-template-hash` would let the placeholder's owner adopt it, and
anything selecting on labels (a Service, a PodDisruptionBudget, an operator) would count the
sandbox twice. Add a key only if nothing in that namespace selects on it.

## Pull Secrets

Nebula copies the host cluster's image pull Secrets (`kubernetes.io/dockerconfigjson`) that a
rule in the table's `reflection` section selects into the same
namespace of every other cluster, creating the namespace if needed, so a real pod there pulls a
private image with them. Nothing is copied unless a rule selects it:

```yaml
reflection:
  rules:
    - resources: [secrets]
      objectSelector: { matchLabels: { app: trainer } }
```

A rule selects objects of its `resources` (`secrets`, `configmaps`), in a namespace matching
`namespaceSelector` (omitted: any; a namespace's name is its `kubernetes.io/metadata.name`
label), whose labels match `objectSelector`, which is required and may not be empty. Select by
your own labels: `nebula.inftyai.com/` keys are Nebula's, and refused. `mode` is optional and only
`AllowList` today. A rule can select other Secrets and ConfigMaps too, but a real pod's env
doesn't read them yet: it holds the values resolved at placement.

A copy follows its source within seconds, so a container restarted there pulls with a registry
token that hasn't expired. It is deleted when its source is deleted or no rule selects
it any more, and every 10 minutes Nebula re-checks every selected object and every copy, for a
change it missed or a copy left while it was down. Copies carry `nebula.inftyai.com/reflected`;
an object of the same name that isn't one is never changed, and the source gets a
`ReflectionRefused` Warning Event instead.

A copy holds every key of its source, so select only what every target may hold. Kubernetes'
own objects (service-account tokens, `kube-root-ca.crt`) carry no labels a rule would select;
don't add some to them.

## Networking

Nebula writes no NetworkPolicies. A real pod's traffic to and from the host and other
namespaces is whatever each cluster's own network allows, so apply your own policies if
tenants must not reach each other's pods.

Give each target one Nebula host: a real pod keeps its placeholder's namespace, so two hosts
placing into one target would put tenants from both into the same-named namespace there.

## Checking it works

```bash
kubectl -n nebula-system logs deploy/nebula-controller-manager | grep -E "registered provider|skipping provider"
kubectl get node nebula-kubernetes          # Ready once the provider is registered
```

Then run a small GPU workload pinned to one cluster (a NodePool with `regions: [<region>]`):
the placeholder should go Running with the real pod's IP, and deleting it should remove the
real pod, after which that cluster's Karpenter removes the empty node.

To check that a target forwards, look for recent events in its streams in the host's group:

```bash
aws logs filter-log-events --region <host region> \
  --log-group-name /aws/containerinsights/<host cluster>/application \
  --log-stream-name-prefix <target cluster>- \
  --start-time $(( ($(date +%s) - 3600) * 1000 )) --max-items 1
```

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
