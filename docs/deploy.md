# Deploying Nebula

From an empty cluster to a GPU box running on AWS.

Nebula installs into a cluster you already have and provisions GPU capacity
*outside* it — so **the cluster itself needs no GPU nodes**. A 3-node Kind cluster
works. You need admin rights (Nebula installs CRDs and a webhook), plus Go 1.24+
and Docker, since there is no published image yet.

You also need a **public DNS name and TLS cert** for the SandD edge (step 2). Boxes
live on other people's networks and dial in to reach you; without that address there
is no way into a box you've paid for.

The steps below are in dependency order. The two that bite if you reorder them: the
signing key must exist **before** the manager starts (it exits at startup without one),
and the edge must be applied **after** the install, because it routes to a Service the
install creates.

---

## 1. Sources and credentials

Do this first: credentials are read at process start, so the manager boots already
configured.

```bash
git clone https://github.com/InftyAI/Nebula.git
# SandD too, BESIDE Nebula: the manager image compiles SandD's Rust controller into
# itself, and that archive is a build artifact nothing publishes, so the build needs this
# checkout (override with SANDD_DIR=<path>). The Go binding is NOT why — that comes from
# the module proxy like any other dependency.
git clone https://github.com/InftyAI/SandD.git

cd Nebula
cp .env.example .env && $EDITOR .env
```

Building the image therefore also needs a **Rust toolchain** ([rustup](https://rustup.rs))
for local `make build`/`make test`; `make docker-build` compiles it inside the image and
needs only Docker.

| Provider | Keys |
|---|---|
| **AWS** | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` — dev only; in production leave blank and use IRSA or an instance role |
| **Modal** | `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET` — from `modal token new` |

AWS needs EC2 permissions to launch and reclaim instances (`RunInstances`,
`CreateFleet`, `TerminateInstances`, the `Describe*` and `*LaunchTemplate` calls)
but **no pre-created infrastructure** — the adapter resolves the GPU AMI and
default-VPC subnets itself. There is no region setting; regions are per-NodePool.

A provider left blank is logged and skipped, not fatal. See
[How credentials are handled](#how-credentials-are-handled).

## 2. SandD

Every box runs the SandD daemon as PID 1 — that daemon dialing out is the *only* way
into a machine on someone else's cloud. So set this up **before** installing: a box
provisioned with SandD off has no control surface at all, and bills anyway.

```bash
kubectl create namespace nebula-system

# Signing key: the private Secret the manager mounts. No public half to distribute —
# the controller that verifies runs in the manager and derives it.
hack/gen-signing-key.sh
```

The controller daemons dial is **embedded in the manager** (it is Rust, linked in via
cgo), so there is no second Deployment to install. Two edits:

- `config/default/kustomization.yaml` — uncomment `- ../sandd` for the Service that
  fronts the manager's dial-in port. Step 3 creates it along with everything else; do
  **not** `kubectl apply -k config/sandd` separately, which would create it unprefixed
  as `sandd-controller` while the edge below routes to `nebula-sandd-controller`.
- `config/manager/manager.yaml` — uncomment `SANDD_SIGNING_KEY_PATH` and
  `SANDD_EXTERNAL_HOST`, setting the host to your public name. The key volume is
  already mounted, so this is two lines.

**Create the key before uncommenting either.** The manager exits at startup if the key
path is set but the key is absent.

`SANDD_EXTERNAL_HOST` cannot be derived — the manager's in-cluster Service name is
neither resolvable nor routable from a NeoCloud VM, so nothing the manager could
invent would be dialable. Only you know the name that fronts it. Set it but leave the
key out and the manager exits at startup: failing loudly beats provisioning instances
that bill indefinitely for something you cannot reach.

Then the edge, applied by hand because it carries your host and TLS cert:

```bash
$EDITOR config/sandd/ingress.yaml       # REPLACE both sandd.example.com lines
kubectl apply -f config/sandd/ingress.yaml -n nebula-system
```

**Apply it now, before the install.** It is admitted with no backend yet — the ingress
controller serves 503 until the manager's Service has endpoints, then picks them up on
its own — and getting it in early lets DNS propagation, LB provisioning and cert
issuance run *while* the image builds instead of after. Verify it in step 3, once
there is something behind it.

It must do WebSocket upgrade, TLS, and tolerate **hours of idle**. nginx's default 60s
`proxy-read-timeout` severs quiet-but-healthy daemon connections — the symptom is not
an outage but endless reconnect churn.

Details and current limits: [config/sandd/README.md](../config/sandd/README.md).

## 3. Install

```bash
make deploy-all IMG=<your-registry>/nebula:<tag>

# On Kind, load the image instead of pushing:
make deploy-all IMG=nebula:dev DEPLOY_KIND_CLUSTER=<cluster-name>
```

One run does image, Secrets, CRDs and the manager, in the order that lets the manager
boot configured. Re-running is safe but rebuilds the image — use `make deploy` when only
manifests changed.

The image build stages the SandD sources into the Docker context and compiles the Rust
archive there, which is why step 1 clones that repo.

```bash
kubectl -n nebula-system get pods          # the manager Running

kubectl -n nebula-system logs deploy/nebula-controller-manager | grep -iE "provider|sandd"
# registered provider {"provider": "aws"}
# SandD enabled with an embedded controller ... endpoint=wss://sandd.example.com/ws

kubectl get nodes -l nebula.inftyai.com/provider   # one per registered provider
```

**Check that endpoint is your public host**, not an in-cluster name — it is the
address every box will dial. Now that there is a backend behind it, confirm the edge
answers:

```bash
curl -i https://sandd.example.com/ws   # 401 = success (reached the controller, auth on)
                                       # 404 = wrong path; 502/503 = wrong Service/port,
                                       #   or the manager is not up yet
```

**Get a 401 before provisioning anything.** Until the edge answers, every box you launch
dials an address that cannot respond — and bills while it retries.

Those virtual nodes are **not machines** — each is a placement target whose virtual
kubelet provisions real instances at the provider.

The first few seconds log `waiting for the webhook certificate to be ready` and
reconcile nothing, while the manager mints its own cert
([why](#webhook-tls-no-cert-manager)).

## 4. NodePool

The policy: which providers may serve a workload, in what order.

```yaml
apiVersion: nebula.inftyai.com/v1alpha1
kind: NodePool
metadata:
  name: gpu
spec:
  providers:
  - name: aws              # region-aware: at least one region required
    regions: [us-east-1, us-west-2]
  capacityTypes: [Spot, OnDemand]
  strategy: Ordered
  failover:
    blocklistTTL: 10m
```

Capacity type is the **outer** axis, region the **inner** one: every provider's Spot
is tried in every region before *any* provider's OnDemand. That is what makes one
pool fail over across clouds.

```bash
kubectl apply -f config/samples/nebula_v1alpha1_nodepool.yaml
kubectl get nodepool gpu -o jsonpath='{.status.conditions}' | jq
# Ready=False / UnknownProvider -> that provider did not register (step 1)
```

## 5. A GPU box

A `Sandbox` is one remote box — the smallest thing to ask for:

```yaml
apiVersion: nebula.inftyai.com/v1alpha1
kind: Sandbox
metadata:
  name: sample
spec:
  nodePoolRef: gpu
  image: ubuntu:24.04     # default. The GPU works (the driver is injected), but
                          # the CUDA toolkit isn't here — ask for a CUDA image
                          # explicitly if you need nvcc.
  acceleratorType: t4     # omit, with the limit below, for a CPU-only box
  resources:
    limits:
      nvidia.com/gpu: "1"
```

```bash
kubectl apply -f config/samples/nebula_v1alpha1_sandbox.yaml
kubectl get sandboxes -w
# Pending -> Provisioning -> Initializing -> Ready
kubectl get nodeclaims          # the instance ledger; this guarantees teardown

# Its daemon should now appear in the controller's registry.
kubectl -n nebula-system port-forward svc/nebula-sandd-controller 8765:8765
curl -s localhost:8765/stats | jq
```

**`t4` is the cheap AWS pick** (`g4dn.xlarge`, 1 GPU, ~$0.53/hr). The checked-in
sample asks for `a100-40gb`, which on AWS resolves only to `p4d.24xlarge` — 8 GPUs
at ~$32/hr — so use the snippet above or edit the file first.

A box in `/stats` is one you can shell into:

```bash
kubectl exec -it <pod> -- bash
```

That works because the virtual node serves the exec route of the kubelet API
(`pkg/vnode/kubelet.go`) on the manager's pod IP, and advertises the address+port on the
Node so the API server knows where to send the request; the relay behind it
(`pkg/vnode/exec.go`) carries the session over SandD to the daemon on the instance. Two
requirements follow from that path: SandD must be **on** (with it off no exec endpoint is
served at all), and the manager Pod needs `POD_IP` from the downward API — it is already
in `config/manager/manager.yaml`, but a hand-rolled manifest without it leaves exec
unable to connect.

Only `exec` is served. `kubectl logs`, `attach`, `port-forward` and `top` return **501 Not
Implemented** on these nodes.

`SandboxSet` keeps N boxes ready, since provisioning takes minutes.

For a normal Deployment instead, three labels on the **Pod template** opt it in
(`config/samples/deployment.yaml`): `enabled: "true"`, `nodepool: gpu`, and
`accelerator-type: t4`, with the count on `nvidia.com/gpu`. Don't set `nodeName` or a
provider `nodeSelector` — placement owns those. A Pod stuck `SchedulingGated` is how
Nebula says "I can't place this"; the manager logs why.

## Uninstalling

```bash
kubectl delete sandboxes --all      # workloads FIRST
kubectl get nodeclaims              # wait until empty
make undeploy && make uninstall
```

**Order matters.** Each NodeClaim holds a terminate finalizer, and the controller that
reclaims the instance runs *in the manager*. Remove the manager first and you are left
with billing instances and finalizers nobody will release.

---

## Troubleshooting

| Symptom | Cause |
|---|---|
| Pod stuck `SchedulingGated` | No pool matched, no provider offers that GPU type, or every candidate is blocklisted |
| `NodePool UnknownProvider` | That provider's credentials are missing or its Secret is misnamed |
| Pods rejected at create | Webhook cert not ready yet (`failurePolicy: Fail`); retry in a few seconds |
| No virtual nodes | No provider registered — check the step 3 log line |
| Box `Ready`, absent from `/stats` | The endpoint handed to it isn't reachable — re-check `SANDD_EXTERNAL_HOST` |
| Daemons reconnect every ~60s | The edge's idle timeout is cutting healthy connections |
| `exec` says `NotFound` | The instance has no daemon connected — check `/stats` first |
| `exec` times out dialing the node | The manager Pod is missing `POD_IP`, so the node advertises no routable address |
| `exec` unsupported on the node | SandD is off; no exec endpoint is served without it |
| `logs`/`attach`/`port-forward` return 501 | Expected; only `exec` is implemented |

## How credentials are handled

**One Secret per provider**, not a shared one, matching the "creds-absent → skip that
provider" model. Each is an **optional** `envFrom.secretRef` on the manager
(`config/manager/manager.yaml`), so the manager boots even when one is missing, and
`registerProviders` in `cmd/main.go` logs and skips it. A `NodePool` naming an
unregistered provider reports `UnknownProvider`, which self-heals once the creds land.
Splitting per provider isolates rotation and RBAC.

> **A misnamed Secret looks exactly like an absent one**, because `envFrom` is
> optional. `UnknownProvider` is your signal.

To add a provider to a **running** install, fill in `.env`, re-run `make deploy-all`,
then `kubectl rollout restart deployment/nebula-controller-manager -n nebula-system`.
The restart is easy to miss: credentials are env vars read once at process start, and
`apply` on an unchanged Deployment rolls no new pod, so the Secret sits unread.

## Webhook TLS (no cert-manager)

The API server needs TLS to call Nebula's mutating webhook. **cert-manager is
intentionally not used**: the manager mints its own serving cert in-process at startup
(`pkg/cert`), stores it in the `nebula-webhook-server-cert` Secret, patches that cert's
CA into the `MutatingWebhookConfiguration`, and **renews** it before expiry — the part
a one-shot script could never do. Deriving the caBundle from the cert it just wrote is
what keeps the two from drifting.

Nothing depending on Pod admission is registered until the cert is ready. That is
deliberate: with `failurePolicy: Fail` a Pod created earlier would be rejected, and
without the gate it would be scheduled by vanilla Kubernetes — silently bypassing
placement.

To switch to cert-manager, re-add `- ../certmanager` and the `CERTMANAGER` replacements
in `config/default/kustomization.yaml`, and drop the `CertsManager` call from
`cmd/main.go`.

## Configuration

`make` variables: `IMG`, `NAMESPACE` (default `nebula-system`), `DEPLOY_KIND_CLUSTER`
(load into Kind instead of pushing), `KUBECTL`.

Manager environment (`config/manager/manager.yaml`):

| Variable | Default | Meaning |
|---|---|---|
| `NEBULA_CATALOG_DIR` | `/etc/nebula/catalog` | Price catalog mount; falls back to the CSVs in the binary |
| `SANDD_SIGNING_KEY_PATH` | *(unset = off)* | The single switch for SandD |
| `SANDD_EXTERNAL_HOST` | *(required with the above)* | Public address daemons dial |
| `SANDD_SIGNING_KID` | `default` | Key id in the JWT header |
| `SANDD_TOKEN_ISSUER` | `nebula` | `iss` claim |
| `SANDD_TOKEN_TTL` | `24h` | Daemon token lifetime |

Not using `deploy-all` (GitOps, sealed-secrets)? Create the namespace and credential
Secrets yourself, build and push the image (`make docker-buildx`), then `make deploy`
— which applies manifests only. Order matters for the same reason as above.
`config/manager/modal-credentials.example.yaml` is a declarative Secret to copy.

Deeper reading: [architecture.md](architecture.md),
[add-a-provider.md](add-a-provider.md).
