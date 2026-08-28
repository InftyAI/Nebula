# Deploying Nebula

This guide covers building and deploying the Nebula manager to a Kubernetes
cluster, including wiring provider credentials.

- [Quick start](#quick-start)
- [How credentials are handled](#how-credentials-are-handled)
- [Webhook TLS (no cert-manager)](#webhook-tls-no-cert-manager)
- [What `deploy-all` does](#what-deploy-all-does)
- [Configuration](#configuration)
- [Modal Environments](#modal-environments)
- [Manual deployment](#manual-deployment)
- [Verifying the deployment](#verifying-the-deployment)
- [Smoke test](#smoke-test)

---

## Quick start

```bash
# 1. Put provider credentials in .env (secrets only — gitignored).
cp .env.example .env
$EDITOR .env            # set MODAL_TOKEN_ID / MODAL_TOKEN_SECRET (from `modal token new`)

# 2. Build, apply credential Secrets, and deploy in one step.
make deploy-all IMG=<your-registry>/nebula:<tag>
```

For a local [Kind](https://kind.sigs.k8s.io/) cluster, load the image instead of
pushing it to a registry:

```bash
make deploy-all IMG=nebula:dev DEPLOY_KIND_CLUSTER=<kind-cluster-name>
```

That's it. The manager comes up in the `nebula-system` namespace and registers
every provider whose credentials are present.

---

## How credentials are handled

Nebula keeps provider credentials in **one Kubernetes Secret per provider**, each
referenced by the manager Deployment through an **optional** `envFrom.secretRef`
(see `config/manager/manager.yaml`). So:

- The manager boots even when a Secret is missing.
- A provider whose credentials are absent is logged and skipped at registration,
  not fatal. A `NodePool` referencing it surfaces `Ready=False / UnknownProvider`,
  which self-heals once the creds are applied.

`.env` holds **only secrets**. Non-secret configuration (image, namespace, Kind
cluster) is passed as `make` variables.

> **A typo in a Secret name looks exactly like an absent one** — the provider is
> silently skipped. `UnknownProvider` on the `NodePool` is your signal.

---

## Webhook TLS (no cert-manager)

Nebula runs a mutating webhook (it injects the scheduling gate into gated Pods),
and the API server requires TLS to call it. **cert-manager is intentionally not
used**, and neither is any out-of-band setup step: the manager provisions its own
serving certificate in-process at startup (`pkg/cert`, built on
[cert-controller](https://github.com/open-policy-agent/cert-controller)). There is
nothing to install and nothing to run before `make deploy`.

At startup it mints a serving cert into the `nebula-webhook-server-cert` Secret,
patches the matching CA into the `MutatingWebhookConfiguration` `caBundle`, and
rotates the cert before it expires.

Two things this implies for the deployment:

- The volume at `/tmp/k8s-webhook-server/serving-certs` **must be that Secret**, not
  an `emptyDir` — the rotator writes only the Secret, and the kubelet projects it.
  With an `emptyDir` the files never appear and nothing starts, while the pod still
  looks healthy.
- The first seconds of a fresh install log
  `waiting for the webhook certificate to be ready` and reconcile nothing. That is
  expected: controllers wait for the cert so no Pod is admitted before the webhook is
  trusted.

To use cert-manager instead, re-add `- ../certmanager` and re-enable the `CERTMANAGER`
replacements blocks in `config/default/kustomization.yaml`, install cert-manager, and
drop the `CertsManager` call from `cmd/main.go`.

---

## What `deploy-all` does

`make deploy-all` runs `hack/deploy.sh`, which is idempotent (safe to re-run):

1. **Builds** the manager image (`make docker-build IMG=…`).
2. **Publishes** it — `docker push`, or `kind load docker-image` when
   `DEPLOY_KIND_CLUSTER` is set.
3. **Creates the namespace and credential Secrets** from `.env`, one per provider,
   before deploying — so the manager boots already-configured. A Secret is created
   only when *all* its required keys are set; a provider with blank keys is skipped.
4. **Deploys** CRDs + the manager (`make deploy IMG=…`).

There is no webhook-cert step and no CA-bundle injection step — the manager does both
itself at startup (see [Webhook TLS](#webhook-tls-no-cert-manager)).

---

## Configuration

`.env` (secrets only, gitignored — see `.env.example`):

| Key | Provider | Required | Notes |
|---|---|---|---|
| `MODAL_TOKEN_ID` | Modal | yes | From `modal token new` |
| `MODAL_TOKEN_SECRET` | Modal | yes | From `modal token new` |
| `MODAL_ENVIRONMENT` | Modal | no | Modal Environment to create sandboxes in. Blank omits the key and the SDK uses the token profile's default. See [Modal Environments](#modal-environments). |
| `AWS_ACCESS_KEY_ID` | AWS | dev only | Prefer IRSA / instance role in production and leave blank — the SDK's default credential chain finds the role. Set only for local/dev. |
| `AWS_SECRET_ACCESS_KEY` | AWS | dev only | Pairs with `AWS_ACCESS_KEY_ID`; both required together or both blank. |

Non-secret config, passed as `make` variables:

| Variable | Default | Meaning |
|---|---|---|
| `IMG` | `controller:latest` | Manager image to build and deploy |
| `NAMESPACE` | `nebula-system` | Namespace the manager runs in |
| `DEPLOY_KIND_CLUSTER` | *(empty)* | If set, load the image into this Kind cluster instead of pushing. Separate from the e2e `KIND_CLUSTER`, so a plain `make deploy-all` pushes. |
| `KUBECTL` | `kubectl` | kubectl binary to use |

Manager flags worth knowing (edit `config/manager/manager.yaml` `args`):

| Flag | Default | Meaning |
|---|---|---|
| `--kubelet-bind-address` | `:10250` | Where the kubelet log endpoint listens — the address the API server proxies `kubectl logs` to. Set it empty to disable the endpoint, which disables logs and nothing else. |
| `--kubelet-serving-tls-bootstrap` | `true` | Request a certificate for the advertised Pod IP from the `kubernetes.io/kubelet-serving` signer. Until it is approved and issued, the endpoint retains its self-signed fallback. Disable this only when the API server does not verify kubelet serving certificates. |
| `--kubelet-client-ca` | *(empty)* | PEM bundle of CAs whose client certificates are accepted on that port. **Empty means client certificates are not verified**, so anything able to reach port 10250 can read the logs of any Pod on Nebula's virtual nodes. Set it to your API server's kubelet client CA to require mTLS, or keep the port closed with a NetworkPolicy. The default is open because which CA signs that client cert is not portable — kubeadm uses the cluster CA, EKS/GKE their own — so requiring it by default would break logs on managed control planes. |

The endpoint needs `POD_IP` (projected via `fieldRef` in `config/manager/manager.yaml`)
because virtual nodes advertise the leader's Pod IP, not a Service. Running the manager
off-cluster leaves it unset, and logs degrade to unsupported. See
[kubelet-api.md](kubelet-api.md).

The Kubernetes signer does not approve kubelet-serving requests itself. On a cluster
without a dedicated approver, inspect and approve Nebula's request after each manager
Pod recreation and certificate renewal:

```bash
CSR=$(kubectl get csr \
  -l app.kubernetes.io/name=nebula,app.kubernetes.io/component=kubelet-serving-certificate \
  --sort-by=.metadata.creationTimestamp -o name | tail -n1)

# Confirm the requested IP SAN matches the manager Pod IP before approving it.
kubectl get csr "$CSR" -o jsonpath='{.spec.request}' \
  | openssl base64 -d -A | openssl req -text -noout
kubectl -n nebula-system get pod -l control-plane=controller-manager -o wide

kubectl certificate approve "$CSR"
kubectl -n nebula-system logs deploy/nebula-controller-manager \
  | grep 'installed trusted kubelet serving certificate'
```

An installation with an external CSR approver should restrict it to requests that
match Nebula's ServiceAccount, `system:nodes` organization, manager Pod identity, and
current Pod IP. Nebula intentionally receives no permission to approve certificates.

---

## Modal Environments

Optional, and off unless you set it. A Modal **Environment** is a named partition of
object *names* — apps, secrets, volumes — inside one workspace.

```bash
# once, on the Modal side — Nebula never creates an environment
modal environment create dev

# then, in .env
MODAL_ENVIRONMENT=dev
```
---

## Manual deployment

If you don't want the script (e.g. you manage Secrets via sealed-secrets or a
GitOps pipeline), do the same steps by hand. Order matters — create the Secrets
before deploying so the manager boots configured:

```bash
# 1. Namespace.
kubectl create namespace nebula-system --dry-run=client -o yaml | kubectl apply -f -

# 2. Modal credential Secret (before deploy — read as env at pod startup).
# The MODAL_ENVIRONMENT literal is optional — drop that line for the token
# profile's default environment (see Modal Environments above).
kubectl create secret generic nebula-modal-credentials -n nebula-system \
  --from-literal=MODAL_TOKEN_ID=ak-... \
  --from-literal=MODAL_TOKEN_SECRET=as-... \
  --from-literal=MODAL_ENVIRONMENT=dev

# 3. Deploy CRDs + manager. The pod reads creds on first boot, and provisions its
#    own webhook cert + caBundle at startup — no cert step of your own.
make deploy IMG=<your-registry>/nebula:<tag>
```

No restart is needed — everything the manager consumes exists before it boots.
If you instead created a Secret *after* the manager was already running, restart
it to pick up the change:
`kubectl rollout restart deployment/nebula-controller-manager -n nebula-system`.

A declarative Secret manifest for GitOps workflows lives at
`config/manager/modal-credentials.example.yaml` (not applied by kustomize — copy
and fill it in, or template it with your secret manager).

To tear everything down:

```bash
make undeploy      # remove the manager
make uninstall     # remove CRDs
```

---

## Verifying the deployment

```bash
# Manager is running.
kubectl -n nebula-system get pods

# Providers registered (look for "registered provider" / "skipping … registration").
kubectl -n nebula-system logs deploy/nebula-controller-manager | grep -i provider

# Virtual nodes exist, one per registered provider.
kubectl get nodes -l nebula.inftyai.com/provider

# Kubelet serving CSR is signed (required by control planes that verify kubelet TLS).
kubectl get csr \
  -l app.kubernetes.io/name=nebula,app.kubernetes.io/component=kubelet-serving-certificate

# Webhook TLS is wired: the caBundle matches the serving cert Secret.
diff <(kubectl get secret nebula-webhook-server-cert -n nebula-system -o jsonpath='{.data.tls\.crt}') \
     <(kubectl get mutatingwebhookconfiguration nebula-mutating-webhook-configuration \
         -o jsonpath='{.webhooks[0].clientConfig.caBundle}') \
  && echo "webhook caBundle matches serving cert"
```

A pool referencing an unregistered provider shows it plainly:

```bash
kubectl get nodepools
# NAME       STATUS   STRATEGY   PROVIDERS   AGE
# gpu-pool   False    Ordered    modal,aws   2m

# Inspect the condition reason and message when STATUS is False.
kubectl get nodepool <name> -o jsonpath='{.status.conditions[?(@.type=="Ready")]}'
# Ready=False / UnknownProvider means that provider's creds are missing or wrong.
```

---

## Smoke test

The checks above only prove the manager is healthy. To confirm a provider can
actually launch — **this starts a real GPU instance and costs money**:

```bash
kubectl apply -f config/samples/nodepool.yaml
kubectl apply -f config/samples/sandbox.yaml

kubectl get sandbox sample -w      # Pending → Initializing → Ready
kubectl describe sandbox sample    # events name the provider, region and instance type

kubectl delete -f config/samples/sandbox.yaml   # terminates the instance
```

Both samples are commented with what the fields mean and which accelerator shapes
actually resolve. If the box never reaches Ready, check your provider GPU quota
(on a new AWS account it is often 0, and On-Demand and Spot are separate limits).
