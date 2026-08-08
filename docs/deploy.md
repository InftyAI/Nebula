# Deploying Nebula

This guide covers building and deploying the Nebula manager to a Kubernetes
cluster, including wiring provider credentials.

- [Quick start](#quick-start)
- [How credentials are handled](#how-credentials-are-handled)
- [Webhook TLS (no cert-manager)](#webhook-tls-no-cert-manager)
- [What `deploy-all` does](#what-deploy-all-does)
- [Configuration](#configuration)
- [Manual deployment](#manual-deployment)
- [Verifying the deployment](#verifying-the-deployment)

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

Nebula keeps provider credentials in **one Kubernetes Secret per provider** — not
a single shared secret. This is deliberate and matches the control plane's
"creds-absent → skip that provider, not fatal" model:

- Each provider Secret is referenced by the manager Deployment through an
  **optional** `envFrom.secretRef` (see `config/manager/manager.yaml`). Optional
  means the manager still boots when a Secret is missing.
- A provider whose credentials are absent is **logged and skipped** at
  registration (`registerProviders` in `cmd/main.go`), not fatal. A `NodePool`
  that references an unregistered provider surfaces `Ready=False /
  UnknownProvider`, which self-heals once the creds are applied.
- Splitting per provider isolates rotation and RBAC, and lets you configure one
  provider without touching another's Secret. Providers namespace their env vars
  (`MODAL_TOKEN_ID`, `RUNPOD_API_KEY`, …), so the merged environment never
  collides.

`.env` holds **only secrets**. Non-secret configuration (image, namespace, Kind
cluster) is passed as `make` variables, never committed to `.env`.

> **Secret vs. absent looks the same.** Because `envFrom` is `optional: true`, a
> typo in a Secret name silently yields no credentials — the provider is just
> skipped. The `NodePool` `UnknownProvider` condition is your signal that a
> referenced provider did not register.

---

## Webhook TLS (no cert-manager)

Nebula runs a mutating webhook (it injects the scheduling gate into gated Pods),
and the API server requires TLS to call it. **cert-manager is intentionally not
used**, and neither is any out-of-band setup step: the manager provisions its own
serving certificate in-process at startup (`pkg/cert`, built on
[cert-controller](https://github.com/open-policy-agent/cert-controller)). There is
nothing to install and nothing to run before `make deploy`.

It does the three things that have to agree with each other:

1. **Serving cert** — mints a self-signed cert for the webhook Service DNS name
   (`nebula-webhook-service.<namespace>.svc`), stores it in the
   `webhook-server-cert` Secret, and writes it to
   `/tmp/k8s-webhook-server/serving-certs` where the webhook server reads it.
2. **CA trust** — patches that cert's CA into the `MutatingWebhookConfiguration`
   `caBundle`, so the API server trusts the webhook. It is derived from the cert
   just written, so the served cert and the trusted CA cannot drift.
3. **Renewal** — rotates the cert before it expires. This is the part neither
   cert-manager-free alternative had: a script-minted cert simply expires, years
   later, when nobody remembers a script was involved.

The Secret is the shared source of truth across replicas — a second replica finds
the existing cert there and writes it to its own disk rather than minting a
competing one. Rotation is *not* leader-elected, because webhook serving is not
either: every replica needs the keypair on its own local disk, and the API server
will call a non-leader.

**Startup ordering.** Nothing that depends on Pod admission is registered until the
cert is ready, so the first seconds of a fresh install log
`waiting for the webhook certificate to be ready` and reconcile nothing. That is
deliberate: with `failurePolicy: Fail` a Pod created before the webhook is trusted
would be rejected, and without the gate it would be scheduled by vanilla Kubernetes
— silently bypassing placement and never reaching a provider.

The cert volume is an `emptyDir`, not a Secret projection, because the rotator
*writes* to that path; a Secret volume is read-only and its kubelet refresh would
fight the rotator.

To switch to cert-manager instead, re-add `- ../certmanager` and re-enable the
`CERTMANAGER` replacements blocks in `config/default/kustomization.yaml`, install
cert-manager, and drop the `CertsManager` call from `cmd/main.go`.

---

## What `deploy-all` does

`make deploy-all` runs `hack/deploy.sh`, which is idempotent (safe to re-run):

1. **Builds** the manager image (`make docker-build IMG=…`).
2. **Publishes** it — `docker push`, or `kind load docker-image` when
   `DEPLOY_KIND_CLUSTER` is set.
3. **Creates Secrets first**, so the manager boots already-configured:
   - the **webhook serving cert** Secret (self-signed — see
     [Webhook TLS](#webhook-tls-no-cert-manager));
   - **credential Secrets** from `.env`, one per provider. A Secret is created
     only when *all* its required keys are set, so a partial config never
     produces a half-populated Secret; a provider with blank keys is skipped.
4. **Deploys** CRDs + the manager (`make deploy IMG=…`). The pod mounts the cert
   and reads provider creds on its first and only boot.
5. **Injects the webhook CA bundle** into the `MutatingWebhookConfiguration`
   (server-side; the manager is not touched).

---

## Configuration

`.env` (secrets only, gitignored — see `.env.example`):

| Key | Provider | Required | Notes |
|---|---|---|---|
| `MODAL_TOKEN_ID` | Modal | yes | From `modal token new` |
| `MODAL_TOKEN_SECRET` | Modal | yes | From `modal token new` |
| `AWS_ACCESS_KEY_ID` | AWS | dev only | Prefer IRSA / instance role in production and leave blank — the SDK's default credential chain finds the role. Set only for local/dev. |
| `AWS_SECRET_ACCESS_KEY` | AWS | dev only | Pairs with `AWS_ACCESS_KEY_ID`; both required together or both blank. |

Non-secret config, passed as `make` variables:

| Variable | Default | Meaning |
|---|---|---|
| `IMG` | `controller:latest` | Manager image to build and deploy |
| `NAMESPACE` | `nebula-system` | Namespace the manager runs in |
| `DEPLOY_KIND_CLUSTER` | *(empty)* | If set, load the image into this Kind cluster instead of pushing. Separate from the e2e `KIND_CLUSTER`, so a plain `make deploy-all` pushes. |
| `KUBECTL` | `kubectl` | kubectl binary to use |

---

## Manual deployment

If you don't want the script (e.g. you manage Secrets via sealed-secrets or a
GitOps pipeline), do the same steps by hand. Order matters — create the Secrets
before deploying so the manager boots configured:

```bash
# 1. Namespace.
kubectl create namespace nebula-system --dry-run=client -o yaml | kubectl apply -f -

# 2. Modal credential Secret (before deploy — read as env at pod startup).
kubectl create secret generic nebula-modal-credentials -n nebula-system \
  --from-literal=MODAL_TOKEN_ID=ak-... \
  --from-literal=MODAL_TOKEN_SECRET=as-...

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

# Webhook TLS is wired: the caBundle matches the serving cert Secret.
diff <(kubectl get secret webhook-server-cert -n nebula-system -o jsonpath='{.data.tls\.crt}') \
     <(kubectl get mutatingwebhookconfiguration nebula-mutating-webhook-configuration \
         -o jsonpath='{.webhooks[0].clientConfig.caBundle}') \
  && echo "webhook caBundle matches serving cert"
```

A pool referencing an unregistered provider shows it plainly:

```bash
kubectl get nodepool <name> -o jsonpath='{.status.conditions}'
# Ready=False / UnknownProvider means that provider's creds are missing or wrong.
```
