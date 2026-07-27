# Deploying Nebula

This guide covers building and deploying the Nebula manager to a Kubernetes
cluster, including wiring provider credentials.

- [Quick start](#quick-start)
- [How credentials are handled](#how-credentials-are-handled)
- [Webhook TLS (no cert-manager)](#webhook-tls-no-cert-manager)
- [What `deploy-all` does](#what-deploy-all-does)
- [Configuration](#configuration)
- [Adding a provider](#adding-a-provider)
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
used** — there is no cert-manager prerequisite to install. Instead,
`hack/gen-webhook-cert.sh` provisions a self-signed certificate. It does the two
things cert-manager would otherwise automate:

1. **Serving cert** — generates a self-signed cert for the webhook Service DNS
   name (`nebula-webhook-service.nebula-system.svc`) and stores it in the
   `webhook-server-cert` TLS Secret the manager mounts.
2. **CA trust** — injects that cert's CA into the `MutatingWebhookConfiguration`
   `caBundle`, so the API server trusts the webhook. The caBundle is always read
   back *from the Secret*, so the served cert and the trusted CA cannot drift.

**No manager restart is involved.** `deploy-all` orders things so the manager
boots already-correct: the cert Secret is created *before* `make deploy` (it is a
required volume mount, and a running pod would otherwise need remounting), while
the caBundle patch happens *after* (it edits only the webhook config, which only
the API server reads — the manager never sees it).

Re-running is safe: an existing cert Secret is kept as-is, and the caBundle is
re-derived to match.

> **No auto-rotation.** This is the one thing cert-manager gives you that the
> self-signed cert does not. The cert is valid 10 years (`CERT_DAYS`, default
> `3650`). To rotate/renew, regenerate the cert and re-inject the CA:
> ```bash
> FORCE_REGEN=true hack/gen-webhook-cert.sh secret
> hack/gen-webhook-cert.sh cabundle
> kubectl rollout restart deployment/nebula-controller-manager -n nebula-system
> ```
> The restart here is only because you are *replacing* the cert under a
> already-running manager — the initial deploy needs none.

To switch back to cert-manager, re-add `- ../certmanager` and re-enable the
`CERTMANAGER` replacements blocks in `config/default/kustomization.yaml`, install
cert-manager, and drop the `gen-webhook-cert.sh` calls from `hack/deploy.sh`.

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

Non-secret config, passed as `make` variables:

| Variable | Default | Meaning |
|---|---|---|
| `IMG` | `controller:latest` | Manager image to build and deploy |
| `NAMESPACE` | `nebula-system` | Namespace the manager runs in |
| `DEPLOY_KIND_CLUSTER` | *(empty)* | If set, load the image into this Kind cluster instead of pushing. Separate from the e2e `KIND_CLUSTER`, so a plain `make deploy-all` pushes. |
| `KUBECTL` | `kubectl` | kubectl binary to use |

---

## Adding a provider

When a new provider adapter lands, wire its credentials in two places:

1. **`hack/deploy.sh`** — add a row to the `PROVIDER_SECRETS` table:

   ```bash
   PROVIDER_SECRETS=(
     "nebula-modal-credentials|MODAL_TOKEN_ID MODAL_TOKEN_SECRET"
     "nebula-runpod-credentials|RUNPOD_API_KEY|"
   )
   ```
   Format: `<secret-name>|<REQUIRED_KEYS>|<OPTIONAL_KEYS>`.

2. **`config/manager/manager.yaml`** — add an optional `secretRef` under
   `envFrom`:

   ```yaml
   - secretRef:
       name: nebula-runpod-credentials
       optional: true
   ```

Then add the new keys to `.env.example`. Nothing else changes — the script
creates the Secret from `.env` and skips it if the keys are blank.

---

## Manual deployment

If you don't want the script (e.g. you manage Secrets via sealed-secrets or a
GitOps pipeline), do the same steps by hand. Order matters — create the Secrets
before deploying so the manager boots configured, and inject the CA after:

```bash
# 1. Namespace + webhook serving cert Secret (before deploy — it's a volume mount).
kubectl create namespace nebula-system --dry-run=client -o yaml | kubectl apply -f -
hack/gen-webhook-cert.sh secret

# 2. Modal credential Secret (before deploy — read as env at pod startup).
kubectl create secret generic nebula-modal-credentials -n nebula-system \
  --from-literal=MODAL_TOKEN_ID=ak-... \
  --from-literal=MODAL_TOKEN_SECRET=as-...

# 3. Deploy CRDs + manager. The pod mounts the cert and reads creds on first boot.
make deploy IMG=<your-registry>/nebula:<tag>

# 4. Inject the webhook CA (server-side; needs the webhook config to exist).
hack/gen-webhook-cert.sh cabundle
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
