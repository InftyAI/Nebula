# SandD access channel

Wires [SandD](https://github.com/InftyAI/SandD) into Nebula so an operator or agent
can run commands and open interactive shells **inside a Nebula-provisioned workload
container** — which `kubectl logs`/`exec` cannot do against a Nebula virtual node
(`pkg/vnode/handler.go` returns `NotFound`).

The daemon runs **inside the workload container** (one per container, single-tenant),
so its shells see the user's own env, cwd, filesystem and code. It dials **out** to a
controller over a Tailscale/headscale mesh, so the GPU box needs no inbound access —
it works for instances behind NAT with no public IP, in any region or cloud (the mesh
is cross-cloud; only outbound reachability to the public headscale endpoint is needed).

## Architecture

**Minting keys** — the broker is the only component with headscale admin authority;
it reaches headscale over a local unix socket. There is no static-key path.

```
   headscale pod
   ┌──────────────────────────────────────────────┐
   │  keybroker sidecar  ──unix socket──►  headscale │
   │  (ClusterIP :8090)                             │
   └──────────▲──────────────────▲──────────────────┘
              │ POST /keys        │ POST /keys
              │ ?kind=controller  │ ?kind=daemon
              │ (reusable)        │ (single-use)
         controller          Nebula manager
         (at startup)        (at Provision, per workload → bakes key into user-data)
```

**Using the mesh** — both sides join headscale, then the workload's daemon dials the
controller by its stable MagicDNS name (no inbound access to the box).

```
                 headscale (internet-facing LB :80 → 8080)
                     ▲                        ▲
           joins mesh│                        │joins mesh
   ┌─────────────────┴────────┐       ┌───────┴──────────────────┐
   │ controller               │◄──────│ GPU workload (EC2)        │
   │ Server(tunnel)           │ ws://…│ sandd --tunnel            │
   │ sandd-controller.        │ (mesh)│ dials controller by name  │
   │   nebula.mesh:8765/ws    │       │ DAEMON_ID = NodeClaim     │
   └──────────────────────────┘       └───────────────────────────┘
```

## What's in the overlay

`config/sandd` is applied by `config/default` (the `- ../sandd` line), so `make deploy`
stands up everything except the hand-applied Service and controller:

| File | Kind | Role |
|------|------|------|
| `headscale.yaml` | Deployment + ConfigMap + Service | Tailscale control server + the **`keybroker` sidecar** (mints pre-auth keys on demand over a local unix socket; in-cluster-only `nebula-keybroker` ClusterIP; the ONLY way keys are minted). |
| `manager-config.yaml` | ConfigMap `nebula-sandd-config` | **Required** — the manager mounts it as `envFrom` with `optional: false`, so it won't start without it. `SANDD_KEYBROKER_URL` is the injection switch: set, the manager mints a fresh per-daemon key from the broker; blank, injection is off (but keep the ConfigMap — blank the value, don't delete it). |

Hand-applied (not in the overlay):

| File | Kind | Role |
|------|------|------|
| `config/samples/headscale-service.yaml` | Service (internet-facing NLB, `:80` → `8080`) | `scheme=internet-facing` — publicly reachable so GPU boxes in **any region or cloud** can join the mesh (they share no private network with the cluster). Listens on **80** (Tailscale's Noise handshake only dials 80/443), so `server_url` is a bare `http://<host>`. Its AWS-assigned ELB hostname is read back after it provisions (step 1). Kept out of the overlay so its `nebula-` prefix doesn't spawn a second LB. gRPC admin (`:50443`) is NOT exposed. The control handshake is end-to-end encrypted and keys never traverse it, but see "Production notes" for TLS/hardening. |
| `config/samples/sandd-controller.yaml` | Deployment `sandd-controller` | The box you drive (`server.exec` / `new_session`). A SandD `Server()` in tunnel mode; sets `SANDD_TUNNEL_SERVER` + `SANDD_KEYBROKER_URL` inline and mints its own key. Adapt the inline script to your own logic. |

## Deploy

**Prereqs:** a running Nebula control plane with the AWS provider configured
(`nebula-aws-credentials` + a NodePool referencing `aws`); GPU instances with outbound
internet access (they dial the public headscale endpoint — no inbound access or shared
VPC needed, which is why the mesh spans regions and clouds); and the AWS Load Balancer
Controller installed (it provisions the internet-facing NLB in step 1).

### 0. Build the key-broker image

```bash
# Override KEYBROKER_IMG for your own registry; then update the sidecar image: in
# config/sandd/headscale.yaml to match.
make docker-build-keybroker docker-push-keybroker
```

### 1. Apply the Service, read its hostname into `.env`, deploy

headscale forces every client to dial the identical `server_url`, and the GPU boxes
live in other regions/clouds with no private path to the cluster — so the endpoint
must be publicly routable. That address is the internet-facing NLB's own AWS-assigned
hostname, which is only known once the LB provisions. So apply the Service first and
read it back:

```bash
kubectl apply -f config/samples/headscale-service.yaml

# Poll until populated (NLB takes 1–3 min):
HS=$(kubectl -n nebula-system get svc nebula-headscale \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}'); echo "$HS"

# Sanity-check it resolves to PUBLIC IPs (proves scheme=internet-facing took effect):
nslookup "$HS"   # expect public IPs, NOT 10.x / 172.16–31.x / 192.168.x
```

Put `http://$HS` (no port) into **`.env`** as `SANDD_TUNNEL_SERVER` — that one line is
the single source of truth for the endpoint:

```bash
echo "SANDD_TUNNEL_SERVER=http://$HS" >> .env   # or edit .env by hand

make deploy-all   # substitutes the hostname into every spot, then deploys
```

`make deploy-all` reads `SANDD_TUNNEL_SERVER` from `.env` and substitutes the
`__SANDD_TUNNEL_SERVER__` token wherever the endpoint must appear — `server_url` in
`config/sandd/headscale.yaml` and `SANDD_TUNNEL_SERVER` in
`config/sandd/manager-config.yaml` — so both render **byte-identical** (exactly what
headscale demands) on the first apply, with no manager or headscale restart. The URL
carries no port because the Service listens on **80**: Tailscale's control (Noise)
handshake only ever dials 80 or 443, never a custom port, so headscale must be on a
default port even though it listens on 8080 in-pod (the Service maps 80 → 8080).

> The ELB hostname is stable for the life of the Service; if you delete and recreate
> the Service, just re-read it into `.env` and re-run `make deploy-all` — no manifest
> edits. (The controller in step 2 is hand-applied, so substitute the same value into
> it too — one `sed`, shown there.)

The broker mints keys under one headscale user (`SANDD_KEYBROKER_USER`, default
`nebula`) and **creates that user itself on startup** if it's missing — no manual
bootstrap. Key policies it owns (see `cmd/keybroker`): daemon keys are **single-use**
(each workload gets its own throwaway credential); the controller key is **reusable**
(re-registers across restarts). Both are **ephemeral**, so headscale auto-reaps a node
shortly after it disconnects — torn-down experiments don't pile up as OFFLINE nodes
squatting MagicDNS names.

### 2. Deploy the controller

Self-contained — it reads `SANDD_TUNNEL_SERVER` + `SANDD_KEYBROKER_URL` from inline env
and mints its own key at startup. It's hand-applied (not in the overlay `make deploy`
substitutes), so its `SANDD_TUNNEL_SERVER` also carries the `__SANDD_TUNNEL_SERVER__`
token — substitute the same value from `.env` at apply time:

```bash
# Pull SANDD_TUNNEL_SERVER out of .env and substitute the token as we apply.
HS=$(grep -E '^SANDD_TUNNEL_SERVER=' .env | cut -d= -f2-)
sed "s|__SANDD_TUNNEL_SERVER__|${HS}|g" config/samples/sandd-controller.yaml | kubectl apply -f -

# Confirm it joined (any 100.64.x.x — we address it by name, not IP):
CTRL=$(kubectl -n nebula-system get pod -l app=sandd-controller -o name | head -1)
kubectl -n nebula-system exec "$CTRL" -c controller -- tailscale ip -4
```

It pins `hostname: sandd-controller` (stable MagicDNS name) and pairs a PVC on
`/var/lib/tailscale` with `strategy: Recreate`, so a restart reclaims the *same*
node/name instead of getting a `-suffix` (which would strand daemons at
`Active daemons: 0`). Needs a default StorageClass — check `kubectl get sc`.

That's it — the manager read `nebula-sandd-config` at startup (step 1 applied it
before the manager pod started), so injection is already on. From now on **every AWS
workload** runs the daemon inside its container, addressed by `DAEMON_ID` = the
NodeClaim name, each with its own broker-minted key. (If you later *change* the config
on a running manager, restart it — `kubectl -n nebula-system rollout restart
deploy/nebula-controller-manager` — since it reads env only at startup.)

## Using it

Apply any Nebula workload — injection is a cluster decision, so the ordinary sample
works unchanged:

```bash
# The workload places against a NodePool, so that must exist first. Apply both
# together (deployment.yaml targets the `sample` NodePool):
kubectl apply -f config/samples/nebula_v1alpha1_nodepool.yaml
kubectl apply -f config/samples/deployment.yaml   # or your own workload
```

Then drive it from the controller:

```bash
CTRL=$(kubectl -n nebula-system get pod -l app=sandd-controller -o name | head -1)
kubectl -n nebula-system exec -it "$CTRL" -c controller -- python3 - <<'PY'
from sandd import Server, TunnelConfig
import os, json, urllib.request
# Mint a key from the broker, same as the controller does at startup.
req = urllib.request.Request(
    os.environ["SANDD_KEYBROKER_URL"].rstrip("/") + "/keys?kind=controller", method="POST")
with urllib.request.urlopen(req, timeout=10) as r:
    authkey = json.load(r)["key"]
server = Server(connect="tunnel", tunnel_config=TunnelConfig(
    authkey=authkey, server=os.environ["SANDD_TUNNEL_SERVER"]))
for d in server.list_daemons():
    print("daemon:", d.id)
    print(server.exec(d.id, "nvidia-smi --query-gpu=name --format=csv,noheader").stdout)
    print(server.exec(d.id, "ls /").stdout)  # the USER's filesystem
PY
```

`d.id` is the NodeClaim name; `server.exec` runs in the workload container. For an
interactive shell use `server.new_session(d.id)` (PTY).

## How injection works

The AWS adapter injects SandD in two parts (`pkg/provider/aws/translate.go`), both
**fail-open** so they can never block the workload:

1. **On the host, before `docker run`:** the user-data fetches the static (musl)
   `sandd` binary + Tailscale bundle into `/opt/sandd`. The GPU AMI has `curl`+`tar`,
   so the user's image needs nothing.
2. **`docker run`** bind-mounts `/opt/sandd` **read-only** and overrides the ENTRYPOINT
   with a `/bin/sh` shim (`sanddShimScript`) that: puts `/opt/sandd` on `PATH`; starts
   `sandd --tunnel` backgrounded (tailscaled in `--tun=userspace-networking` — no
   `NET_ADMIN`, no `/dev/net/tun`); then `exec`s the user's command as PID 1.

Fetching on the host means no fetcher/package manager in the user's image, one download
per instance, and a safe shared read-only mount (each container keeps its own writable
`/var/lib/tailscale` + `/tmp`).

`SANDD_TUNNEL_AUTHKEY` arrives as container env — the per-workload key the manager
minted (single-use + ephemeral), so even though the container can see it, it's a
throwaway good for one node and reaped on disconnect. The daemon is single-tenant, so a
container seeing its own key is fine.

Both parts log to stderr with a `[sandd]` prefix, landing in the **EC2 console**
(`aws ec2 get-console-output --instance-id <id> --latest`) — the place to debug
bring-up. The daemon's own log is at `/tmp/sandd.log` inside the container.

**Image requirements:** the shim needs only `/bin/sh` (binaries are mounted, not
fetched — even distroless-with-shell works); the **host** needs outbound network. A Pod
relying solely on its image's baked-in ENTRYPOINT (no `command`/`args`) can't be
reconstructed — set an explicit `command`.

## Turning it off

- **Stop injecting** (keep infra): blank `SANDD_KEYBROKER_URL` in `manager-config.yaml`
  and restart the manager (empty broker URL => nil minter => nothing injected). Do NOT
  delete the ConfigMap — it is a required mount (`optional: false`), so removing it
  leaves the manager stuck in `CreateContainerConfigError`.
- **Remove the controller:** `kubectl delete -f config/samples/sandd-controller.yaml`.
- **Remove all infra:** comment out `- ../sandd` in
  `config/default/kustomization.yaml` and re-apply — but keep `manager-config.yaml`
  applied (with `SANDD_KEYBROKER_URL` blank) so the required mount is still satisfied.

## Production notes

This is a working DEV setup, not hardened: headscale uses SQLite on an `emptyDir` (its
state — and every issued key — is lost on pod restart) over plain HTTP. Each workload
already gets its own single-use ephemeral key, but for real use also: back headscale
with a PersistentVolume + real database + TLS, and add per-tenant **tags/ACLs** to
minted keys so daemons are mesh-isolated, not just credential-distinct. (headscale's
`emptyDir` is separate from the controller's PVC in step 2, which persists the
*controller's* identity, not headscale's state.) See https://headscale.net.
