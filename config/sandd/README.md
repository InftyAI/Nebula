# SandD access channel

Wires [SandD](https://github.com/InftyAI/SandD) into Nebula so an operator or
agent can run commands and open interactive shells **inside a Nebula-provisioned
workload container** — even though `kubectl logs` and `kubectl exec` do **not**
work against a Nebula virtual node (`pkg/vnode/handler.go` returns `NotFound` for
`GetContainerLogs` / `RunInContainer`).

The daemon runs **inside the workload container** (single-tenant, one per
container), so its shells see the user's own env, cwd, filesystem and code. It
dials **out** to a controller over a Tailscale/headscale mesh, so the GPU box
needs no inbound access — this works for instances in a private VPC with no
public IP or open port.

## What's in this overlay

`config/sandd` is deployed by `config/default` (the `- ../sandd` line), so a
normal `make deploy` stands up the shared **infrastructure**:

| File | Kind | Role |
|------|------|------|
| `headscale.yaml` | Deployment + ConfigMap | Tailscale control server. Coordinates the mesh. |

The headscale **Service is separate and applied first**, because its LoadBalancer
address must be known before headscale starts (that address is `server_url`, which
headscale validates clients against). It therefore lives in `config/samples/`
(hand-applied), NOT in this overlay — everything in `config/sandd/` is applied
together by `make deploy`, but the Service has to go up on its own, first:

| File | Kind | Role |
|------|------|------|
| `config/samples/headscale-service.yaml` | Service (LoadBalancer, `:8080` only) | Exposes headscale to the internet so out-of-cluster GPU boxes can join. Apply FIRST, read its ELB hostname, put it in `server_url`, then `make deploy`. gRPC admin (`:50443`) is deliberately NOT exposed. |

Everything else you supply by hand — the **controller is user-facing** (you drive
it with your own `exec`/session logic), so it ships as an example to adapt rather
than a fixed deployment resource:

| File | Kind | Role |
|------|------|------|
| `config/samples/sandd-controller.yaml` | Deployment `sandd-controller` | A SandD `Server()` in tunnel mode. Joins the mesh (reachable at `sandd-controller.nebula.mesh` via MagicDNS), listens on `:8765`, and is the box you drive. Replace the inline script with your own controller logic. |
| `config/samples/sandd-config.yaml` | Secret `nebula-sandd-config` | **Flips SandD on for the manager.** When present, the manager injects the daemon into every AWS workload it provisions. Absent ⇒ nothing injected. |
| `config/samples/sandd-controller-auth.yaml` | Secret `sandd-controller-auth` | The controller's own auth key + headscale URL. |

```
                         ┌───────────────────────────────┐
   (public internet)     │  headscale  (control server)  │
        ┌───────────────►│  Service type=LoadBalancer    │◄───────────┐
        │                └───────────────────────────────┘            │
        │ joins mesh                                       joins mesh  │
        │                                                              │
┌────────────────────┐                                   ┌───────────┴──────────┐
│  controller        │  ws://sandd-controller.nebula.mesh │  GPU workload (EC2)  │
│  Server(tunnel)    │◄──────────:8765/ws (over mesh)──────│  sandd --tunnel      │
│  MagicDNS hostname │        daemon dials the controller  │  DAEMON_ID = claim   │
└────────────────────┘                                   └──────────────────────┘
```

## Enabling it (deploy a controller + two secrets)

headscale deploys by default, but SandD injection stays **off** until the
`nebula-sandd-config` Secret exists — so a cluster that never finishes these steps
just runs an idle headscale and injects nothing.

> **Prereqs:** a running Nebula control plane with the AWS provider configured
> (`nebula-aws-credentials` + a NodePool referencing `aws`), and a cluster that
> can expose headscale to the internet (this overlay uses `Service
> type=LoadBalancer`; substitute a NodePort + DNS or an Ingress if you have no LB
> provider — a purely in-cluster ClusterIP will NOT work, the GPU boxes are
> outside the cluster).

### 1. Create the Service first, then set `server_url`, then deploy

The LoadBalancer address must go into headscale's `server_url` (headscale rejects a
client whose `--login-server` disagrees with it). AWS assigns the ELB hostname as
soon as the Service exists — before any headscale pod — so apply the Service on its
own, read the hostname, bake it in, and only then deploy. headscale then starts
already-correct, with **no restart**.

```bash
# a) create just the Service (namespace must exist — `make deploy` creates it, or
#    `kubectl create ns nebula-system`).
kubectl apply -f config/samples/headscale-service.yaml

# b) wait for the ELB hostname and capture it.
kubectl -n nebula-system get svc nebula-headscale \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{"\n"}'
# e.g. a1b2c3.elb.us-east-1.amazonaws.com
export HEADSCALE_URL="http://a1b2c3.elb.us-east-1.amazonaws.com:8080"

# c) put that address into server_url in config/sandd/headscale.yaml (it ships as
#    REPLACE_WITH_LOADBALANCER_ADDRESS), then deploy — headscale reads it at boot.
make deploy-all
```

### 2. Mint a mesh user + reusable, ephemeral pre-auth key(s)

Mint keys **reusable + ephemeral**. *Reusable* lets a node re-register after a
restart (and lets many daemons share one key). *Ephemeral* makes headscale
**auto-delete a node shortly after it disconnects**, so torn-down experiments and
dead workload pods don't pile up as OFFLINE nodes that hold onto MagicDNS names —
the failure that produced `Active daemons: 0` (a stale offline `sandd-controller`
node kept the clean name, so the live pod got a `-suffix` daemons never dialed).

```bash
POD=$(kubectl -n nebula-system get pod -l app=headscale -o name | head -1)
kubectl -n nebula-system exec "$POD" -- headscale users create nebula

# One reusable+ephemeral key works for both the controller and every GPU daemon.
AUTHKEY=$(kubectl -n nebula-system exec "$POD" -- \
  headscale preauthkeys create --user nebula --reusable --ephemeral --expiration 720h)
echo "authkey: $AUTHKEY"
```

Ephemeral reaps a node on *disconnect*; the controller additionally needs its
identity to survive **restarts while an experiment is running**, so its manifest
mounts a PVC on `/var/lib/tailscale` (see step 3). Daemons need no such thing —
they dial *out* and are addressed by `DAEMON_ID`, not by a mesh name, so a fresh
node per pod is fine. For real multi-tenant isolation, mint a per-daemon key with
tags instead of sharing one (see the SandD tunnel guide).

### 3. Deploy a controller with its auth (Secret `sandd-controller-auth`)

Edit `config/samples/sandd-controller-auth.yaml`: set `SANDD_TUNNEL_AUTHKEY` to
the `$AUTHKEY` from step 2 and `SANDD_TUNNEL_SERVER` to your headscale URL
(`$HEADSCALE_URL`). Then apply it:

```bash
kubectl apply -f config/samples/sandd-controller-auth.yaml

# The controller is a SAMPLE you adapt (swap the inline script for your own
# exec/session logic), so it is applied by hand, not by `make deploy`. It pins
# hostname: sandd-controller, so headscale's MagicDNS makes it reachable at
# sandd-controller.nebula.mesh regardless of which mesh IP it gets. It also
# declares a PVC (sandd-controller-tailscale) mounted at /var/lib/tailscale and
# uses strategy: Recreate, so the tailscale node key persists across restarts and
# the pod reclaims the SAME node/name instead of getting a -suffix. Needs a
# default StorageClass (EBS CSI on EKS) — check with `kubectl get sc`; without one
# the PVC stays Pending.
kubectl apply -f config/samples/sandd-controller.yaml

# Confirm it joined the mesh (any 100.64.x.x is fine — we address it by name):
CTRL=$(kubectl -n nebula-system get pod -l app=sandd-controller -o name | head -1)
kubectl -n nebula-system exec "$CTRL" -c controller -- tailscale ip -4
```

### 4. Turn on injection (Secret `nebula-sandd-config`)

Same pattern — edit `config/samples/sandd-config.yaml`, which carries the same two
values plus `SANDD_SERVER_URL` (the mesh address workloads dial the controller at;
already set to the MagicDNS name, leave it as-is). Set the authkey and headscale
URL, then apply:

```bash
kubectl apply -f config/samples/sandd-config.yaml

# The manager reads these only at startup, so restart it to pick them up.
kubectl -n nebula-system rollout restart deploy/nebula-controller-manager
```

From now on **every AWS workload Nebula provisions** runs the daemon inside its
container, addressed by `DAEMON_ID` = the NodeClaim name.

## Using it

Apply any Nebula workload — the ordinary sample works unchanged, because
injection is a cluster decision, not a per-workload one:

```bash
kubectl apply -f config/samples/deployment.yaml   # or your own Nebula workload
```

Then, from the controller, list daemons and run commands in the workload's own
environment:

```bash
CTRL=$(kubectl -n nebula-system get pod -l app=sandd-controller -o name | head -1)
kubectl -n nebula-system exec -it "$CTRL" -c controller -- python3 - <<'PY'
from sandd import Server, TunnelConfig
import os
server = Server(connect="tunnel", tunnel_config=TunnelConfig(
    authkey=os.environ["SANDD_TUNNEL_AUTHKEY"],
    server=os.environ["SANDD_TUNNEL_SERVER"],
))
for d in server.list_daemons():
    print("daemon:", d.id)
    print(server.exec(d.id, "nvidia-smi --query-gpu=name --format=csv,noheader").stdout)
    print(server.exec(d.id, "ls /").stdout)  # the USER's filesystem
PY
```

`d.id` is the NodeClaim name; `server.exec` runs in the workload container. For an
interactive shell use `server.new_session(d.id)` (PTY).

## How injection works

On an opted-in cluster, Nebula's AWS adapter injects SandD in two parts
(`pkg/provider/aws/translate.go`), both **fail-open** so they can never block the
workload:

1. **On the host, before `docker run`:** the user-data fetches the static (musl)
   `sandd` binary and the static Tailscale bundle into `/opt/sandd`. The GPU AMI
   (Amazon Linux 2) has `curl`+`tar`, so this needs nothing from the user's image.
2. **`docker run`** bind-mounts `/opt/sandd` **read-only** into the container and
   overrides the ENTRYPOINT with a tiny `/bin/sh` shim (`sanddShimScript`) that:
   - puts `/opt/sandd` on `PATH` (no fetch, no package install in the container);
   - starts `sandd --tunnel` backgrounded — which brings up `tailscaled` in
     `--tun=userspace-networking` mode (no `NET_ADMIN`, no `/dev/net/tun`), joins
     the mesh, and dials `SANDD_SERVER_URL`;
   - `exec`s the user's real command, so the workload is PID 1.

Fetching on the host (not in the container) means the user's image needs no
fetcher or package manager, the download happens **once per instance**, and the
read-only mount is safe to share (the binaries are immutable; each container keeps
its own writable `/var/lib/tailscale` + `/tmp`).

Config arrives as container env (`DAEMON_ID`, `SERVER_URL`, `SANDD_TUNNEL_*`).
Because the daemon runs inside the container, the auth key is visible to that
container — acceptable because the daemon is single-tenant (one per container).

Both parts log to stderr with a `[sandd]` prefix, landing in the **EC2 instance
console** (`aws ec2 get-console-output --instance-id <id> --latest`) — the place to
debug bring-up, because `kubectl logs`/`exec` do not work against a Nebula virtual
node. The daemon's own log stays at `/tmp/sandd.log` inside the container (reachable
via SandD once the mesh is up).

**Image requirements:** the shim needs only `/bin/sh` in the image (binaries are
mounted, not fetched, so no `curl`/`wget`/package manager is required — even
distroless with a shell works). The **host** needs outbound network to fetch the
binaries. A Pod that relies solely on its image's baked-in ENTRYPOINT (sets neither
`command` nor `args`) cannot be reconstructed — set an explicit `command`.

## Turning it off

- **Stop injecting** (keep the infra): delete `nebula-sandd-config` and restart
  the manager. New workloads launch with the plain bootstrap.
- **Remove the controller:** `kubectl delete -f config/samples/sandd-controller.yaml`.
- **Remove the headscale infra:** comment out `- ../sandd` in
  `config/default/kustomization.yaml` and re-apply.

## Production notes

This overlay is a working DEV setup, not hardened: headscale itself uses SQLite on
an `emptyDir` (its OWN state — and every issued auth key — is lost if the headscale
pod restarts) over plain HTTP, and a single reusable+ephemeral key is shared by the
controller and all daemons. For real use, back headscale with a PersistentVolume +
a real database + TLS, and mint a per-daemon key with tags for tenant isolation
(one shared key means every workload authenticates with the same credential). Note
this is separate from the controller's own PVC (step 3), which persists the
*controller's* tailscale identity, not headscale's server state. See
https://headscale.net and the SandD tunnel guide.
