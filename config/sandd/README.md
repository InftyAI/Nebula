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
| `headscale.yaml` | Deployment + Service (LoadBalancer) + ConfigMap | Tailscale control server. Assigns mesh IPs. Exposed to the internet so out-of-cluster GPU boxes can join. |

Everything else you supply by hand — the **controller is user-facing** (you drive
it with your own `exec`/session logic), so it ships as an example to adapt rather
than a fixed deployment resource:

| File | Kind | Role |
|------|------|------|
| `config/samples/sandd-controller.yaml` | Deployment `sandd-controller` | A SandD `Server()` in tunnel mode. Joins the mesh (reachable at `sandd-controller.nebula.mesh` via MagicDNS), listens on `:8765`, and is the box you drive. Replace the inline script with your own controller logic. |
| `config/samples/sandd-config.yaml` | Secret `nebula-sandd-config` | **Flips SandD on for the manager.** When present, the manager injects the daemon into every AWS workload it provisions. Absent ⇒ nothing injected. |
| — | Secret `sandd-controller-auth` | The controller's own auth key + headscale URL. |

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

### 1. Get the headscale address and fix `server_url`

```bash
kubectl -n nebula-system get svc nebula-headscale \
  -o jsonpath='{.status.loadBalancer.ingress[0].hostname}{"\n"}'
# e.g. a1b2c3.elb.us-east-1.amazonaws.com
export HEADSCALE_URL="http://a1b2c3.elb.us-east-1.amazonaws.com:8080"
```

headscale rejects a client whose `--login-server` disagrees with its configured
`server_url`. Edit `headscale.yaml`'s `server_url` to the resolved address (it
ships as `REPLACE_WITH_LOADBALANCER_ADDRESS`), re-apply, and restart:

```bash
kubectl -n nebula-system rollout restart deploy/nebula-headscale
```

### 2. Mint a mesh user + reusable pre-auth key

```bash
POD=$(kubectl -n nebula-system get pod -l app=headscale -o name | head -1)
kubectl -n nebula-system exec "$POD" -- headscale users create nebula

# REUSABLE: the controller and every GPU daemon join with this one key. For
# production, mint a per-daemon key instead (see the SandD tunnel guide).
AUTHKEY=$(kubectl -n nebula-system exec "$POD" -- \
  headscale preauthkeys create --user nebula --reusable --expiration 720h)
echo "authkey: $AUTHKEY"
```

### 3. Deploy a controller with its auth (Secret `sandd-controller-auth`)

```bash
kubectl -n nebula-system create secret generic sandd-controller-auth \
  --from-literal=SANDD_TUNNEL_AUTHKEY="$AUTHKEY" \
  --from-literal=SANDD_TUNNEL_SERVER="$HEADSCALE_URL"

# The controller is a SAMPLE you adapt (swap the inline script for your own
# exec/session logic), so it is applied by hand, not by `make deploy`. It pins
# hostname: sandd-controller, so headscale's MagicDNS makes it reachable at
# sandd-controller.nebula.mesh regardless of which mesh IP it gets.
kubectl apply -f config/samples/sandd-controller.yaml

# Confirm it joined the mesh (any 100.64.x.x is fine — we address it by name):
CTRL=$(kubectl -n nebula-system get pod -l app=sandd-controller -o name | head -1)
kubectl -n nebula-system exec "$CTRL" -c controller -- tailscale ip -4
```

### 4. Turn on injection (Secret `nebula-sandd-config`)

```bash
kubectl -n nebula-system create secret generic nebula-sandd-config \
  --from-literal=SANDD_TUNNEL_AUTHKEY="$AUTHKEY" \
  --from-literal=SANDD_TUNNEL_SERVER="$HEADSCALE_URL" \
  --from-literal=SANDD_SERVER_URL="ws://sandd-controller.nebula.mesh:8765/ws"

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

On an opted-in cluster, Nebula's AWS adapter overrides the container ENTRYPOINT
with a tiny `/bin/sh` shim (`sanddShimScript` in `pkg/provider/aws/translate.go`),
**fail-open** so it can never block the workload:

1. fetches the static (musl) `sandd` binary and the static Tailscale bundle into
   `/tmp` and puts them on `PATH`;
2. starts `sandd --tunnel` backgrounded — which brings up `tailscaled` in
   `--tun=userspace-networking` mode (no `NET_ADMIN`, no `/dev/net/tun`), joins
   the mesh, and dials `SANDD_SERVER_URL`;
3. `exec`s the user's real command, so the workload is PID 1.

Config arrives as container env (`DAEMON_ID`, `SERVER_URL`, `SANDD_TUNNEL_*`,
`SANDD_BINARY_URL`, `TAILSCALE_TARBALL_URL`). Because the daemon runs inside the
container, the auth key is visible to that container — acceptable because the
daemon is single-tenant (one per container).

**Image requirements:** the shim needs `/bin/sh` and `curl` or `wget`, plus
outbound network. Stock `nvidia/cuda`, `ubuntu`, `debian`, `python`, etc. qualify;
distroless/scratch do **not**. A Pod that relies solely on its image's baked-in
ENTRYPOINT (sets neither `command` nor `args`) cannot be reconstructed — set an
explicit `command`.

## Turning it off

- **Stop injecting** (keep the infra): delete `nebula-sandd-config` and restart
  the manager. New workloads launch with the plain bootstrap.
- **Remove the controller:** `kubectl delete -f config/samples/sandd-controller.yaml`.
- **Remove the headscale infra:** comment out `- ../sandd` in
  `config/default/kustomization.yaml` and re-apply.

## Production notes

This overlay is a working DEV setup, not hardened: headscale uses SQLite on an
`emptyDir` (state — and every issued auth key — is lost on restart) over plain
HTTP, and one reusable auth key is shared. For real use, back headscale with a
PersistentVolume + a real database + TLS, and mint a per-daemon key. See
https://headscale.net and the SandD tunnel guide.
