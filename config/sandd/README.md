# SandD — reaching inside a Nebula workload

A Nebula workload is a VM on someone else's cloud, and the API server has no route to it —
so nothing that reaches *into* a workload works by default. SandD is the channel that fills
that gap, and with it installed `kubectl exec` works normally. (`kubectl logs` still does
not; only `exec` is served.)

A **daemon** runs inside the workload container and dials **out** to the Nebula manager
over WebSocket, authenticating with a JWT the manager minted for it. Because the daemon
dials out, the GPU box needs no inbound access, no public IP, and no firewall change.

The controller the daemon dials runs **inside the manager process** — it is Rust, embedded
via cgo (`setupSandD` in `cmd/main.go`). That is not an optimization: a daemon's connection
is a live socket owned by whichever process accepted it, so hosting it in the manager is
what lets the virtual kubelet reach back into a workload for `kubectl exec` directly,
instead of asking a second process to relay.

This folder is the cluster plumbing in front of that port. It is **opt-in** and not part of
`config/default`.

## Shape

```
 NeoCloud VM                      your cluster
┌──────────────┐                 ┌────────────────────────────────────────┐
│ workload     │                 │                                        │
│  └─ sandd ───┼── wss ────────► │ edge (Ingress/LB) ──► nebula-controller │
│     (daemon) │   + JWT         │ sandd.example.com     -manager :8765    │
└──────────────┘                 │                       (embedded SandD  │
                                 │                        controller)     │
                                 └────────────────────────────────────────┘
```

One controller per cluster — **not** one per workload. Its registry is keyed by daemon id,
which is already unique cluster-wide, so partitioning by workload would cost a Deployment,
a Service and a routing object per workload and buy only blast-radius isolation.

That id is the NodeClaim name, and it reaches the controller only as the token's `sub` — an
authenticated daemon is registered under the identity the JWT proves, whatever id it claims
for itself. So the token is the whole of a daemon's identity: nothing else on the instance
names it, and a workload cannot register as someone else's daemon to receive their exec
traffic.

Two pieces have to agree:

| Piece | What it needs |
|---|---|
| manager | the signing key + its own **external** address (it verifies with the public half it derives from that key) |
| edge | a public DNS name, a TLS cert, WebSocket upgrade, and a long idle timeout |

## Enable

```bash
# 1. Key: the private-key Secret the manager mounts. No public half to distribute —
#    the manager derives it.
hack/gen-signing-key.sh

# 2. The edge. Early on purpose: it is admitted with no backend (503 until the
#    manager's Service has endpoints, then picked up automatically), so DNS, LB and
#    cert issuance overlap with the install rather than following it.
$EDITOR config/sandd/ingress.yaml            # replace both sandd.example.com lines
kubectl apply -n nebula-system -f config/sandd/ingress.yaml

# 3. Turn it on: uncomment `- ../sandd` in config/default/kustomization.yaml (the
#    Service), plus SANDD_SIGNING_KEY_PATH and SANDD_EXTERNAL_HOST in
#    config/manager/manager.yaml. Then deploy both together:
make deploy
```

Go through `config/default`, **not** `kubectl apply -k config/sandd`: applied on its own
this folder renders the Service unprefixed as `sandd-controller`, while the Ingress routes
to the prefixed `nebula-sandd-controller` — the symptom is a 502 at the edge.

**The key must come first.** Step 3 is the switch, and with it on the manager *exits at
startup* if the key or the external host is missing. That is deliberate: an instance
provisioned without a working dial-in bills indefinitely for something you cannot reach, so
failing loudly at startup is the cheaper failure. The edge's position is a convenience
rather than a dependency — it works applied at any point, but applied early its slow parts
finish while the image builds.

`SANDD_EXTERNAL_HOST` must be the **same host** as the Ingress. It cannot be derived: the
manager's in-cluster Service name is not resolvable or routable from a NeoCloud instance, so
nothing the manager could invent would be an address a daemon can dial.

## Verify

```bash
# The controller is up and holds the daemons you expect.
kubectl -n nebula-system port-forward svc/nebula-sandd-controller 8765:8765
curl -s localhost:8765/stats | jq

# The edge upgrades to WebSocket and the token is required.
curl -i https://sandd.example.com/ws          # expect 401, not 404 or 502
```

A `404` means the Ingress path is wrong; `502` means the backend Service name or port
is. A `401` is success — it proves the request reached the controller and auth is on.

## When nothing connects

- **No daemons in `/stats`, instances Running.** Check what address the manager handed
  them: it logs `SandD enabled with an embedded controller ... endpoint=...` at startup. If
  that endpoint is not the public one, `SANDD_EXTERNAL_HOST` is wrong.
- **Connections churn every ~60s.** The edge's idle timeout is cutting a healthy but
  quiet connection. See the timeout annotations in `ingress.yaml`.
- **Manager logs refuse every daemon.** `kid`, `iss` or `aud` disagree with what the token
  carries. These are exact-match checks with no fallback. The public key can no longer
  disagree with the private one — it is derived, not configured.
- **No daemon for a Running instance.** The exec relay reports that as NotFound on purpose
  — a still-booting instance is not an error. The same check covers a cluster with SandD
  switched off.

## Known limits

- **One manager replica.** A daemon's connection is a live socket in one process's memory,
  so a second replica would hold a different half of the fleet and `/stats` would report a
  partial answer while looking correct. Peer forwarding over a headless Service is what
  unblocks this — a replica must *ask* its peers who holds a daemon, never compute where it
  ought to be, since no hash can relocate an established TCP connection. A rollout today
  drops the sockets and every daemon reconnects on its own retry loop.
- **`exec` only.** `kubectl logs`, `attach`, `port-forward` and `top` answer 501 on a
  virtual node. Logs are the notable gap: the daemon can already run a command, so
  streaming a container's stdout is a matter of wiring, not of a missing channel.
- **The exec port is unauthenticated.** The kubelet API each virtual node serves
  (`pkg/vnode/kubelet.go`) does not verify its caller — a real kubelet checks the API
  server's client cert and runs a SubjectAccessReview, which needs the `k8s.io/apiserver`
  authn stack this project cannot compile against its pinned k8s line. It is on an
  ephemeral port on the manager's pod IP and is deliberately not exposed by any Service or
  Ingress, so what contains it is the pod network. Anything able to reach that port can
  exec into any workload, with no audit trail.
- **Rotation is a cutover.** The controller holds exactly one key, so `FORCE_REGEN=true`
  invalidates live tokens rather than overlapping old and new, and takes effect when the
  manager restarts.
