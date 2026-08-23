# Logs and exec

`kubectl logs` (`-f` included) and `kubectl exec` (`-it` included) both work against a
Nebula Pod. It is worth spelling out how, because none of the usual kubelet machinery is
present.

- [The transport](#the-transport)
- [When the API server verifies the certificate](#when-the-api-server-verifies-the-certificate)
- [The provider seam](#the-provider-seam)
- [What logs honour, and the one heuristic](#what-logs-honour-and-the-one-heuristic)
- [Containers are not addressable](#containers-are-not-addressable)
- [Exec needs no agent in the image](#exec-needs-no-agent-in-the-image)

---

## The transport

Neither is a control-plane read: the API server proxies both to the kubelet of the node
the Pod is on, dialing the address in the Node's `status.addresses` and the port in
`status.daemonEndpoints`. A virtual node has no kubelet, so the manager serves those
routes itself (`pkg/vnode/kubelet.go`) and every virtual node advertises the manager's
Pod IP and that port. Consequences worth knowing:

- The endpoint is **leader-scoped and dialed by Pod IP**, not through a Service. The
  tracked Pods live in one process's memory, so a Service balancing across replicas
  would send requests to a replica that answers `NotFound`.
- It serves TLS with a self-signed, in-memory certificate — what the API server
  expects of a kubelet, which does not verify it unless
  `--kubelet-certificate-authority` is set. Where it is set, that certificate is
  rejected and both commands fail until the CSR path below is enabled.
- Client certificates are **not** verified by default, because which CA signs the API
  server's kubelet client cert is not portable across distributions. Anything that can
  reach the port can therefore read the logs of, and **run commands in**, any Pod on
  these virtual nodes, with no RBAC check: keep it closed with a NetworkPolicy, or pass
  `--kubelet-client-ca` to require mTLS.
- No POD_IP (running the manager off-cluster) means no endpoint. Logs and exec degrade
  to unsupported; nothing else is affected.

## When the API server verifies the certificate

An API server started with `--kubelet-certificate-authority` checks the certificate the
kubelet API presents, and a self-signed one fails:

```
Error from server: Get "https://10.244.0.6:10250/containerLogs/default/my-pod/workload":
tls: failed to verify certificate: x509: certificate signed by unknown authority
```

Publishing our own CA somewhere the API server would trust it is not an option: `Node`
has no `caBundle` field, unlike `APIService` and the webhook configurations. The only
certificate that API server accepts is one from the cluster's own
`kubernetes.io/kubelet-serving` signer, so the manager asks for one
(`pkg/vnode/servingcert.go`). Two switches, both off by default.

### Enabling it

**1. Grant the RBAC.** Uncomment both resources in `config/rbac/kustomization.yaml`:

```yaml
- kubelet_serving_role.yaml
- kubelet_serving_role_binding.yaml
```

**2. Pass the flag.** Add it to the manager's `args` in `config/manager/manager.yaml`:

```yaml
        args:
          - --leader-elect
          - --health-probe-bind-address=:8081
          - --kubelet-serving-csr
```

**3. Apply**, with `make deploy IMG=<your image>`, then confirm the request was issued and
that both commands work:

```console
$ kubectl get csr | grep nebula-kubelet-serving
nebula-kubelet-serving-nebula-aws   8s   kubernetes.io/kubelet-serving   system:serviceaccount:nebula-system:nebula-controller-manager   <none>   Approved,Issued

$ kubectl logs <a-nebula-pod>
$ kubectl exec <a-nebula-pod> -- sh -c 'echo ok'
```

One certificate covers every virtual node this manager hosts: the API server verifies the
SAN, which is the manager's Pod IP, and not the `system:node:<name>` subject the signer
insists on — so the CSR is named after whichever provider registered first, and that is
not a mistake.

To enable it on an already-running deployment instead of redeploying, apply the same two
RBAC files and append the flag with
`kubectl -n nebula-system patch deploy nebula-controller-manager --type=json -p
'[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-serving-csr"}]'`.
The restart changes the manager's Pod IP, and with it the address the nodes advertise and
the SAN in the new request — all three move together, so nothing needs coordinating.

### Why the RBAC is separate

Because the manager **self-approves its own request**:
kube-controller-manager auto-approves node *client* certificates only, never serving
ones, since an approver cannot verify that a requester owns the SANs it asks for. So the
grant includes `approve` on that signer, which is the power to obtain a serving
certificate for any node's kubelet endpoint — a deliberate act, hence opt-in. What it is
not is a node identity: the issued certificate carries the `serverAuth` usage only, so
despite its `system:node:<name>` subject it cannot authenticate *as* a node.

### Renewal, and what failure looks like

The certificate is renewed at two thirds of its life, because the signer's
`--cluster-signing-duration` is the cluster's to choose and a short one would otherwise
lose logs mid-run.

Nothing here is fatal: every failure degrades to the self-signed certificate and retries
every 10 minutes, so granting the RBAC late takes effect without a restart. The two things
to read are `signerIssued` in the `serving kubelet api` startup line and the CSR's own
condition, which name the cause between them:

| symptom | cause |
|---|---|
| no CSR at all, `signerIssued=false` | the flag is not set |
| `create`/`approve ... is forbidden` in the log | the RBAC is not applied |
| CSR stays `Approved` and never reaches `Issued` | the cluster's kubelet-serving signer is disabled — common on managed control planes, and not fixable from here |
| `signerIssued=true` but still `x509` | the API server's `--kubelet-certificate-authority` is a different CA than the signer's |

### On kind

kind always sets `--kubelet-certificate-authority`, so both switches above are needed for
`kubectl logs` and `kubectl exec` to work on Nebula pods at all. It also has a second,
unrelated fault worth recognising: it sets the kubelet's `serverTLSBootstrap: true` but
ships nothing that approves the resulting CSRs. So the *real* kubelet never gets a serving
certificate either, and `kubectl logs` fails for every pod in the cluster — with `remote
error: tls: internal error` rather than the x509 error above, since a kubelet with no
certificate cannot complete the handshake at all. Nebula's pods are unaffected by that (it
approves its own), but `kubectl certificate approve` on the pending `system:node:` requests
is what fixes the rest.

kind also signs for 15 minutes, which makes the renewal loop load-bearing here rather than
decorative: a `kubectl logs` that worked at startup and fails twenty minutes later is a
rotation failure, not an issuance one.

## The provider seam

Both are optional: a provider opts in by implementing `provider.LogStreamer` and
`provider.Executor`, and one that does not answers `NotFound` rather than carrying a
stub. Modal implements both; AWS implements neither. Each seam is deliberately minimal —
logs are one stream from the instance's first byte, stdout and stderr merged; exec only
STARTS the command and hands back its streams — so every kubectl option is honoured
once, for all providers, in `pkg/vnode/logs.go` and `pkg/vnode/exec.go`.

## What logs honour, and the one heuristic

A provider stream has no EOF while the instance lives and no marker for "you have now
caught up", which the real kubelet gets for free from a file on disk. So:

| flag | behaviour |
|---|---|
| (none) | ends at the first silent gap (1s) or a 30s ceiling, whichever comes first |
| `--follow` | runs until the instance exits, the client disconnects, or the manager shuts down; a silent gap does NOT end it |
| `--tail=N` | the backlog is buffered into a ring of the last N lines and only those are emitted, then `--follow` continues from there |
| `--limit-bytes` | hard cap on bytes handed to the client, applied last |
| `--timestamps`, `--previous`, `--since`, `--since-time` | accepted and **ignored** |

The idle gap is the heuristic, and it is unavoidable: the alternative for a one-shot
read is hanging until the workload exits, which for a long-running server is forever.
The ceiling covers the opposite case, a workload chatty enough that the stream never
goes idle — it truncates rather than failing, and `--follow` has no ceiling.

`--tail` costs what buffering costs: the full backlog still crosses the provider's API,
because there is no seek. Only the delivery is trimmed, bounded by N lines.

The ignored flags cannot be served from this seam rather than merely being unfinished.
`--timestamps` would have to invent a receive-time stamp, attributing the backlog's
whole history to the moment it was fetched. `--previous` and `--since` need a
per-container restart history and time-indexed storage that no provider exposes. They
are ignored rather than rejected so a habitual `--since=1h` prints the full stream
instead of an error.

## Containers are not addressable

`-c` is accepted and ignored, for logs and exec alike: a Nebula Pod maps to exactly one
external instance with one console, so there is no per-container stream to select or
shell to enter. Honouring the name would mean rejecting `kubectl logs pod` (which sends
no container) or lying about a second container's output.

## Exec needs no agent in the image

The provider's own worker starts the command, so `kubectl exec -it pod -- bash` works
against an unmodified user image — including the `sleep infinity` placeholder a Sandbox
runs. What the exec does need is a **running** instance: a sandbox still queued for
capacity has no container to run in, and the attempt fails rather than waiting.

Nebula only pumps bytes: stdin is forwarded and closed at EOF (so `exec -i -- cat < f`
ends), stdout and stderr are copied back, and a non-zero exit reaches kubectl as
`command terminated with exit code N` rather than a server error. Two gaps, both from the
provider side:

| behaviour | why |
|---|---|
| terminal **resize** is ignored | no provider exposes a window-size call, so `-it` opens at the remote default and stays there |
| under `-t`, **stderr is merged into stdout** | a PTY is one stream; kubectl forbids asking for both anyway |
