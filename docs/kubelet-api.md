# Logs and exec

`kubectl logs` (`-f` included) and `kubectl exec` (`-it` included) both work against a
Nebula Pod. It is worth spelling out how, because none of the usual kubelet machinery is
present.

- [The transport](#the-transport)
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
- It starts with a self-signed, in-memory certificate and, by default, creates a
  `kubernetes.io/kubelet-serving` CSR whose IP SAN is the advertised Pod IP. This is
  required by control planes such as EKS that verify kubelet serving certificates.
  The built-in signer requires an external approval decision; once the certificate is
  issued, new TLS handshakes use it immediately without restarting the manager. See
  [deploy.md](deploy.md#configuration) for approval and inspection commands.
- Client certificates are **not** verified by default, because which CA signs the API
  server's kubelet client cert is not portable across distributions. Serving-certificate
  bootstrap secures the opposite direction and does not change that. Anything that can
  reach the port can therefore read logs and **run commands in** any Pod on these virtual
  nodes with no RBAC check: keep it closed with a NetworkPolicy, or pass
  `--kubelet-client-ca` to require mTLS.
- No POD_IP (running the manager off-cluster) means no endpoint. Logs and exec degrade
  to unsupported; nothing else is affected.

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
