# logship

Copies the logs of the instances Nebula runs outside the cluster to CloudWatch Logs, so a run can
still be explained after the provider's own retention window closes. Optional: a cluster that does
not deploy it is unaffected, and the manager does not link a byte of it.

Modal is the one provider implemented so far, and it is one backend behind a port rather than the
subject of this component — see [Providers](#providers). The design and the reasoning live in
[design.md](design.md); this file is how to run what exists.

## Layout

| path | what is in it |
| --- | --- |
| `cmd/main.go` | the entrypoint, and all of it: signals, the provider set, the watch. No flags |
| `cmd/fleet.go` | the wiring — an instance to its provider, a Pod's labels to a record, one sink for the process. `register` is the only place a provider is named |
| `internal/provider/` | the port a backend implements, and the registry that opens one when the first Pod needs it |
| `internal/modal/` | the Modal backend: its own `SandboxGetLogs` client, the cursor-aware reader and re-open loop, a gRPC connection pool |
| `internal/ship/` | one stream's mechanism: chunks to lines, lines to a record, records to batches, and the `Pipeline` that runs them |
| `internal/emit/` | the sink — one record per line on stdout, shared by every stream, plus the size limits that keep a line collectable |
| `internal/supervise/` | lifetimes: a changing set of instances becomes a running set of pipelines, with the restart budget |
| `internal/watch/` | the cluster half — which Pods to ship, and the instance each one names |
| `hack/deploy.sh` | what `make deploy` runs: ServiceAccount, ClusterRole, Deployment, and the checks to run afterwards in the order they can fail |
| `Makefile`, `Dockerfile`, `go.mod` | its own module and image, so the manager cannot depend on this even by accident |

Tests sit beside the code they cover. `internal/modal/fake_server_test.go` is a fake
`SandboxGetLogs` server, which is why `make test` needs no Modal account.

## Running

The reader, the pipeline, the sink, the supervisor and the Pod watch exist; the cursor checkpoint and
the drain finalizer do not. So one process discovers and ships every instance in the cluster, but a
restart replays each one from the beginning.

It takes no arguments, and there are none to take: it lists Pods labelled
`nebula.inftyai.com/enabled=true`, reads which provider each was placed on off its `spec.nodeSelector`
(`nebula.inftyai.com/provider`), and reads the instance ID off the `nebula.inftyai.com/instance-id`
annotation the virtual kubelet writes back once `Provision` returns one. A Pod without it is skipped,
not an error.

```console
$ export MODAL_TOKEN_ID=ak-... MODAL_TOKEN_SECRET=as-...
$ make run
logship: watching Pods enabled nebula.inftyai.com/enabled instanceID nebula.inftyai.com/instance-id providers [modal]
logship: syncing instances 1
```

There is deliberately no way to name an instance — not to ship one, and not to read one either. A
record's identity has one source, the Pod that owns the instance, because a second source can only
disagree with it, and a record with the wrong identity is delivered, retained, billed and invisible.

Records go to **stdout**, one per line; every diagnostic goes to stderr. Each record carries the
*instance's* pod name and labels, because logship prints another pod's output while the log agent
stamps only its own identity:

```console
{"time":"...","log":"training step 1","kubernetes":{"pod_name":"exp-1-sandbox-0","labels":{...}}}
```

Name and labels are the whole of it — no namespace, because the consumer identifies a line by pod name
and labels alone.

Modal's credentials come from the environment only: `MODAL_TOKEN_ID`, `MODAL_TOKEN_SECRET`, and
optionally `MODAL_SERVER_URL`. The SDK merges `~/.modal.toml` too, but its loader is unexported, so a
local profile has to be exported as variables. They are needed only when something is actually on
Modal — a backend opens when the first Pod on it arrives, not at startup.

## Providers

A provider is one implementation of `internal/provider.Provider`: which streams an instance has, a
`ship.Source` for one of them, and `Reserve` for a backend whose transport caps concurrent streams per
connection. Everything above it — the batching, the record shape, the restart budget, the watch — is
written against that port and names no provider. Adding one is a package plus one line in
`cmd/fleet.go`'s `register`.

Which provider an instance needs is the Pod's to say, and is deliberately not configurable: a
configured provider would have to be kept in agreement with the cluster's node pools, and being wrong
about it is indistinguishable from an idle cluster — every Pod skipped, nothing shipped, no error.
Routing per Pod also means a cluster split across two providers ships from both with no extra wiring.
A Pod on a provider this build has no backend for is logged and skipped, because Nebula gaining a
provider before logship does is a real state that must not look like silence.

## Deploying

One Deployment for the whole cluster. It needs no instance, no pod and no labels, because the watch
takes all three off each Pod:

```console
make docker-push IMG=inftyai/nebula-logship:latest   # amd64 + arm64, pushed as one tag
make deploy IMG=inftyai/nebula-logship:latest
make undeploy      # also removes the ServiceAccount and ClusterRole
```

`docker-push` does its own build and does not run `docker-build`: a manifest list cannot sit in the
local image store, so there is nothing there to push. Pushing a single-platform image instead fails on
a node of the other architecture at *pull* time — `no match for platform in manifest`, which reads like
a missing tag rather than the wrong build. Override `PLATFORMS` to publish just one.

Four things about a deployed one, in the order they bite:

- **`replicas: 1` and `strategy: Recreate` are both load-bearing.** Two processes on one instance read
  from the same cursor and ship every line twice, and there is no leader election to prevent it —
  `RollingUpdate` would do exactly that for the seconds its `maxSurge` pod overlaps the old one. The
  single replica is therefore the whole fleet's collection and a single point of failure, deliberately:
  scaling out needs the lease first.
- **Restarts replay, and replays are billed.** A fresh process resumes from the cursor it was given,
  which is the beginning, so the provider re-sends the instance's whole history and CloudWatch stores
  it again. `supervise` bounds this in-process, but a *container* restart escapes that bound, which
  makes a CrashLoopBackOff a billing loop rather than an outage. Check `RESTARTS` on a long run.
- **The namespace decides whether records arrive at all**, since they reach CloudWatch through the log
  agent of the cluster logship runs in. The default, `nebula-system`, is where Nebula's manager already
  runs in the dev cluster, and the manager's own container log is a live stream in the group the
  consumer reads — so collection there is checked, not assumed.
- **A shipped instance and a findable one are not the same thing.** The consumer filters on `app` and
  its three tenant IDs, so an instance's Pod missing one of them ships records that are delivered,
  retained, billed and invisible, with nothing reporting a failure. Nothing here can fix that; the
  labels are the Pod's. The fields the watch reads — note the two domains, Nebula's markers on
  `nebula.inftyai.com` and the tenant IDs on the consumer's own:

  ```console
  kubectl -n <namespace> get pod exp-1-sandbox-0 -o json | jq -r '
    .metadata.annotations["nebula.inftyai.com/instance-id"],
    .spec.nodeSelector["nebula.inftyai.com/provider"], (.metadata.labels)'
  ```

In CloudWatch the record arrives one level down, under `log_processed`, with the agent's own identity
at the top. That nesting is the contract the consumer's filter patterns are written against — and a
pattern matching nothing is not an error, so a filter written against the top level alone reads as
"nothing arrived". There is no fleet size to configure either: each provider makes room for an instance
before its streams open, and one that cannot skips the instance with a log.

## Building

```console
make test          # unit tests, no provider account needed
make lint          # the repo's shared golangci config, run against this module
make build         # bin/logship
make docker-build  # this machine's platform only; context is the repo ROOT, see the Dockerfile header
```

`make -C ../.. components-test` runs the same from the root. Note that the root's own `make test` and
`make lint` do **not** cover this module: `./...` stops at a nested `go.mod`.
