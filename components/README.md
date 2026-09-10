# Components

Optional pieces that ship separately from the manager. A cluster that deploys none of them is a
complete Nebula install.

Each subdirectory is its **own Go module**, deliberately: the root module cannot import a
component even by accident, so "the manager does not depend on this" is enforced by the module
boundary rather than by review. The dependency only runs the other way — a component may import
`github.com/InftyAI/Nebula` for the API types, via a `replace` on the repo root.

## The contract

Held to so that the root plumbing keeps working and the second component does not invent its own
conventions:

- **Its own `go.mod`**, with `replace github.com/InftyAI/Nebula => ../..` when it needs the API
  types. The replace is what makes an API change break the component's build in the same PR
  instead of drifting until someone bumps a tag. CI also runs `go mod tidy` and fails on a diff,
  so an untidy `go.mod` is a red build rather than a surprise later.
- **Its own `Makefile` — which is what makes the component visible at all.** The root's
  `components-%` rule and the CI matrix both discover components by globbing
  `components/*/Makefile`, so a directory without one is checked by nothing, silently. `test` is
  the target CI calls; `lint`, `build` and `docker-build` are called through the root by name
  (`make components-lint`).
- **Its own `Dockerfile`, built with the repo root as context** — `docker build -f
  components/<name>/Dockerfile .` — because the `replace` reaches outside the component
  directory. A Dockerfile that assumes its own directory as context cannot resolve it.
- **Its own deployment, kept inside the component.** The root `config/` is the manager's install
  and stays that way. A `config/` of its own or a script both work — logship uses
  [`logship/hack/deploy.sh`](logship/hack/deploy.sh).
- **`internal/` for everything but `cmd/`**, because nothing outside the component should import
  it.

## Why the root tooling does not reach inside

`go build ./...` skips directories containing their own `go.mod`, so the root's `lint`, `test`
and `vet` targets do not see any of this — silently, which is the usual way a nested module rots.
Two things prevent it: the `components-%` pattern rule in the root `Makefile`, and the `discover`
job in `.github/workflows/ci.yml` that fans lint and test out over one job per component. Both
build the list from `components/*/Makefile`, so adding a component needs no edit to either.

## Components

- [`logship/`](logship/) — copies the logs of the instances Nebula runs outside the cluster to
  CloudWatch Logs, so a run can still be explained after the provider's own retention window
  closes. Modal is the first backend, behind a port. [How to run it](logship/README.md);
  [why it is shaped this way](logship/design.md).
