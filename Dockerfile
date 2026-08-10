# The SandD controller, as a static archive.
#
# It is Rust, and the manager EMBEDS it: the WebSocket server every provisioned instance's
# daemon dials into runs inside the manager process, because a daemon's connection is a
# live socket owned by whichever process accepted it — so hosting it here is what lets the
# virtual kubelet reach back into a workload for `kubectl exec` without a second process to
# relay through.
#
# alpine (musl) rather than the glibc image so the archive can be linked FULLY STATIC
# below, which is what keeps the final image on distroless/static.
FROM rust:1-alpine AS sandd
RUN apk add --no-cache musl-dev
WORKDIR /sandd
# .sandd-build is not checked in: SandD is a separate repo, and `make docker-build` stages a
# copy of it there because Docker cannot COPY from outside the build context. Building with
# a bare `docker build` therefore fails here — go through the Makefile.
#
# The whole workspace, not just server/: Cargo resolves every member listed in the root
# Cargo.toml even when only one package is built, so omitting a member fails the build.
COPY .sandd-build/ .
# --features ffi is what exports the C ABI (server/src/ffi.rs) the Go binding links
# against. Without it the archive builds and the symbols are simply absent.
RUN cargo build -p sandbox-server --features ffi --lib --release

# Build the manager binary
FROM golang:1.24-alpine AS builder
ARG TARGETOS
ARG TARGETARCH
# VERSION is stamped into the binary (pkg/version) and surfaces as the virtual
# node's kubelet VERSION. Passed by `make docker-build` (defaults to git describe).
ARG VERSION=nebula-dev

# gcc and musl-dev because the build is now cgo: the Go toolchain shells out to a C
# compiler to link the archive above.
RUN apk add --no-cache gcc musl-dev

WORKDIR /workspace
# No COPY for the Go binding: it is a published module as of SandD v0.0.7, so
# `go mod download` fetches it like any other dependency. It used to be staged at /SandD/go
# to satisfy a `replace` directive pointing at a sibling checkout.
#
# The Rust archive above is still staged from .sandd-build, and must be: it is a build
# artifact, not a Go module, and nothing publishes it.
# Copy the Go Modules manifests
COPY go.mod go.mod
COPY go.sum go.sum
# cache deps before building and copying source so that we don't need to re-download as much
# and so that source changes don't invalidate our downloaded layer
RUN go mod download

COPY --from=sandd /sandd/target/release/libsandbox_server.a /sandd/lib/

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

# Build
# the GOARCH has not a default value to allow the binary be built according to the host where the command
# was called. For example, if we call make docker-build in a local env which has the Apple Silicon M1 SO
# the docker BUILDPLATFORM arg will be linux/arm64 when for Apple x86 it will be linux/amd64. Therefore,
# by leaving it empty we can ensure that the container and binary shipped on it will have the same platform.
#
# CGO_ENABLED=1, unlike a pure-Go manager: the embedded controller crosses a C ABI. The
# `-extldflags -static` is what preserves the property that mattered before — a
# self-contained binary with no libc to find at runtime — so the distroless/static base
# below still works. Drop that flag and the image starts failing at exec with a missing
# shared object, which looks like a corrupt image rather than a link setting.
RUN CGO_ENABLED=1 CGO_LDFLAGS="-L/sandd/lib" GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a -ldflags "-extldflags '-static' -X github.com/InftyAI/Nebula/pkg/version.gitVersion=${VERSION}" \
    -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
