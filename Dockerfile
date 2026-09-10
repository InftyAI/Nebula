# Build the manager binary.
#
# --platform=${BUILDPLATFORM} pins the builder stage to the machine doing the building and leaves the
# target to GOOS/GOARCH below. Without it, buildx runs a foreign-arch builder under emulation —
# minutes of QEMU per architecture for a cross-compile Go does for free.
FROM --platform=${BUILDPLATFORM} golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH
# VERSION is stamped into the binary (pkg/version) and surfaces as the virtual
# node's kubelet VERSION. Passed by `make docker-build` (defaults to git describe).
ARG VERSION=nebula-dev

WORKDIR /workspace
# Split from the source COPY below so a source-only change does not re-resolve modules.
COPY go.mod go.mod
COPY go.sum go.sum
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Copy the go source
COPY cmd/main.go cmd/main.go
COPY api/ api/
COPY internal/ internal/
COPY pkg/ pkg/

# TARGETOS/TARGETARCH come from buildx's --platform; a plain `docker build` leaves them at the host
# platform, so `make docker-build` still yields a native image.
#
# The mounts are BuildKit-managed volumes, so GOCACHE and the module cache survive between builds
# instead of starting empty every time; GOCACHE is keyed by arch so two platforms building at once
# do not thrash one directory. `-a` is deliberately absent, and re-adding it (kubebuilder scaffolds
# it) undoes all of this: it means "ignore the build cache", for a byte-identical binary.
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build,id=gobuild-${TARGETARCH} \
    CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -ldflags "-X github.com/InftyAI/Nebula/pkg/version.gitVersion=${VERSION}" \
    -o manager cmd/main.go

# Use distroless as minimal base image to package the manager binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/manager .
USER 65532:65532

ENTRYPOINT ["/manager"]
