# Build the logship binary.
#
# THE BUILD CONTEXT IS THE REPO ROOT, not this directory:
#
#     docker build -f components/logship/Dockerfile .
#
# go.mod carries `replace github.com/InftyAI/Nebula => ../..` for the API types, so a context
# rooted at this directory cannot resolve the replacement and `go mod download` fails.
#
# --platform=${BUILDPLATFORM} pins the builder stage to the machine doing the building and leaves the
# target to GOOS/GOARCH below. Without it, buildx runs an arm64 builder under emulation to produce the
# arm64 image — minutes of QEMU per architecture for a cross-compile Go does for free. It is also why
# this needs no Dockerfile.cross sed dance like the root's docker-buildx.
FROM --platform=${BUILDPLATFORM} golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# The replaced module's go.mod must be present for the component's own resolution to work, even
# where no package from it is imported yet.
COPY go.mod go.mod
COPY go.sum go.sum
COPY api/ api/

WORKDIR /workspace/components/logship
COPY components/logship/go.mod go.mod
COPY components/logship/go.sum go.sum
RUN go mod download

COPY components/logship/cmd/ cmd/
COPY components/logship/internal/ internal/

RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -a -o logship ./cmd

FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY --from=builder /workspace/components/logship/logship .
USER 65532:65532

ENTRYPOINT ["/logship"]
