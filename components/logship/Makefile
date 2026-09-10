# Image URL to use all building/pushing image targets
IMG ?= inftyai/nebula-logship:latest

CONTAINER_TOOL ?= docker

# What `docker-push` publishes: a manifest list covering both, so the same tag runs on Graviton and
# x86 nodes. A single-platform image is not a build failure and not a run failure either — the kubelet
# rejects it at pull time with "no match for platform in manifest", which reads like a missing tag.
PLATFORMS ?= linux/amd64,linux/arm64

# Shared with the root's image targets on purpose. Every image target names this builder explicitly,
# because the Dockerfile's Go build cache is a BuildKit cache mount and so lives INSIDE the builder:
# targets that drift onto different builders each start from a cold cache, and `buildx use` state is
# not something a Makefile should depend on.
BUILDX_BUILDER ?= nebula-builder

# The repo root. The Docker build context and the shared linter config both live there; see the
# Dockerfile's header for why the context cannot be this directory.
ROOT := ../..

# Reuses the root's bin/ so a component does not download its own copy of every tool.
LOCALBIN ?= $(abspath $(ROOT)/bin)
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: fmt vet ## Run tests.
	go test ./... -coverprofile cover.out

# One config for the whole repo, run separately per module: `./...` at the root cannot see a nested
# module at all, which is the only reason this target exists. Fork the config here if the rules
# ever need to differ.
.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter.
	$(GOLANGCI_LINT) run --config $(abspath $(ROOT)/.golangci.yml)

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes.
	$(GOLANGCI_LINT) run --config $(abspath $(ROOT)/.golangci.yml) --fix

##@ Build

.PHONY: build
build: fmt vet ## Build the logship binary.
	mkdir -p bin
	go build -o bin/logship ./cmd

.PHONY: run
run: fmt vet ## Run logship against your current kubectl context. It takes no flags.
	go run ./cmd

.PHONY: buildx-builder
buildx-builder:
	@$(CONTAINER_TOOL) buildx inspect $(BUILDX_BUILDER) >/dev/null 2>&1 \
	  || $(CONTAINER_TOOL) buildx create --name $(BUILDX_BUILDER) --driver docker-container >/dev/null

# Host platform, not PLATFORMS: --load takes a single image, and the one worth having locally is the
# one this machine can run — a kind node on Apple silicon wants the arm64 build, an x86 CI box the amd64.
.PHONY: docker-build
docker-build: buildx-builder ## Build for this machine, into the local image store. Context is the repo root; see the Dockerfile.
	$(CONTAINER_TOOL) buildx build --builder $(BUILDX_BUILDER) --load \
	  -f Dockerfile -t ${IMG} $(ROOT)

# Builds again instead of pushing what docker-build made, and has to: a manifest list cannot live in
# the local image store, so there is nothing there to push. Pushing the single-platform image that IS
# there is what makes a node of the other architecture fail the pull with "no match for platform in
# manifest" — at pull time, so it reads as a missing tag rather than as the wrong build.
.PHONY: docker-push
docker-push: buildx-builder ## Build every platform in PLATFORMS and push them as one tag.
	$(CONTAINER_TOOL) buildx build --builder $(BUILDX_BUILDER) --push \
	  --platform $(PLATFORMS) -f Dockerfile -t ${IMG} $(ROOT)

##@ Deployment

# Takes no instance: it watches the cluster and ships every Pod Nebula placed, through whichever
# provider placed it, reading each record's identity off that Pod. hack/deploy.sh has the details.
.PHONY: deploy
deploy: ## Ship every Nebula instance in the cluster.
	IMG=$(IMG) hack/deploy.sh

DEPLOY_NAME = $(or $(NAME),logship)

.PHONY: undeploy
undeploy: ## Remove the Deployment and its RBAC.
	kubectl -n $(or $(DEPLOY_NS),nebula-system) delete deployment $(DEPLOY_NAME) --ignore-not-found
	kubectl delete clusterrolebinding $(DEPLOY_NAME) clusterrole $(DEPLOY_NAME) --ignore-not-found
	kubectl -n $(or $(DEPLOY_NS),nebula-system) delete serviceaccount $(DEPLOY_NAME) --ignore-not-found

##@ Dependencies

.PHONY: golangci-lint
golangci-lint: ## Install golangci-lint into the root's bin/ if necessary.
	$(MAKE) -C $(ROOT) golangci-lint
