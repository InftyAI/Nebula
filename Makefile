# Image URL to use all building/pushing image targets
IMG ?= inftyai/nebula-controller:latest

# NAMESPACE is where the manager runs (must match config/manager). Consumed by
# hack/deploy.sh via the deploy-all target.
NAMESPACE ?= nebula-system

# DEPLOY_KIND_CLUSTER, when set, makes deploy-all load the image into that Kind
# cluster instead of pushing it. It is deliberately SEPARATE from the e2e
# KIND_CLUSTER (which defaults to a throwaway test cluster), so a plain
# `make deploy-all` pushes the image rather than loading it into the e2e cluster.
DEPLOY_KIND_CLUSTER ?=

# VERSION is stamped into the binary (pkg/version) and surfaces as the virtual
# node's kubelet VERSION. Defaults to `git describe` (tag, or short commit for an
# untagged repo, with -dirty when the tree has uncommitted changes). Override
# with `make build VERSION=v1.2.3`.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo nebula-dev)
LDFLAGS ?= -X github.com/InftyAI/Nebula/pkg/version.gitVersion=$(VERSION)

# Get the currently used golang install path (in GOPATH/bin, unless GOBIN is set)
ifeq (,$(shell go env GOBIN))
GOBIN=$(shell go env GOPATH)/bin
else
GOBIN=$(shell go env GOBIN)
endif

# CONTAINER_TOOL defines the container tool to be used for building images.
# Be aware that the target commands are only tested with Docker which is
# scaffolded by default. However, you might want to replace it to use other
# tools. (i.e. podman)
CONTAINER_TOOL ?= docker

# Setting SHELL to bash allows bash commands to be executed by recipes.
# Options are set to exit when a recipe line exits non-zero or a piped command fails.
SHELL = /usr/bin/env bash -o pipefail
.SHELLFLAGS = -ec

.PHONY: all
all: build

##@ General

# The help target prints out all targets with their descriptions organized
# beneath their categories. The categories are represented by '##@' and the
# target descriptions by '##'. The awk command is responsible for reading the
# entire set of makefiles included in this invocation, looking for lines of the
# file as xyz: ## something, and then pretty-format the target and help. Then,
# if there's a line with ##@ something, that gets pretty-printed as a category.
# More info on the usage of ANSI control characters for terminal formatting:
# https://en.wikipedia.org/wiki/ANSI_escape_code#SGR_parameters
# More info on the awk command:
# http://linuxcommand.org/lc3_adv_awk.php

.PHONY: help
help: ## Display this help.
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_0-9-]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

##@ Development

.PHONY: manifests
manifests: controller-gen ## Generate WebhookConfiguration, ClusterRole and CustomResourceDefinition objects.
	$(CONTROLLER_GEN) rbac:roleName=manager-role crd webhook paths="./..." output:crd:artifacts:config=config/crd/bases

.PHONY: generate
generate: controller-gen ## Generate code containing DeepCopy, DeepCopyInto, and DeepCopyObject method implementations.
	$(CONTROLLER_GEN) object:headerFile="hack/boilerplate.go.txt" paths="./..."

# The catalog ConfigMap is generated from CSVs that live canonically under
# pkg/ (so Go can embed them), which is outside config/catalog. Kustomize's
# default load restrictor forbids reaching up the tree, so builds that include
# the catalog pass LoadRestrictionsNone.
KUSTOMIZE_BUILD_FLAGS ?= --load-restrictor=LoadRestrictionsNone

.PHONY: verify-catalog
verify-catalog: kustomize ## Verify the price catalog CSVs parse and the catalog ConfigMap renders.
	go test ./pkg/provider/catalog/...
	$(KUSTOMIZE) build $(KUSTOMIZE_BUILD_FLAGS) config/catalog >/dev/null && echo "catalog ConfigMap renders OK"

##@ SandD (embedded controller)

# The SandD checkout whose Go binding and Rust archive the manager embeds. It is a
# SEPARATE REPO, and go.mod currently `replace`s the module with this path, so building
# Nebula requires it on disk. Override if your checkout lives elsewhere:
#   make build SANDD_DIR=/path/to/SandD
SANDD_DIR ?= $(shell dirname $(CURDIR))/SandD

# Where the static archive lands. The cgo link needs -L pointing here.
SANDD_LIB := $(SANDD_DIR)/target/release/libsandbox_server.a

.PHONY: sandd-archive
sandd-archive: ## Build the SandD controller static archive the manager links against.
	@test -d "$(SANDD_DIR)" || { \
		echo "SANDD_DIR=$(SANDD_DIR) not found. The manager embeds the SandD controller;"; \
		echo "clone https://github.com/InftyAI/SandD beside this repo or set SANDD_DIR=<path>."; \
		exit 1; }
	@command -v cargo >/dev/null || { \
		echo "cargo not found. The embedded controller is Rust: install https://rustup.rs"; \
		exit 1; }
	cd $(SANDD_DIR) && cargo build -p sandbox-server --features ffi --lib --release

# Every Go build and test links the archive, so both need this on CGO_LDFLAGS.
export CGO_LDFLAGS := -L$(SANDD_DIR)/target/release

.PHONY: fmt
fmt: ## Run go fmt against code.
	go fmt ./...

.PHONY: format
format: golangci-lint ## Format code with the configured formatters (gofmt, goimports).
	$(GOLANGCI_LINT) fmt

.PHONY: vet
vet: ## Run go vet against code.
	go vet ./...

.PHONY: test
test: manifests generate fmt vet setup-envtest sandd-archive ## Run tests.
	KUBEBUILDER_ASSETS="$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path)" go test $$(go list ./... | grep -v /e2e) -coverprofile cover.out

# TODO(user): To use a different vendor for e2e tests, modify the setup under 'tests/e2e'.
# The default setup assumes Kind is pre-installed and builds/loads the Manager Docker image locally.
# No cert-manager is needed: the manager provisions its own webhook serving cert
# in-process (pkg/cert), so the suite only needs a Kind cluster.
KIND_CLUSTER ?= nebula-test-e2e

.PHONY: setup-test-e2e
setup-test-e2e: ## Set up a Kind cluster for e2e tests if it does not exist
	@command -v $(KIND) >/dev/null 2>&1 || { \
		echo "Kind is not installed. Please install Kind manually."; \
		exit 1; \
	}
	@case "$$($(KIND) get clusters)" in \
		*"$(KIND_CLUSTER)"*) \
			echo "Kind cluster '$(KIND_CLUSTER)' already exists. Skipping creation." ;; \
		*) \
			echo "Creating Kind cluster '$(KIND_CLUSTER)'..."; \
			$(KIND) create cluster --name $(KIND_CLUSTER) ;; \
	esac

.PHONY: test-e2e
test-e2e: setup-test-e2e manifests generate fmt vet ## Run the e2e tests. Expected an isolated environment using Kind.
	KIND_CLUSTER=$(KIND_CLUSTER) go test ./test/e2e/ -v -ginkgo.v
	$(MAKE) cleanup-test-e2e

.PHONY: cleanup-test-e2e
cleanup-test-e2e: ## Tear down the Kind cluster used for e2e tests
	@$(KIND) delete cluster --name $(KIND_CLUSTER)

.PHONY: lint
lint: golangci-lint ## Run golangci-lint linter
	$(GOLANGCI_LINT) run

.PHONY: lint-fix
lint-fix: golangci-lint ## Run golangci-lint linter and perform fixes
	$(GOLANGCI_LINT) run --fix

.PHONY: lint-config
lint-config: golangci-lint ## Verify golangci-lint linter configuration
	$(GOLANGCI_LINT) config verify

##@ Build

.PHONY: build
build: manifests generate fmt vet sandd-archive ## Build manager binary.
	go build -ldflags "$(LDFLAGS)" -o bin/manager cmd/main.go

.PHONY: run
run: manifests generate fmt vet sandd-archive ## Run a controller from your host.
	go run -ldflags "$(LDFLAGS)" ./cmd/main.go

# If you wish to build the manager image targeting other platforms you can use the --platform flag.
# (i.e. docker build --platform linux/arm64). However, you must enable docker buildKit for it.
# More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# SandD's RUST sources are staged INTO the build context because Docker cannot read a path
# outside it, and the manager embeds the controller as a static archive the image compiles
# itself. Staged as a copy rather than symlinked (the daemon follows neither).
#
# Only the Rust half: the Go binding is a published module as of SandD v0.0.7 and comes
# from the proxy via go.mod, so it no longer has to be visible here.
#
# Excludes target/: it is the host's build output, often gigabytes and wrong-platform, and
# the image rebuilds the archive for its own target anyway.
SANDD_STAGE := .sandd-build
.PHONY: sandd-stage
sandd-stage: ## Stage SandD sources into the docker build context.
	@test -d "$(SANDD_DIR)" || { \
		echo "SANDD_DIR=$(SANDD_DIR) not found. The manager embeds the SandD controller;"; \
		echo "clone https://github.com/InftyAI/SandD beside this repo or set SANDD_DIR=<path>."; \
		exit 1; }
	rm -rf $(SANDD_STAGE)
	mkdir -p $(SANDD_STAGE)
	cd $(SANDD_DIR) && tar --exclude=target --exclude=.git -cf - \
		Cargo.toml Cargo.lock server protocol sandd | tar -xf - -C $(CURDIR)/$(SANDD_STAGE)

# The stage is removed even when the build fails, so a broken build does not leave a copy of
# another repo sitting in the tree.
.PHONY: docker-build
docker-build: sandd-stage ## Build docker image with the manager.
	$(CONTAINER_TOOL) build --build-arg VERSION=$(VERSION) -t ${IMG} . \
		; status=$$?; rm -rf $(SANDD_STAGE); exit $$status

.PHONY: docker-push
docker-push: ## Push docker image with the manager.
	$(CONTAINER_TOOL) push ${IMG}

# PLATFORMS defines the target platforms for the manager image be built to provide support to multiple
# architectures. (i.e. make docker-buildx IMG=myregistry/mypoperator:0.0.1). To use this option you need to:
# - be able to use docker buildx. More info: https://docs.docker.com/build/buildx/
# - have enabled BuildKit. More info: https://docs.docker.com/develop/develop-images/build_enhancements/
# - be able to push the image to your registry (i.e. if you do not set a valid value via IMG=<myregistry/image:<tag>> then the export will fail)
# To adequately provide solutions that are compatible with multiple platforms, you should consider using this option.
PLATFORMS ?= linux/arm64,linux/amd64
.PHONY: docker-buildx
docker-buildx: sandd-stage ## Build and push docker image for the manager for cross-platform support
	# copy existing Dockerfile and insert --platform=${BUILDPLATFORM} into Dockerfile.cross, and preserve the original Dockerfile
	sed -e '1 s/\(^FROM\)/FROM --platform=\$$\{BUILDPLATFORM\}/; t' -e ' 1,// s//FROM --platform=\$$\{BUILDPLATFORM\}/' Dockerfile > Dockerfile.cross
	- $(CONTAINER_TOOL) buildx create --name nebula-builder
	$(CONTAINER_TOOL) buildx use nebula-builder
	- $(CONTAINER_TOOL) buildx build --push --platform=$(PLATFORMS) --build-arg VERSION=$(VERSION) --tag ${IMG} -f Dockerfile.cross .
	- $(CONTAINER_TOOL) buildx rm nebula-builder
	rm Dockerfile.cross
	rm -rf $(SANDD_STAGE)

.PHONY: build-installer
build-installer: manifests generate kustomize ## Generate a consolidated YAML with CRDs and deployment.
	mkdir -p dist
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build $(KUSTOMIZE_BUILD_FLAGS) config/default > dist/install.yaml

##@ Deployment

ifndef ignore-not-found
  ignore-not-found = false
endif

.PHONY: install
install: manifests kustomize ## Install CRDs into the K8s cluster specified in ~/.kube/config.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) apply -f -

.PHONY: uninstall
uninstall: manifests kustomize ## Uninstall CRDs from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build config/crd | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy
deploy: manifests kustomize ## Deploy controller to the K8s cluster specified in ~/.kube/config.
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build $(KUSTOMIZE_BUILD_FLAGS) config/default | $(KUBECTL) apply -f -

.PHONY: deploy-e2e
deploy-e2e: manifests kustomize ## Deploy for e2e: config/default plus the fake-provider env var (baked in at deploy time, not via a post-deploy rollout).
	cd config/manager && $(KUSTOMIZE) edit set image controller=${IMG}
	$(KUSTOMIZE) build $(KUSTOMIZE_BUILD_FLAGS) config/e2e | $(KUBECTL) apply -f -

.PHONY: undeploy
undeploy: kustomize ## Undeploy controller from the K8s cluster specified in ~/.kube/config. Call with ignore-not-found=true to ignore resource not found errors during deletion.
	$(KUSTOMIZE) build $(KUSTOMIZE_BUILD_FLAGS) config/default | $(KUBECTL) delete --ignore-not-found=$(ignore-not-found) -f -

.PHONY: deploy-all
deploy-all: ## Build the image, apply per-provider credential Secrets from .env, and deploy. See .env.example.
	IMG=$(IMG) NAMESPACE=$(NAMESPACE) KIND_CLUSTER=$(DEPLOY_KIND_CLUSTER) KUBECTL=$(KUBECTL) hack/deploy.sh

##@ Dependencies

## Location to install dependencies to
LOCALBIN ?= $(shell pwd)/bin
$(LOCALBIN):
	mkdir -p $(LOCALBIN)

## Tool Binaries
KUBECTL ?= kubectl
KIND ?= kind
KUSTOMIZE ?= $(LOCALBIN)/kustomize
CONTROLLER_GEN ?= $(LOCALBIN)/controller-gen
ENVTEST ?= $(LOCALBIN)/setup-envtest
GOLANGCI_LINT = $(LOCALBIN)/golangci-lint

## Tool Versions
KUSTOMIZE_VERSION ?= v5.6.0
CONTROLLER_TOOLS_VERSION ?= v0.18.0
#ENVTEST_VERSION is the version of controller-runtime release branch to fetch the envtest setup script (i.e. release-0.20)
ENVTEST_VERSION ?= $(shell go list -m -f "{{ .Version }}" sigs.k8s.io/controller-runtime | awk -F'[v.]' '{printf "release-%d.%d", $$2, $$3}')
#ENVTEST_K8S_VERSION is the version of Kubernetes to use for setting up ENVTEST binaries (i.e. 1.31)
ENVTEST_K8S_VERSION ?= $(shell go list -m -f "{{ .Version }}" k8s.io/api | awk -F'[v.]' '{printf "1.%d", $$3}')
GOLANGCI_LINT_VERSION ?= v2.1.6

.PHONY: kustomize
kustomize: $(KUSTOMIZE) ## Download kustomize locally if necessary.
$(KUSTOMIZE): $(LOCALBIN)
	$(call go-install-tool,$(KUSTOMIZE),sigs.k8s.io/kustomize/kustomize/v5,$(KUSTOMIZE_VERSION))

.PHONY: controller-gen
controller-gen: $(CONTROLLER_GEN) ## Download controller-gen locally if necessary.
$(CONTROLLER_GEN): $(LOCALBIN)
	$(call go-install-tool,$(CONTROLLER_GEN),sigs.k8s.io/controller-tools/cmd/controller-gen,$(CONTROLLER_TOOLS_VERSION))

.PHONY: setup-envtest
setup-envtest: envtest ## Download the binaries required for ENVTEST in the local bin directory.
	@echo "Setting up envtest binaries for Kubernetes version $(ENVTEST_K8S_VERSION)..."
	@$(ENVTEST) use $(ENVTEST_K8S_VERSION) --bin-dir $(LOCALBIN) -p path || { \
		echo "Error: Failed to set up envtest binaries for version $(ENVTEST_K8S_VERSION)."; \
		exit 1; \
	}

.PHONY: envtest
envtest: $(ENVTEST) ## Download setup-envtest locally if necessary.
$(ENVTEST): $(LOCALBIN)
	$(call go-install-tool,$(ENVTEST),sigs.k8s.io/controller-runtime/tools/setup-envtest,$(ENVTEST_VERSION))

.PHONY: golangci-lint
golangci-lint: $(GOLANGCI_LINT) ## Download golangci-lint locally if necessary.
$(GOLANGCI_LINT): $(LOCALBIN)
	$(call go-install-tool,$(GOLANGCI_LINT),github.com/golangci/golangci-lint/v2/cmd/golangci-lint,$(GOLANGCI_LINT_VERSION))

# go-install-tool will 'go install' any package with custom target and name of binary, if it doesn't exist
# $1 - target path with name of binary
# $2 - package url which can be installed
# $3 - specific version of package
define go-install-tool
@[ -f "$(1)-$(3)" ] || { \
set -e; \
package=$(2)@$(3) ;\
echo "Downloading $${package}" ;\
rm -f $(1) || true ;\
GOBIN=$(LOCALBIN) go install $${package} ;\
mv $(1) $(1)-$(3) ;\
} ;\
ln -sf $(1)-$(3) $(1)
endef
