#!/usr/bin/env bash
#
# deploy.sh — build the manager image, create per-provider credential Secrets
# from a .env file, and deploy Nebula to the cluster in the current kube context.
#
# Usage:
#   cp .env.example .env && $EDITOR .env
#   hack/deploy.sh                                    # or: make deploy-all
#   IMG=myrepo/nebula:v1 KIND_CLUSTER=kind hack/deploy.sh
#
# .env holds ONLY secrets (provider credentials); see .env.example. Non-secret
# config is passed as environment variables / make flags:
#   IMG           manager image to build+deploy   (default example.com/nebula:v0.0.1)
#   NAMESPACE     namespace the manager runs in    (default nebula-system)
#   KIND_CLUSTER  if set, load the image into this Kind cluster instead of pushing
#
# The script is idempotent: re-running rebuilds the image and re-applies
# Secrets/manifests in place.
#
# Design notes:
#   - Credentials live in ONE Secret PER PROVIDER (not a shared one), matching
#     Nebula's "creds-absent → skip that provider, not fatal" model: a provider
#     whose env vars are blank is skipped here, its Secret is never created, and
#     the manager logs+skips it at registration (see registerProviders).
#   - A provider Secret is (re)created only when ALL its required keys are set,
#     so a partial config never produces a half-populated Secret.
set -euo pipefail

# --- locate repo root so the script works from anywhere --------------------
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${REPO_ROOT}"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
warn() { printf '\033[33mWARN:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# --- non-secret config: env/flags only, never from .env --------------------
IMG="${IMG:-example.com/nebula:v0.0.1}"
NAMESPACE="${NAMESPACE:-nebula-system}"
KIND_CLUSTER="${KIND_CLUSTER:-}"
KUBECTL="${KUBECTL:-kubectl}"

command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found on PATH"

# --- load .env (secrets only) ----------------------------------------------
# We PARSE .env into a private associative array rather than `source`-ing it, so
# its values never enter this script's ENVIRONMENT. That is what lets .env use the
# standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY names safely: sourcing them
# (even without `set -a`, if they are already exported in the caller's shell) would
# hand the PROVISIONING identity to every child process — including `aws eks
# get-token`, kubectl's EKS auth plugin — and break cluster auth with a 401. Held
# in ENV_VARS[], the creds reach the Secret (via create_provider_secret) but not
# kubectl, so the deployer's own AWS identity keeps talking to the cluster.
declare -A ENV_VARS=()
ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "${ENV_FILE}" ]]; then
  log "loading credentials from ${ENV_FILE}"
  while IFS= read -r line || [[ -n "${line}" ]]; do
    line="${line%$'\r'}"                        # tolerate CRLF
    [[ "${line}" =~ ^[[:space:]]*# ]] && continue   # comment
    [[ "${line}" =~ ^[[:space:]]*$ ]] && continue   # blank
    line="${line#export }"                      # allow `export KEY=val`
    [[ "${line}" != *=* ]] && continue          # not an assignment
    local_key="${line%%=*}"; local_val="${line#*=}"
    local_key="${local_key//[[:space:]]/}"      # trim whitespace around the key
    # strip one layer of matching surrounding quotes from the value
    if [[ "${local_val}" == \"*\" || "${local_val}" == \'*\' ]]; then
      local_val="${local_val:1:${#local_val}-2}"
    fi
    ENV_VARS["${local_key}"]="${local_val}"
  done < "${ENV_FILE}"
else
  warn "${ENV_FILE} not found; no provider credentials will be applied. Copy .env.example to .env."
fi

# --- per-provider secret table ---------------------------------------------
# One entry per provider: "<secret-name>|<REQUIRED_KEYS>|<OPTIONAL_KEYS>".
# Each key names both the variable read from .env (ENV_VARS[KEY]) and the field it
# is stored under in the Secret (the name the provider SDK reads inside the pod) —
# they are the same because .env is PARSED, not sourced, so a key can match the
# SDK's standard name without leaking into the deployer's environment (see the
# loader above). To add a provider, append a row — nothing else needs to change.
PROVIDER_SECRETS=(
  # Only secrets belong here.
  "nebula-modal-credentials|MODAL_TOKEN_ID MODAL_TOKEN_SECRET|"
  # AWS: creds are the only secret. CRITICAL — these are the PROVISIONING identity
  # (the account that launches GPU instances), which is DISTINCT from the identity
  # that talks to the EKS control plane this deploy runs against. They use the
  # standard AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY names — safe ONLY because the
  # loader parses .env into ENV_VARS[] instead of exporting it, so these never reach
  # `aws eks get-token` (kubectl's EKS auth plugin, which reads those exact env
  # vars) and cannot hijack cluster auth. Both required together (a lone key is a
  # misconfig) → a blank pair skips the Secret and the SDK falls back to IRSA /
  # instance role (the preferred path). Region is NON-SECRET (on the manager
  # Deployment); the adapter self-configures the rest (GPU AMI + subnets).
  "nebula-aws-credentials|AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY|"
  # "nebula-runpod-credentials|RUNPOD_API_KEY|"
)

# create_provider_secret <secret-name> <required-keys> <optional-keys>
# Skips (leaving any existing Secret untouched) when a required key is blank.
create_provider_secret() {
  local name="$1" required="$2" optional="$3"
  local args=() key val missing=0

  # Each key names both the .env variable (ENV_VARS[key]) and the Secret field.
  for key in ${required}; do
    val="${ENV_VARS[${key}]:-}"
    if [[ -z "${val}" ]]; then
      missing=1
      continue
    fi
    args+=(--from-literal="${key}=${val}")
  done

  if [[ "${missing}" -eq 1 ]]; then
    log "skipping Secret ${name} (required keys unset) — provider will be skipped at registration"
    return 0
  fi

  for key in ${optional}; do
    val="${ENV_VARS[${key}]:-}"
    [[ -n "${val}" ]] && args+=(--from-literal="${key}=${val}")
  done

  log "applying Secret ${name} in ${NAMESPACE}"
  # create --dry-run | apply makes this idempotent (create-or-update in place).
  "${KUBECTL}" create secret generic "${name}" \
    --namespace "${NAMESPACE}" \
    "${args[@]}" \
    --dry-run=client -o yaml | "${KUBECTL}" apply -f -
}

# --- 1. build the image, and make it reachable by the cluster --------------
# Kind and a real registry differ in BOTH build arch and delivery:
#   - Kind runs on the host's single architecture, so a plain single-arch
#     `docker-build` + `kind load` is correct (and multi-arch buildx cannot
#     `kind load` anyway — buildx only pushes).
#   - A cloud registry may back nodes of a DIFFERENT arch than this machine
#     (e.g. building on an arm64 Mac for amd64 EKS nodes), so we MUST build a
#     multi-arch manifest — otherwise the node finds the image but "no match
#     for platform in manifest". `docker-buildx` builds AND pushes in one step.
if [[ -n "${KIND_CLUSTER}" ]]; then
  command -v kind >/dev/null 2>&1 || die "KIND_CLUSTER=${KIND_CLUSTER} set but 'kind' not found on PATH"
  log "building image ${IMG}"
  make docker-build IMG="${IMG}"
  log "loading ${IMG} into Kind cluster ${KIND_CLUSTER}"
  kind load docker-image "${IMG}" --name "${KIND_CLUSTER}"
else
  log "building + pushing multi-arch ${IMG} (set KIND_CLUSTER to load into Kind instead)"
  make docker-buildx IMG="${IMG}"
fi

# --- 3. Secrets FIRST, so the manager mounts them on its very first boot ----
# Ordering matters and lets us avoid any manager restart:
#   - the webhook cert Secret is a REQUIRED volume mount, so it must exist
#     before the pod starts;
#   - provider credentials are read from the environment at process start.
# Both are consumed only at pod startup, so creating them before `make deploy`
# means the manager comes up already correct — no restart, no race.
#
# Secrets need the namespace, which `make deploy` would create — so create it
# up front (idempotent; kustomize re-applies it harmlessly during deploy).
log "ensuring namespace ${NAMESPACE}"
"${KUBECTL}" create namespace "${NAMESPACE}" --dry-run=client -o yaml | "${KUBECTL}" apply -f -

# Webhook serving cert (no cert-manager): generate the self-signed cert into the
# Secret now. The caBundle is injected later, after the webhook config exists.
log "provisioning webhook serving certificate Secret (self-signed)"
NAMESPACE="${NAMESPACE}" KUBECTL="${KUBECTL}" hack/gen-webhook-cert.sh secret

# Provider credential Secrets, one per provider (blank required keys → skipped).
for row in "${PROVIDER_SECRETS[@]}"; do
  IFS='|' read -r name required optional <<<"${row}"
  create_provider_secret "${name}" "${required}" "${optional}"
done

# --- 4. install CRDs + deploy the manager ----------------------------------
# The pod mounts the cert Secret and reads provider creds at boot — both already
# exist, so the manager comes up fully configured with no restart needed.
log "installing CRDs and deploying the manager"
make deploy IMG="${IMG}"

# --- 5. inject the webhook CA bundle (server-side, no manager restart) ------
# This edits only the MutatingWebhookConfiguration, which just got created by
# `make deploy`. Only the API server reads caBundle, so the manager is untouched.
log "injecting webhook CA bundle"
NAMESPACE="${NAMESPACE}" KUBECTL="${KUBECTL}" hack/gen-webhook-cert.sh cabundle

log "done. Check status with:"
printf '    %s -n %s get pods\n' "${KUBECTL}" "${NAMESPACE}"
printf '    %s -n %s logs deploy/nebula-controller-manager\n' "${KUBECTL}" "${NAMESPACE}"
