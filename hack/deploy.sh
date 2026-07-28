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
ENV_FILE="${ENV_FILE:-.env}"
if [[ -f "${ENV_FILE}" ]]; then
  log "loading credentials from ${ENV_FILE}"
  set -a                # export everything defined while sourcing
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
else
  warn "${ENV_FILE} not found; no provider credentials will be applied. Copy .env.example to .env."
fi

# --- per-provider secret table ---------------------------------------------
# One entry per provider: "<secret-name>|<REQUIRED_KEYS>|<OPTIONAL_KEYS>".
# Keys are env var names read from .env; the Secret stores each verbatim (the
# SDKs read them by these exact names). To add a provider, append a row —
# nothing else in this script needs to change.
PROVIDER_SECRETS=(
  # Only secrets belong here.
  "nebula-modal-credentials|MODAL_TOKEN_ID MODAL_TOKEN_SECRET|"
  # AWS: creds are the only secret. The access key + secret are required together
  # (a lone key is a misconfig), so a blank pair skips the Secret — the SDK then
  # relies on IRSA / instance role, which is the preferred path. AWS_SESSION_TOKEN
  # is OPTIONAL: it is REQUIRED for temporary STS/SSO creds (ASIA... keys) and
  # unused for long-lived IAM user keys (AKIA...), so it is only added to the
  # Secret when set. The region is NON-SECRET and lives on the manager Deployment,
  # not in this Secret; the adapter self-configures the rest (GPU AMI + subnets).
  "nebula-aws-credentials|AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY|AWS_SESSION_TOKEN"
  # "nebula-runpod-credentials|RUNPOD_API_KEY|"
)

# create_provider_secret <secret-name> <required-keys> <optional-keys>
# Skips (leaving any existing Secret untouched) when a required key is blank.
create_provider_secret() {
  local name="$1" required="$2" optional="$3"
  local args=() key val missing=0

  for key in ${required}; do
    val="${!key:-}"
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
    val="${!key:-}"
    [[ -n "${val}" ]] && args+=(--from-literal="${key}=${val}")
  done

  log "applying Secret ${name} in ${NAMESPACE}"
  # create --dry-run | apply makes this idempotent (create-or-update in place).
  "${KUBECTL}" create secret generic "${name}" \
    --namespace "${NAMESPACE}" \
    "${args[@]}" \
    --dry-run=client -o yaml | "${KUBECTL}" apply -f -
}

# --- 1. build the image ----------------------------------------------------
log "building image ${IMG}"
make docker-build IMG="${IMG}"

# --- 2. make the image reachable by the cluster ----------------------------
if [[ -n "${KIND_CLUSTER}" ]]; then
  command -v kind >/dev/null 2>&1 || die "KIND_CLUSTER=${KIND_CLUSTER} set but 'kind' not found on PATH"
  log "loading ${IMG} into Kind cluster ${KIND_CLUSTER}"
  kind load docker-image "${IMG}" --name "${KIND_CLUSTER}"
else
  log "pushing ${IMG} (set KIND_CLUSTER to load into Kind instead)"
  make docker-push IMG="${IMG}"
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
