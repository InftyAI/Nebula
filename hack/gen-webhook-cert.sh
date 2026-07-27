#!/usr/bin/env bash
#
# gen-webhook-cert.sh — provision the webhook serving cert WITHOUT cert-manager.
#
# The webhook needs two things cert-manager would otherwise automate:
#   1. a TLS Secret (webhook-server-cert) the manager mounts and serves from;
#   2. that cert's CA injected into the MutatingWebhookConfiguration caBundle,
#      so the API server trusts the webhook when it calls it.
#
# These two steps have DIFFERENT ordering requirements, so this script exposes
# them as separate actions (see usage) and a running manager never needs a
# restart:
#   - the Secret must exist BEFORE the manager pod starts (it is a required
#     volume mount), so create it before `make deploy`;
#   - the caBundle patch is server-side (only the API server reads it), so it
#     runs AFTER `make deploy` creates the webhook config. The manager is
#     untouched by it.
#
# The caBundle is always derived from the cert already in the Secret, so the
# served cert and the trusted CA can never drift, even across re-runs.
#
# Usage:
#   hack/gen-webhook-cert.sh secret     # ensure the TLS Secret exists (generate if absent)
#   hack/gen-webhook-cert.sh cabundle   # inject the Secret's cert into the webhook config
#   hack/gen-webhook-cert.sh all        # both, in order (default; standalone use)
#
# Config (env / make flags):
#   NAMESPACE       namespace the manager runs in     (default nebula-system)
#   SERVICE         webhook Service name              (default nebula-webhook-service)
#   SECRET          TLS Secret the manager mounts     (default webhook-server-cert)
#   WEBHOOK_CONFIG  MutatingWebhookConfiguration name (default nebula-mutating-webhook-configuration)
#   CERT_DAYS       certificate validity in days      (default 3650)
#   FORCE_REGEN     if "true", regenerate even if the Secret exists (default false)
#   KUBECTL         kubectl binary                    (default kubectl)
#
# NOTE: the self-signed cert does NOT auto-rotate (that is cert-manager's main
# advantage). It is valid CERT_DAYS days; re-run with FORCE_REGEN=true to renew.
set -euo pipefail

NAMESPACE="${NAMESPACE:-nebula-system}"
SERVICE="${SERVICE:-nebula-webhook-service}"
SECRET="${SECRET:-webhook-server-cert}"
WEBHOOK_CONFIG="${WEBHOOK_CONFIG:-nebula-mutating-webhook-configuration}"
CERT_DAYS="${CERT_DAYS:-3650}"
FORCE_REGEN="${FORCE_REGEN:-false}"
KUBECTL="${KUBECTL:-kubectl}"
ACTION="${1:-all}"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
die()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found on PATH"

CN="${SERVICE}.${NAMESPACE}.svc"

# ensure_secret — create the TLS Secret if absent (or if FORCE_REGEN=true).
# Leaves an existing Secret untouched so re-runs don't needlessly rotate the
# cert (which would otherwise require remounting on the manager).
ensure_secret() {
  if [[ "${FORCE_REGEN}" != "true" ]] && \
     "${KUBECTL}" get secret "${SECRET}" -n "${NAMESPACE}" >/dev/null 2>&1; then
    log "Secret ${SECRET} already exists in ${NAMESPACE}; keeping it (FORCE_REGEN=true to rotate)"
    return 0
  fi

  command -v openssl >/dev/null 2>&1 || die "openssl not found on PATH"

  local tmp
  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmp}'" RETURN

  log "generating self-signed cert for ${CN} (valid ${CERT_DAYS}d)"
  # The cert is its own CA: it both serves TLS and is trusted via caBundle.
  # SANs cover both DNS forms the webhook may be addressed by.
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "${tmp}/tls.key" -out "${tmp}/tls.crt" \
    -days "${CERT_DAYS}" -subj "/CN=${CN}" \
    -addext "subjectAltName=DNS:${CN},DNS:${CN}.cluster.local" >/dev/null 2>&1

  log "applying TLS Secret ${SECRET} in ${NAMESPACE}"
  "${KUBECTL}" create secret tls "${SECRET}" \
    --cert="${tmp}/tls.crt" --key="${tmp}/tls.key" \
    --namespace "${NAMESPACE}" \
    --dry-run=client -o yaml | "${KUBECTL}" apply -f -
}

# inject_cabundle — read tls.crt from the Secret and set it as the webhook's
# caBundle. Deriving from the Secret guarantees the trusted CA matches the
# served cert. Requires the MutatingWebhookConfiguration to already exist.
inject_cabundle() {
  "${KUBECTL}" get secret "${SECRET}" -n "${NAMESPACE}" >/dev/null 2>&1 \
    || die "Secret ${SECRET} not found in ${NAMESPACE}; run '$0 secret' first"
  "${KUBECTL}" get mutatingwebhookconfiguration "${WEBHOOK_CONFIG}" >/dev/null 2>&1 \
    || die "${WEBHOOK_CONFIG} not found; deploy the manager (make deploy) before injecting the CA"

  # tls.crt in the Secret is already base64-encoded, which is exactly the form
  # caBundle wants — no re-encoding needed.
  local ca_b64
  ca_b64="$("${KUBECTL}" get secret "${SECRET}" -n "${NAMESPACE}" -o jsonpath='{.data.tls\.crt}')"
  [[ -n "${ca_b64}" ]] || die "Secret ${SECRET} has no tls.crt"

  log "injecting CA bundle into ${WEBHOOK_CONFIG}"
  # JSON Patch "add" on an existing member replaces it, so this is correct on
  # both first run (caBundle absent) and re-runs (caBundle present).
  "${KUBECTL}" patch mutatingwebhookconfiguration "${WEBHOOK_CONFIG}" \
    --type=json \
    -p="[{\"op\":\"add\",\"path\":\"/webhooks/0/clientConfig/caBundle\",\"value\":\"${ca_b64}\"}]"
}

case "${ACTION}" in
  secret)   ensure_secret ;;
  cabundle) inject_cabundle ;;
  all)      ensure_secret; inject_cabundle ;;
  *)        die "unknown action '${ACTION}' (want: secret | cabundle | all)" ;;
esac
