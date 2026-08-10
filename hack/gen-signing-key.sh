#!/usr/bin/env bash
#
# gen-signing-key.sh — provision the SandD daemon-JWT Ed25519 signing key.
#
# The manager is the SOLE signer of daemon JWTs for the SandD dial-out control
# plane (it signs in-process via pkg/sandd; there is no separate keybroker). It
# needs an Ed25519 private key, mounted into the MANAGER pod (and NO other workload)
# as the sandd-signing-key Secret, under signing.pem.
#
# ONLY the private half is provisioned, and that is a deliberate simplification: the
# verifier is the SandD controller, which now runs INSIDE the manager process, so it
# derives the public half from this same key at startup (see setupSandD). There is no
# public-key ConfigMap to keep in sync — which removes the failure that used to take the
# whole fleet down at once, a published public key that had drifted from the private one so
# every daemon failed to authenticate together.
#
# The private key is written only into the Secret and a temp file deleted on exit — it is
# never printed and never left on disk. The public key IS printed, for out-of-cluster
# verifiers and for checking by hand which key a cluster is signing with; it is not a
# secret, and that asymmetry is the whole point of signing rather than sharing one.
#
# This script only creates the KEY. It does not enable the feature: SandD stays off
# until SANDD_SIGNING_KEY_PATH is set on the manager (see config/manager/manager.yaml),
# which is what gates daemon-token minting. The full enable is three steps:
#
#   1. hack/gen-signing-key.sh                   # create the key Secret (this script)
#   2. kubectl apply -f config/sandd/ingress.yaml -n "${NAMESPACE}"   # edit the host first
#   3. uncomment `- ../sandd` in config/default/kustomization.yaml (the Service fronting
#      the manager's dial-in port) and SANDD_SIGNING_KEY_PATH + SANDD_EXTERNAL_HOST in
#      config/manager/manager.yaml; make deploy
#
# The KEY comes first, though not because the manager's volume is required — it is
# optional, so the manager schedules fine either way. It matters because with the env var
# set and no key the manager exits at startup by design: instances whose daemons cannot
# dial in are worse than instances that never launched. Creating the key first avoids
# that window. The edge's position is only a convenience (its DNS/LB/cert work overlaps
# the install); an Ingress with no backend yet is admitted and simply serves 503.
#
# Usage:
#   hack/gen-signing-key.sh                   # ensure the key exists; print public key
#   FORCE_REGEN=true hack/gen-signing-key.sh  # rotate the key (see NOTE below)
#
# Config (env / make flags):
#   NAMESPACE    namespace the manager runs in     (default nebula-system)
#   SECRET       private-key Secret name           (default sandd-signing-key)
#   PUB_OUT      file to also write the public key  (default: stdout only)
#   FORCE_REGEN  if "true", regenerate even if the Secret exists (default false)
#   KUBECTL      kubectl binary                    (default kubectl)
#
# NOTE: rotating (FORCE_REGEN=true) mints tokens under a NEW key, and the verifier picks it
# up when the manager restarts — so tokens signed by the old key stop verifying then. Live
# daemons hold tokens with a TTL (default 24h), so a rotation is a hard cutover for
# anything already connected: expect reconnects. The token header does carry `kid`, and the
# controller checks it before the signature so a mismatch is distinguishable from a forgery
# in the logs — but it holds exactly ONE key, so overlapping old and new is not yet
# possible. Bump SANDD_SIGNING_KID when you rotate, so the logs name the real problem.
set -euo pipefail

NAMESPACE="${NAMESPACE:-nebula-system}"
SECRET="${SECRET:-sandd-signing-key}"
PUB_OUT="${PUB_OUT:-}"
FORCE_REGEN="${FORCE_REGEN:-false}"
KUBECTL="${KUBECTL:-kubectl}"

log()  { printf '\033[36m==>\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found on PATH"
command -v openssl >/dev/null 2>&1 || die "openssl not found on PATH"

# emit_public_key — derive the public key from a private PEM and write it to stdout (and
# PUB_OUT if set). Nothing in the cluster consumes this: the manager derives its own copy.
# It is printed because "which key is this cluster signing with" is otherwise unanswerable
# without reading the Secret.
emit_public_key() {
  local priv="$1" pub
  pub="$(openssl pkey -in "${priv}" -pubout)"

  if [[ -n "${PUB_OUT}" ]]; then
    printf '%s\n' "${pub}" > "${PUB_OUT}"
    log "wrote public key to ${PUB_OUT}"
  fi
  printf '%s\n' "${pub}"
}

# If the Secret already exists and we're not rotating, leave it untouched (so
# re-runs don't invalidate live tokens) and just re-emit its public key.
if [[ "${FORCE_REGEN}" != "true" ]] && \
   "${KUBECTL}" get secret "${SECRET}" -n "${NAMESPACE}" >/dev/null 2>&1; then
  log "Secret ${SECRET} already exists in ${NAMESPACE}; keeping it (FORCE_REGEN=true to rotate)"
  tmp="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '${tmp}'" EXIT
  "${KUBECTL}" get secret "${SECRET}" -n "${NAMESPACE}" \
    -o jsonpath='{.data.signing\.pem}' | base64 -d > "${tmp}/signing.pem" \
    || die "Secret ${SECRET} has no signing.pem"
  emit_public_key "${tmp}/signing.pem"
  exit 0
fi

tmp="$(mktemp -d)"
# shellcheck disable=SC2064
trap "rm -rf '${tmp}'" EXIT

log "generating Ed25519 signing key"
# PKCS#8 PEM is the form pkg/sandd.NewSigner parses.
openssl genpkey -algorithm ed25519 -out "${tmp}/signing.pem" >/dev/null 2>&1

log "applying Secret ${SECRET} in ${NAMESPACE}"
# create --dry-run | apply makes this idempotent (create-or-update in place).
"${KUBECTL}" create secret generic "${SECRET}" \
  --namespace "${NAMESPACE}" \
  --from-file=signing.pem="${tmp}/signing.pem" \
  --dry-run=client -o yaml | "${KUBECTL}" apply -f -

log "signing public key:"
emit_public_key "${tmp}/signing.pem"
