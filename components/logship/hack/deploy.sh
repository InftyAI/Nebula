#!/usr/bin/env bash
#
# deploy.sh — run logship in the cluster.
#
# One deployment for the whole cluster, and it finds its own work: it lists Pods labelled
# nebula.inftyai.com/enabled=true, reads which provider each was placed on off its nodeSelector, and
# ships its instance through that one. No instance, pod or labels to pass — see "the metadata is the
# product". The Modal secret below is one provider's; it is only read if the cluster has Modal Pods.
#
# Usage:
#   hack/deploy.sh                                        # or: make deploy
#   make docker-build docker-push deploy IMG=inftyai/nebula-logship:0906-01
#
# Inputs (environment or make flags):
#   IMG         image to run          (default inftyai/nebula-logship:latest)
#   DEPLOY_NS   namespace to run in   (default nebula-system)
#   NAME        Pod name              (default logship)
#   FOLLOW      0 to skip tailing the Pod's output afterwards
#
# A Deployment, so a drained or lost node reschedules instead of ending collection for the
# rest of the run — silently, since nothing downstream can tell "no logs" from "no output".
#
# But strictly ONE reader, enforced twice: replicas 1, and strategy Recreate. RollingUpdate
# is the trap — its default maxSurge starts the new pod before the old one goes, and for the
# seconds they overlap both read the same instance from the same cursor and every line is
# shipped, stored and billed twice. There is no leader election to fall back on.
#
# Restarts DO replay until the checkpoint lands (phase 3): a fresh process resumes from the
# cursor it was given, which is the beginning, so the provider re-sends the whole history.
# In-process this is already bounded — supervise caps restarts at DefaultMaxRestarts for
# exactly this reason — but a container restart escapes that bound, and CrashLoopBackOff is
# then a billing loop, not just an outage. Watch RESTARTS on a long run.
#
# Why the deploy namespace matters: with the stdout handoff, logship's records reach
# CloudWatch through the log agent of the cluster it runs in. The default is nebula-system
# because that is where Nebula's manager already runs in the dev cluster, and the
# manager's own container log is a live stream in the group the consumer reads — so collection
# there is verified, not assumed. It is also where nebula-modal-credentials lives, and a
# Secret cannot be referenced across namespaces. Moving this Pod elsewhere means checking
# both again.
#
# The metadata is the product: logship stamps the instance Pod's name and labels onto each
# record because the log agent stamps ITS OWN identity, not the instance's. The consumer then
# filters on the app label, the consumer's org/team/experiment IDs and pod_name, so a record
# shipped with the wrong ones is delivered, charged for, and invisible in the UI — no error
# anywhere, just an empty log view. Which is why none of it can be passed in here: it is read
# off the Pod that owns the instance, one object, so the parts cannot disagree.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log()  { printf '\033[36m==>\033[0m %s\n' "$*"; }
die()  { printf '\033[31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }

NAME="${NAME:-logship}"
IMG="${IMG:-inftyai/nebula-logship:latest}"
DEPLOY_NS="${DEPLOY_NS:-nebula-system}"
MODAL_SECRET="${MODAL_SECRET:-nebula-modal-credentials}"
FOLLOW="${FOLLOW:-1}"
KUBECTL="${KUBECTL:-kubectl}"

command -v "${KUBECTL}" >/dev/null 2>&1 || die "kubectl not found on PATH"

# Unreachable and absent are separated on purpose: this cluster is reached through an SSM
# tunnel, and when it is down every kubectl fails as "not found", which reads like a missing
# namespace and sends you looking in the wrong place.
"${KUBECTL}" version --request-timeout=10s >/dev/null 2>&1 \
  || die "cannot reach the cluster ($("${KUBECTL}" config current-context 2>/dev/null || echo 'no context')) — tunnel down?"
"${KUBECTL}" get namespace "${DEPLOY_NS}" >/dev/null 2>&1 \
  || die "namespace ${DEPLOY_NS} not found — is this the cluster whose agent ships to the group the consumer reads?"
# A warning rather than fatal: a provider is only opened when a Pod on it arrives, so a cluster with
# no Modal Pods needs no Modal tokens. If some do arrive, the process says so once per Pod.
"${KUBECTL}" -n "${DEPLOY_NS}" get secret "${MODAL_SECRET}" >/dev/null 2>&1 \
  || log "WARNING: Secret ${MODAL_SECRET} not found in ${DEPLOY_NS} — Modal instances will not ship (other providers are unaffected)"

log "granting ${NAME} read access to Pods"
# Cluster-scoped because instance Pods live in one namespace per org, and the set of orgs is not
# known here. Read-only and Pods-only: the watcher writes nothing, and the drain finalizer that
# will need NodeClaims does not exist yet.
"${KUBECTL}" apply -f - <<YAML
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ${NAME}
  namespace: ${DEPLOY_NS}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ${NAME}
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ${NAME}
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ${NAME}
subjects:
  - kind: ServiceAccount
    name: ${NAME}
    namespace: ${DEPLOY_NS}
YAML

# The limits are a guess, and being wrong tight costs money rather than just capacity: an OOM kill
# restarts the process, and a restart replays every instance from the cursor it was given. Nothing has
# measured per-instance memory yet; check actual usage before trimming. Modal's connection pool needs
# no sizing here — it widens itself as instances arrive, see modal.Pool.Grow.
log "deploying ${NAME} to ${DEPLOY_NS} (image ${IMG})"
"${KUBECTL}" apply -f - <<YAML
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ${NAME}
  namespace: ${DEPLOY_NS}
  labels: &labels
    app.kubernetes.io/name: logship
    app.kubernetes.io/part-of: nebula
    app.kubernetes.io/instance: ${NAME}
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app.kubernetes.io/instance: ${NAME}
  template:
    metadata:
      labels: *labels
    spec:
      # The drain has to outlive SIGTERM: on shutdown each stream flushes what it has read but
      # not yet printed. The flush is a write to stdout and takes no time; the wait is the gRPC
      # streams closing, which is what the default 30s would be spent on.
      terminationGracePeriodSeconds: 60
      serviceAccountName: ${NAME}
      containers:
        - name: logship
          image: ${IMG}
          # Records go to stdout, every diagnostic to stderr. The kubelet interleaves both into
          # the container log, so a diagnostic printed on stdout would reach CloudWatch as a
          # record no consumer can parse — which is why main.go writes logs to stderr on purpose.
          # optional, so the Pod still starts in a cluster with nothing on Modal. A non-optional
          # secretRef fails container creation outright, which would make one provider's credentials
          # a prerequisite for shipping any provider's logs.
          envFrom:
            - secretRef:
                name: ${MODAL_SECRET}
                optional: true
          resources:
            requests:
              cpu: 200m
              memory: 256Mi
            limits:
              cpu: "2"
              memory: 1Gi
          securityContext:
            allowPrivilegeEscalation: false
            readOnlyRootFilesystem: true
            runAsNonRoot: true
            runAsUser: 65532
            capabilities:
              drop: ["ALL"]
YAML

log "waiting for ${NAME} to roll out"
"${KUBECTL}" -n "${DEPLOY_NS}" rollout status "deployment/${NAME}" --timeout=120s \
  || die "rollout did not complete; ${KUBECTL} -n ${DEPLOY_NS} describe deployment ${NAME}"

# Check 0 first because "nothing shipped" is ambiguous: an empty cluster and a selector that matches
# nothing look identical from the outside.
cat >&2 <<EOF

$(log "deployed. Checks, in the order they can fail:")
  0. there is anything to ship     ${KUBECTL} get pods -A -l nebula.inftyai.com/enabled=true
  1. the watch started              ${KUBECTL} -n ${DEPLOY_NS} logs deployment/${NAME} | grep 'watching Pods'
  2. a record is well-formed       ${KUBECTL} -n ${DEPLOY_NS} logs deployment/${NAME} | grep -v '^logship:' | tail -1 | jq .
  3. the agent picked it up        aws logs filter-log-events --log-group-name <group> --filter-pattern '{ \$.log_processed.kubernetes.pod_name = "*" }'
  4. no restarts (each one replays) ${KUBECTL} -n ${DEPLOY_NS} get pods -l app.kubernetes.io/instance=${NAME}
  5. tear down                     ${KUBECTL} -n ${DEPLOY_NS} delete deployment/${NAME} clusterrolebinding/${NAME} clusterrole/${NAME} sa/${NAME}

EOF
