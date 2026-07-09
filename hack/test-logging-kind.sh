#!/usr/bin/env bash
# End-to-end test of spec.logging against a real Apache NiFi 2.10 in kind. spec.logging is a
# convenience that renders conf/logback.xml by overlaying settings onto NiFi's default logback,
# flowing through the same override plumbing as configOverrides.logbackXml. The risks this
# catches are that the render is not just present on disk but actually *drives NiFi's logging*,
# and that NiFi's default noise suppression survives the overlay:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster with spec.logging: root DEBUG, a custom
#      logger, console on, and tightened nifi-app.log retention. Confirm:
#        a. the rendered logback.xml carries the root level, the custom logger, the CONSOLE
#           appender on the APP_FILE root, and the retention sizes scoped to the APP_FILE
#           appender only (DEPRECATION_FILE keeps its own smaller defaults),
#        b. the overlay preserved the baseline: the deprecation appender and the Spring
#           noise-suppression logger are still present,
#        c. it takes effect at runtime: nifi-app.log contains DEBUG lines (root DEBUG is live)
#           while Spring stays suppressed (the preserved pin holds even at root DEBUG), and the
#           application log reaches the container's stdout (console is live),
#   3. admission: setting both spec.logging and configOverrides.logbackXml is rejected (they
#      both render conf/logback.xml),
#   4. remove spec.logging -> the image's shipped logback.xml is restored on the next rollout
#      and the overrides Secret is cleaned up.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-logging-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-logging}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-logging"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
logdir="/opt/nifi/nifi-current/logs"
conf="/opt/nifi/nifi-current/conf"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster lg --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster lg --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# logback_grep <pattern>: succeeds when the node's rendered conf/logback.xml matches.
logback_grep() {
  kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
    grep -- "${1}" "${conf}/logback.xml" >/dev/null 2>&1
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster lg -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get nificluster lg -o yaml >&2; exit 1; }
}

sts_checksum() {
  kubectl --context "${ctx}" -n "${namespace}" get statefulset lg-nifi \
    -o "go-template={{index .spec.template.metadata.annotations \"nifi.controlnifi.io/config-overrides-checksum\"}}" 2>/dev/null || true
}

wait_rollout() {
  local previous="$1" changed=0
  for _ in $(seq 1 60); do
    [ "$(sts_checksum)" != "${previous}" ] && { changed=1; break; }
    sleep 5
  done
  [ "${changed}" = "1" ] || { echo "StatefulSet pod template never picked up the new overrides checksum" >&2; exit 1; }
  kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/lg-nifi --timeout=600s >/dev/null
  wait_cluster_ready
}

# wait_for_log <pattern>: wait until nifi-app.log matches (the app is Ready before it has
# necessarily written much, so poll rather than assert once).
wait_for_log() {
  for _ in $(seq 1 30); do
    kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
      grep -qE -- "${1}" "${logdir}/nifi-app.log" 2>/dev/null && return 0
    sleep 5
  done
  return 1
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

echo "Building the operator image and loading it into kind..."
( cd "${repo_root}" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/nificontrol-manager ./cmd/manager )
build_dir="$(mktemp -d)"
cp /tmp/nificontrol-manager "${build_dir}/manager"
cat > "${build_dir}/Dockerfile" <<'DOCKER'
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
DOCKER
docker build -t "${image}" "${build_dir}" >/dev/null
rm -rf "${build_dir}"
kind load docker-image "${image}" --name "${cluster}"

echo "Applying the current CRDs..."
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

echo "Deploying the operator (helm) with the freshly built image..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: cluster with spec.logging (root DEBUG, custom logger, console, retention)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: lg}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: true, size: 2Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  logging:
    level: DEBUG
    console: true
    loggers:
      com.example.nificontrol.demo: TRACE
    retention:
      maxFileSize: 20MB
      maxHistory: 3
      totalSizeCap: 200MB
YAML
wait_cluster_ready

echo "  2a: rendered logback.xml carries the settings..."
# Both root blocks at DEBUG.
[ "$(kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
  grep -c '<root level="DEBUG">' "${conf}/logback.xml")" = "2" ] || { echo "root blocks not both at DEBUG" >&2; exit 1; }
logback_grep '<logger name="com.example.nificontrol.demo" level="TRACE"/>' || { echo "custom logger not injected" >&2; exit 1; }
# CONSOLE appended to the APP_FILE root block: the root's APP_FILE ref line (with a trailing
# space before />, unlike the logger appender-refs) is immediately followed by the CONSOLE ref.
kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
  bash -c "grep -A1 'ref=\"APP_FILE\" />' '${conf}/logback.xml' | grep -q 'ref=\"CONSOLE\"'" \
  || { echo "CONSOLE not attached to the APP_FILE root" >&2; exit 1; }
logback_grep '<maxFileSize>20MB</maxFileSize>' || { echo "APP_FILE maxFileSize not applied" >&2; exit 1; }
logback_grep '<totalSizeCap>200MB</totalSizeCap>' || { echo "APP_FILE totalSizeCap not applied" >&2; exit 1; }
echo "    rendered settings present."

echo "  2b: the overlay preserved NiFi's baseline (deprecation appender, Spring suppressor)..."
logback_grep '<appender name="DEPRECATION_FILE"' || { echo "deprecation appender lost" >&2; exit 1; }
logback_grep '<logger name="org.springframework" level="ERROR"/>' || { echo "Spring noise-suppression logger lost" >&2; exit 1; }
# Retention is scoped: DEPRECATION_FILE keeps its own 10MB default, untouched by our 20MB.
kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
  bash -c "awk '/<appender name=\"DEPRECATION_FILE\"/,/<\/appender>/' '${conf}/logback.xml' | grep -q '<maxFileSize>10MB</maxFileSize>'" \
  || { echo "retention leaked into DEPRECATION_FILE" >&2; exit 1; }
echo "    baseline preserved; retention scoped to APP_FILE."

echo "  2c: it drives NiFi's logging at runtime..."
# root DEBUG is live: nifi-app.log carries DEBUG lines (they come from loggers not pinned by the
# baseline; org.apache.nifi itself stays INFO by NiFi's own default pin, which is the point of
# preserving the baseline).
wait_for_log ' DEBUG ' || { echo "no DEBUG lines in nifi-app.log; root DEBUG did not take effect" >&2; exit 1; }
# Spring stays suppressed even with the root at DEBUG (the preserved ERROR pin holds).
if kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
  grep -qE ' (DEBUG|INFO) .*org\.springframework' "${logdir}/nifi-app.log" 2>/dev/null; then
  echo "Spring emitted DEBUG/INFO at root DEBUG; the suppressor was not preserved" >&2; exit 1
fi
# console is live: application log lines from non-bootstrap loggers reach the container stdout.
# NiFi logs 100+ org.apache.nifi.nar.* INFO lines while unpacking NARs at startup; those reach
# stdout only via the root's CONSOLE appender (the separate bootstrap.Command logger aside), so
# their presence proves console. Search the whole log, not a --tail window: the NAR lines are at
# startup and would scroll out of a fixed tail once the node has logged enough at DEBUG.
console_live=0
for _ in $(seq 1 24); do
  # grep -c (not -q): -q exits on the first match and closes the pipe, so `kubectl logs`
  # dies with SIGPIPE and `set -o pipefail` propagates 141 — defeating the match. -c reads
  # to EOF, so kubectl finishes cleanly. || true keeps a zero count from tripping set -e.
  n="$(kubectl --context "${ctx}" -n "${namespace}" logs lg-nifi-0 -c nifi 2>/dev/null | grep -cE ' INFO .*org\.apache\.nifi\.nar' || true)"
  [ "${n:-0}" -gt 0 ] && { console_live=1; break; }
  sleep 5
done
[ "${console_live}" = "1" ] || { echo "application logs did not reach stdout; console not live" >&2; exit 1; }
echo "    root DEBUG live, Spring suppressed, console live."

echo "Phase 3: setting both spec.logging and configOverrides.logbackXml is rejected..."
if kubectl --context "${ctx}" -n "${namespace}" patch nificluster lg --type=merge \
  -p '{"spec":{"configOverrides":{"logbackXml":"<configuration><root level=\"WARN\"/></configuration>"}}}' >/dev/null 2>&1; then
  echo "expected the CEL rule to reject spec.logging + configOverrides.logbackXml together" >&2; exit 1
fi
echo "  conflicting logback sources rejected."

echo "Phase 4: removing spec.logging restores the image's shipped logback.xml..."
checksum_before="$(sts_checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster lg --type=merge \
  -p '{"spec":{"logging":null}}' >/dev/null
wait_rollout "${checksum_before}"
kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- \
  cmp -s "${conf}/logback.xml" "${conf}/logback.xml.image-default" || { echo "logback.xml not restored to the image default" >&2; exit 1; }
if kubectl --context "${ctx}" -n "${namespace}" exec lg-nifi-0 -c nifi -- test -f "${conf}/.nificontrol-logback-overridden" 2>/dev/null; then
  echo "logback override marker survived removal" >&2; exit 1
fi
if kubectl --context "${ctx}" -n "${namespace}" get secret lg-nifi-config-overrides >/dev/null 2>&1; then
  echo "overrides Secret not cleaned up" >&2; exit 1
fi
echo "  image logback.xml restored; overrides Secret cleaned up."

echo "PASS: spec.logging rendered, drove NiFi's logging, preserved the baseline, was guarded, and cleanly removed."
