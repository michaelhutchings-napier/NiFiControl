#!/usr/bin/env bash
# End-to-end test that spec.pod.terminationGracePeriodSeconds gives NiFi a real graceful
# shutdown window — and that the stock apache/nifi image actually honors SIGTERM and exits
# WITHIN that window rather than being force-killed (SIGKILL) at the deadline:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision a single-node NiFiCluster with an explicit terminationGracePeriodSeconds and
#      wait for it to become Ready,
#   3. assert the generated StatefulSet pod template carries that grace period,
#   4. delete the node pod and prove NiFi actually shuts down gracefully. NiFi writes its
#      shutdown sequence to logs/nifi-app.log (not stdout), so we stream that file during
#      termination and assert SIGTERM triggered the graceful flow-controller shutdown and it
#      completed cleanly ("Initiated graceful shutdown of flow controller" + "Application
#      shutdown completed") — and that the pod was removed before the grace-period deadline
#      (i.e. NiFi exited on its own, it was not SIGKILLed). Timing alone cannot tell a
#      graceful stop from an abrupt SIGTERM-exit; the log markers are the real proof.
#
# Requires kind, kubectl, docker, go, helm.
# Run: make integration-graceterm-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-graceterm}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-graceterm"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
grace=90          # terminationGracePeriodSeconds we set on the cluster
graceful_max=80   # a graceful shutdown must remove the pod before this (< grace => not SIGKILLed)

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster term --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster term --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster term -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get events --sort-by=.lastTimestamp >&2; exit 1; }
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi
docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1 && kind load docker-image apache/nifi:2.10.0 --name "${cluster}" >/dev/null 2>&1 || true

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

echo "Provisioning a single-node NiFiCluster with terminationGracePeriodSeconds=${grace}..."
kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: term}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  pod:
    terminationGracePeriodSeconds: ${grace}
YAML
wait_cluster_ready
echo "  cluster is Ready."

echo "Phase 1: the generated StatefulSet pod template carries the grace period..."
sts_grace="$(kubectl --context "${ctx}" -n "${namespace}" get statefulset term-nifi -o jsonpath='{.spec.template.spec.terminationGracePeriodSeconds}')"
[ "${sts_grace}" = "${grace}" ] || { echo "StatefulSet terminationGracePeriodSeconds=${sts_grace}, want ${grace}" >&2; exit 1; }
echo "  StatefulSet pod template terminationGracePeriodSeconds=${sts_grace}."

echo "Phase 2: NiFi performs a graceful shutdown on SIGTERM within the grace period..."
# NiFi writes its shutdown sequence to logs/nifi-app.log (a file), NOT the container stdout,
# so timing alone cannot distinguish a graceful stop from an abrupt SIGTERM-exit. Stream the
# new nifi-app.log lines and assert NiFi actually ran its graceful flow-controller shutdown
# and completed cleanly (not force-killed mid-shutdown).
sf="$(mktemp)"
kubectl --context "${ctx}" -n "${namespace}" exec term-nifi-0 -c nifi -- tail -f -n 0 logs/nifi-app.log > "${sf}" 2>/dev/null &
tailpid=$!
sleep 3   # let the log stream attach before we send SIGTERM
start="$(date +%s)"
# --wait blocks until this specific pod object is fully removed.
kubectl --context "${ctx}" -n "${namespace}" delete pod term-nifi-0 --wait=true >/dev/null
end="$(date +%s)"
kill "${tailpid}" >/dev/null 2>&1 || true
duration=$(( end - start ))
echo "  pod termination took ${duration}s (grace period ${grace}s)."

# Force-kill at the deadline would mean duration ~= grace period with no clean completion.
if [ "${duration}" -ge "${graceful_max}" ]; then
  echo "termination took ${duration}s (>= ${graceful_max}s): NiFi was force-killed at the grace-period deadline, not graceful." >&2
  echo "---- captured nifi-app.log ----" >&2; tail -n 40 "${sf}" >&2 || true; rm -f "${sf}"; exit 1
fi

# The real proof: SIGTERM triggered NiFi's graceful flow-controller shutdown, and it finished.
if ! grep -q "Initiated graceful shutdown of flow controller" "${sf}"; then
  echo "no graceful flow-controller shutdown in nifi-app.log: SIGTERM did not trigger a graceful stop." >&2
  echo "---- captured nifi-app.log ----" >&2; tail -n 40 "${sf}" >&2 || true; rm -f "${sf}"; exit 1
fi
if ! grep -q "Application shutdown completed" "${sf}"; then
  echo "NiFi did not finish shutting down cleanly (no 'Application shutdown completed'): killed mid-shutdown." >&2
  echo "---- captured nifi-app.log ----" >&2; tail -n 40 "${sf}" >&2 || true; rm -f "${sf}"; exit 1
fi
echo "  graceful shutdown confirmed in nifi-app.log:"
grep -E "checkpointed FlowFile Repository|Initiated graceful shutdown of flow controller|Controller has been terminated successfully|Application shutdown completed" "${sf}" | sed 's/^/    /'
rm -f "${sf}"

# The StatefulSet recreates the pod (proving the delete was a real pod-level termination, not
# a cluster teardown) and it comes back to Ready.
wait_cluster_ready
echo "  StatefulSet recreated the pod and the cluster is Ready again."

echo "PASS: terminationGracePeriodSeconds gives NiFi a real graceful-shutdown window and NiFi honors SIGTERM within it."
