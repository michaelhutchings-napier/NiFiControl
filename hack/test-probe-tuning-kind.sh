#!/usr/bin/env bash
# End-to-end test that spec.pod.probes tunes the operator's startup/liveness/readiness probes
# on the real NiFi container — and that a cluster with tuned probes still becomes Ready (i.e.
# the tuning is applied to the scheduling fields only, without breaking the probe action):
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision a single-node NiFiCluster with non-default probe tuning and wait for Ready
#      (proving the tuned readiness probe still passes and the widened startup probe admits),
#   3. assert the running pod's nifi container carries the tuned scheduling fields on all
#      three probes, while the probe ACTION is unchanged (httpGet against the NiFi about
#      endpoint) — the operator owns the action; only periods/timeouts/thresholds are tunable.
#
# Requires kind, kubectl, docker, go, helm.
# Run: make integration-probetuning-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-probetuning}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-probetuning"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster probe --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster probe --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster probe -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
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

echo "Provisioning a single-node NiFiCluster with tuned probes..."
kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: probe}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  pod:
    probes:
      startup:
        periodSeconds: 15
        failureThreshold: 80
      liveness:
        periodSeconds: 30
        failureThreshold: 5
      readiness:
        periodSeconds: 15
        timeoutSeconds: 8
YAML
wait_cluster_ready
echo "  cluster is Ready (tuned readiness probe passes, widened startup probe admitted)."

echo "Phase 1: the running pod's nifi container carries the tuned scheduling fields..."
jp() { kubectl --context "${ctx}" -n "${namespace}" get pod probe-nifi-0 -o jsonpath="$1"; }
su_p="$(jp '{.spec.containers[?(@.name=="nifi")].startupProbe.periodSeconds}')"
su_f="$(jp '{.spec.containers[?(@.name=="nifi")].startupProbe.failureThreshold}')"
lv_p="$(jp '{.spec.containers[?(@.name=="nifi")].livenessProbe.periodSeconds}')"
lv_f="$(jp '{.spec.containers[?(@.name=="nifi")].livenessProbe.failureThreshold}')"
rd_p="$(jp '{.spec.containers[?(@.name=="nifi")].readinessProbe.periodSeconds}')"
rd_t="$(jp '{.spec.containers[?(@.name=="nifi")].readinessProbe.timeoutSeconds}')"
echo "  startup: period=${su_p} failureThreshold=${su_f} | liveness: period=${lv_p} failureThreshold=${lv_f} | readiness: period=${rd_p} timeout=${rd_t}"
[ "${su_p}" = "15" ] && [ "${su_f}" = "80" ] || { echo "startup probe not tuned: period=${su_p} failureThreshold=${su_f}, want 15/80" >&2; exit 1; }
[ "${lv_p}" = "30" ] && [ "${lv_f}" = "5" ]  || { echo "liveness probe not tuned: period=${lv_p} failureThreshold=${lv_f}, want 30/5" >&2; exit 1; }
[ "${rd_p}" = "15" ] && [ "${rd_t}" = "8" ]  || { echo "readiness probe not tuned: period=${rd_p} timeout=${rd_t}, want 15/8" >&2; exit 1; }
echo "  all three probes carry the tuned scheduling fields."

echo "Phase 2: the probe ACTION is still operator-managed (httpGet /nifi-api/flow/about)..."
su_path="$(jp '{.spec.containers[?(@.name=="nifi")].startupProbe.httpGet.path}')"
rd_path="$(jp '{.spec.containers[?(@.name=="nifi")].readinessProbe.httpGet.path}')"
[ "${su_path}" = "/nifi-api/flow/about" ] || { echo "startup probe action changed: httpGet.path=${su_path}" >&2; exit 1; }
[ "${rd_path}" = "/nifi-api/flow/about" ] || { echo "readiness probe action changed: httpGet.path=${rd_path}" >&2; exit 1; }
# An unset field must keep the operator default: startup timeoutSeconds was not tuned (default 3).
su_to="$(jp '{.spec.containers[?(@.name=="nifi")].startupProbe.timeoutSeconds}')"
[ "${su_to}" = "3" ] || { echo "unset startup timeout not left at default: ${su_to}, want 3" >&2; exit 1; }
echo "  probe actions unchanged and unset fields kept their defaults."

echo "PASS: spec.pod.probes tunes the scheduling fields while the operator keeps the probe actions, and NiFi still reaches Ready."
