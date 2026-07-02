#!/usr/bin/env bash
# End-to-end test of NiFiReportingTask against a real Apache NiFi 2.10 in kind. Reporting tasks
# are controller-level components that the operator must create/start/stop over NiFi's REST API,
# so (like the authz harness) the operator runs IN-cluster where it can reach NiFi via cluster
# DNS. An insecure NiFi is used so the harness can read the reporting task's run state directly:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster,
#   3. create a Disabled NiFiReportingTask -> operator creates it in NiFi, STOPPED,
#   4. flip it to Enabled -> operator starts it via the run-status endpoint (RUNNING),
#   5. flip it back to Disabled -> operator stops it (STOPPED),
#   6. delete the CR -> operator stops and deletes it in NiFi,
#   7. deletion-deadlock check: with a reporting task still present, delete the CLUSTER first and
#      confirm the reporting task's finalizer clears instead of blocking forever.
#
# Requires kind, kubectl, docker, go, helm. Reuses the NiFi image cached on the node. Run:
#   make integration-reportingtask-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-authz}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-rt"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
rt_type="org.apache.nifi.controller.MonitorMemory"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifireportingtask --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster rt --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster rt --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# rt_state <name> -> prints the reporting task's run state as seen by NiFi (via the pod).
rt_state() {
  local id="$1"
  kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
    curl -fsS "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/reporting-tasks/${id}" 2>/dev/null \
    | grep -o '"state":"[A-Z]*"' | head -1 | cut -d'"' -f4
}

rt_diag() {
  local id="$1"
  echo "==== reporting task ${id} as seen by NiFi ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
    curl -fsS "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/reporting-tasks/${id}" 2>&1 >&2 || true
  echo "==== NiFiReportingTask status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask -o wide 2>&1 >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
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

echo "Deploying the operator (helm) with the freshly built image..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
# Same tag as a prior run means the pod spec is unchanged; force the new node image in.
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Provisioning an insecure single-node NiFiCluster..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: rt}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
  deletionPolicy: Delete
YAML

echo "Waiting for the NiFiCluster to become Ready..."
ready=0
for _ in $(seq 1 90); do
  if [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster rt -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ]; then
    ready=1; break
  fi
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

echo "Phase 1: create a Disabled reporting task -> created in NiFi, STOPPED..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiReportingTask
metadata: {name: mem}
spec:
  clusterRef: {name: rt}
  type: ${rt_type}
  scheduling: {strategy: TIMER_DRIVEN, period: 60 sec}
  properties:
    "Memory Pool": "G1 Old Gen"
    "Usage Threshold": "90%"
  state: Disabled
  deletionPolicy: Delete
YAML

id=""
for _ in $(seq 1 30); do
  id="$(kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask mem -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask mem -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ -n "${id}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ -n "${id}" ] && [ "${ready}" = "true" ] || { echo "reporting task was not created/ready" >&2; rt_diag "${id}"; exit 1; }
state="$(rt_state "${id}")"
echo "  created reporting task ${id}; NiFi state=${state}"
[ "${state}" = "STOPPED" ] || { echo "expected STOPPED, got '${state}'" >&2; rt_diag "${id}"; exit 1; }

echo "Phase 2: flip to Enabled -> started (RUNNING) via run-status..."
kubectl --context "${ctx}" -n "${namespace}" patch nifireportingtask mem --type=merge -p '{"spec":{"state":"Enabled"}}' >/dev/null
running=0
for _ in $(seq 1 24); do
  [ "$(rt_state "${id}")" = "RUNNING" ] && { running=1; break; }
  sleep 5
done
[ "${running}" = "1" ] || { echo "reporting task did not reach RUNNING" >&2; rt_diag "${id}"; exit 1; }
echo "  reporting task ${id} is RUNNING."

echo "Phase 3: flip to Disabled -> stopped (STOPPED)..."
kubectl --context "${ctx}" -n "${namespace}" patch nifireportingtask mem --type=merge -p '{"spec":{"state":"Disabled"}}' >/dev/null
stopped=0
for _ in $(seq 1 24); do
  [ "$(rt_state "${id}")" = "STOPPED" ] && { stopped=1; break; }
  sleep 5
done
[ "${stopped}" = "1" ] || { echo "reporting task did not return to STOPPED" >&2; rt_diag "${id}"; exit 1; }
echo "  reporting task ${id} is STOPPED."

echo "Phase 4: delete the CR -> removed from NiFi..."
kubectl --context "${ctx}" -n "${namespace}" delete nifireportingtask mem --wait=true --timeout=60s >/dev/null
gone=0
for _ in $(seq 1 12); do
  code="$(kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
    curl -s -o /dev/null -w '%{http_code}' "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/reporting-tasks/${id}" 2>/dev/null || true)"
  [ "${code}" = "404" ] && { gone=1; break; }
  sleep 5
done
[ "${gone}" = "1" ] || { echo "reporting task was not deleted from NiFi" >&2; rt_diag "${id}"; exit 1; }
echo "  reporting task ${id} removed from NiFi."

echo "Phase 5: deletion-deadlock check (delete the cluster while a reporting task exists)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiReportingTask
metadata: {name: mem2}
spec:
  clusterRef: {name: rt}
  type: ${rt_type}
  scheduling: {strategy: TIMER_DRIVEN, period: 60 sec}
  properties: {"Memory Pool": "G1 Old Gen", "Usage Threshold": "90%"}
  state: Disabled
  deletionPolicy: Delete
YAML
for _ in $(seq 1 20); do
  [ -n "$(kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask mem2 -o jsonpath='{.status.nifiId}' 2>/dev/null || true)" ] && break
  sleep 5
done
# Delete the cluster first; the reporting task references it with deletionPolicy: Delete.
kubectl --context "${ctx}" -n "${namespace}" delete nificluster rt --wait=false >/dev/null
# The reporting task must not hang forever waiting for a cluster that is gone.
kubectl --context "${ctx}" -n "${namespace}" delete nifireportingtask mem2 --wait=false >/dev/null
freed=0
for _ in $(seq 1 24); do
  kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask mem2 >/dev/null 2>&1 || { freed=1; break; }
  sleep 5
done
[ "${freed}" = "1" ] || { echo "reporting task finalizer deadlocked after cluster deletion" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifireportingtask mem2 -o yaml >&2; exit 1; }
echo "  reporting task finalizer cleared after the cluster was deleted (no deadlock)."

echo "PASS: NiFiReportingTask create/start/stop/delete works against real NiFi, and the deletion finalizer does not deadlock when the cluster is gone."
