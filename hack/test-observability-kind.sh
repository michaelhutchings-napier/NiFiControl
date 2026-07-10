#!/usr/bin/env bash
# End-to-end test of the observability milestone against a real Apache NiFi 2.10 in kind:
#
#   1. install the CRDs and run the operator out-of-cluster against kind,
#   2. provision a clustered, metrics-enabled NiFiCluster (ZooKeeper + 2 nodes),
#   3. confirm NiFi 2.x actually serves Prometheus metrics at
#      /nifi-api/flow/metrics/prometheus (nifi_amount_items_queued present),
#   4. with the Prometheus Operator CRDs absent, confirm the operator degrades gracefully
#      (MetricsReady=False, reason CRDsNotInstalled) without failing the cluster,
#   5. install the ServiceMonitor CRD and confirm the operator then renders a ServiceMonitor
#      and reports MetricsReady=True (ServiceMonitorReady).
#
# Clustered NiFi binds its pod FQDN as the web host, so metrics are verified in-cluster with
# `kubectl exec` against that FQDN (kubectl port-forward dials 127.0.0.1 and cannot reach it).
#
# Requires kind, kubectl, docker, go, and internet access (for the ServiceMonitor CRD). Run:
#   make integration-observability-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-observability}"
ctx="kind-${cluster}"
namespace="nifi-observability"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
kubeconfig="$(mktemp)"
operator_bin="$(mktemp -u)"
operator_pid=""
sm_crd="https://raw.githubusercontent.com/prometheus-operator/prometheus-operator/v0.76.0/example/prometheus-operator-crd/monitoring.coreos.com_servicemonitors.yaml"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster obs --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster obs --ignore-not-found --wait=false >/dev/null 2>&1 || true
  sleep 5
  [ -n "${operator_pid}" ] && kill "${operator_pid}" >/dev/null 2>&1 || true
  rm -f "${kubeconfig}" "${operator_bin}"
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

# Best-effort side-load of the host NiFi image to avoid an in-node pull.
if ! docker exec "${cluster}-control-plane" crictl images 2>/dev/null | grep -q 'apache/nifi'; then
  if docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1; then
    archive="$(mktemp --suffix=.tar)"
    docker save apache/nifi:2.10.0 -o "${archive}"
    kind load image-archive "${archive}" --name "${cluster}" || true
    rm -f "${archive}"
  fi
fi

echo "Installing CRDs..."
# Server-side apply: the nificlusters CRD exceeds the 262144-byte client-side last-applied
# annotation limit.
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

# Make sure the ServiceMonitor CRD is absent for phase A (it may linger from a prior run).
kubectl --context "${ctx}" delete crd servicemonitors.monitoring.coreos.com --ignore-not-found >/dev/null 2>&1 || true

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Deploying ZooKeeper..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: zookeeper, labels: {app: zookeeper}}
spec:
  replicas: 1
  selector: {matchLabels: {app: zookeeper}}
  template:
    metadata: {labels: {app: zookeeper}}
    spec:
      containers:
        - name: zookeeper
          image: zookeeper:3.9
          ports: [{containerPort: 2181, name: client}]
          readinessProbe: {tcpSocket: {port: client}, periodSeconds: 5, failureThreshold: 30}
---
apiVersion: v1
kind: Service
metadata: {name: zookeeper}
spec:
  selector: {app: zookeeper}
  ports: [{name: client, port: 2181, targetPort: client}]
YAML
kubectl --context "${ctx}" -n "${namespace}" rollout status deploy/zookeeper --timeout=120s

echo "Building and starting the operator out-of-cluster..."
( cd "${repo_root}" && go build -o "${operator_bin}" ./cmd/manager )
kind get kubeconfig --name "${cluster}" > "${kubeconfig}"
KUBECONFIG="${kubeconfig}" "${operator_bin}" --metrics-bind-address=0 --health-probe-bind-address=0 \
  > /tmp/nificontrol-observability-operator.log 2>&1 &
operator_pid=$!
echo "operator pid ${operator_pid}"

echo "Creating a clustered, metrics-enabled NiFiCluster..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: obs}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 2
  coordination:
    zookeeperConnectString: zookeeper.nifi-observability.svc:2181
    zookeeperRootNode: /obs
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 200m, memory: 1Gi}
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 30s
  deletionPolicy: Delete
YAML

echo "Waiting for the StatefulSet pods to become ready (first run also pulls the image)..."
for _ in $(seq 1 60); do
  kubectl --context "${ctx}" -n "${namespace}" get statefulset obs-nifi >/dev/null 2>&1 && break
  sleep 5
done
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/obs-nifi --timeout=900s

fqdn="obs-nifi-0.obs-nifi-headless.${namespace}.svc"
metrics_url="http://${fqdn}:8080/nifi-api/flow/metrics/prometheus"
echo "Verifying NiFi 2.x serves Prometheus metrics at /nifi-api/flow/metrics/prometheus..."
metrics_ok=0
for _ in $(seq 1 36); do
  body="$(kubectl --context "${ctx}" -n "${namespace}" exec obs-nifi-0 -c nifi -- \
    curl -fsS "${metrics_url}" 2>/dev/null || true)"
  if printf '%s' "${body}" | grep -q 'nifi_amount_items_queued'; then
    echo "  metrics endpoint serves nifi_amount_items_queued:"
    printf '%s\n' "${body}" | grep -m3 '^nifi_amount_items_queued' || true
    metrics_ok=1
    break
  fi
  sleep 5
done
if [ "${metrics_ok}" != "1" ]; then
  echo "DIAGNOSTIC: nifi_amount_items_queued not found; inspecting the endpoint..." >&2
  kubectl --context "${ctx}" -n "${namespace}" exec obs-nifi-0 -c nifi -- sh -ec "
    echo '--- HTTP status ---'
    curl -s -o /tmp/m.txt -w 'status=%{http_code} bytes=%{size_download}\n' '${metrics_url}' || true
    echo '--- first 30 lines of body ---'
    head -30 /tmp/m.txt || true
    echo '--- distinct nifi_ metric names present ---'
    grep -oE '^nifi_[a-zA-Z0-9_]+' /tmp/m.txt | sort -u | head -60 || true
    echo '--- any *flowfiles*/*queued* lines ---'
    grep -iE 'flowfile|queued' /tmp/m.txt | head -20 || true
  " >&2 || true
  echo "NiFi metrics endpoint did not serve nifi_amount_items_queued" >&2
  exit 1
fi

echo "Phase A: Prometheus Operator CRDs absent -> MetricsReady=False (CRDsNotInstalled)..."
phaseA_ok=0
for _ in $(seq 1 30); do
  reason="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster obs -o jsonpath='{.status.conditions[?(@.type=="MetricsReady")].reason}' 2>/dev/null || true)"
  if [ "${reason}" = "CRDsNotInstalled" ]; then
    echo "  MetricsReady reason=CRDsNotInstalled, and the cluster is otherwise reconciling."
    phaseA_ok=1
    break
  fi
  sleep 4
done
[ "${phaseA_ok}" = "1" ] || { echo "expected MetricsReady=CRDsNotInstalled, got '${reason:-<none>}'" >&2; exit 1; }

echo "Phase B: installing the ServiceMonitor CRD and forcing a re-reconcile..."
kubectl --context "${ctx}" apply -f "${sm_crd}" >/dev/null
kubectl --context "${ctx}" wait --for=condition=Established crd/servicemonitors.monitoring.coreos.com --timeout=60s >/dev/null
# Nudge a reconcile so the operator notices the now-present CRD.
kubectl --context "${ctx}" -n "${namespace}" annotate nificluster obs nifi.controlnifi.io/rereconcile="$(date +%s)" --overwrite >/dev/null

phaseB_ok=0
for _ in $(seq 1 30); do
  reason="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster obs -o jsonpath='{.status.conditions[?(@.type=="MetricsReady")].reason}' 2>/dev/null || true)"
  sm="$(kubectl --context "${ctx}" -n "${namespace}" get servicemonitor obs-nifi-metrics -o jsonpath='{.spec.endpoints[0].path}' 2>/dev/null || true)"
  if [ "${reason}" = "ServiceMonitorReady" ] && [ "${sm}" = "/nifi-api/flow/metrics/prometheus" ]; then
    echo "  ServiceMonitor obs-nifi-metrics rendered; MetricsReady reason=ServiceMonitorReady."
    kubectl --context "${ctx}" -n "${namespace}" get servicemonitor obs-nifi-metrics -o jsonpath='{.spec.endpoints[0]}{"\n"}' || true
    phaseB_ok=1
    break
  fi
  sleep 5
done
[ "${phaseB_ok}" = "1" ] || { echo "operator did not render the ServiceMonitor (reason='${reason:-<none>}', path='${sm:-<none>}')" >&2; exit 1; }

echo "PASS: NiFi metrics endpoint live, graceful CRD-absent degradation, and ServiceMonitor rendered."
