#!/usr/bin/env bash
# Full end-to-end test of a plain HorizontalPodAutoscaler (no KEDA) driving a managed
# NiFiCluster through its scale subresource, in a local kind cluster:
#
#   1. install the CRDs and run the operator out-of-cluster against kind,
#   2. let the operator provision a clustered NiFiCluster (ZooKeeper + 2 nodes),
#   3. confirm the scale subresource is populated (status.replicas / status.selector),
#   4. confirm `kubectl scale nificluster ...` (the write path an HPA uses) resizes it,
#   5. create a CPU HorizontalPodAutoscaler and confirm it reads the scale subresource and
#      computes a replica count from a live metric (ScalingActive / ValidMetricFound).
#
# Requires kind, kubectl, docker, go, and internet access (for metrics-server). Run with:
#   make integration-hpa-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-hpa}"
ctx="kind-${cluster}"
namespace="nifi-hpa"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
kubeconfig="$(mktemp)"
operator_bin="$(mktemp -u)"
operator_pid=""

cleanup() {
  # Remove the test resources while the operator is still up so its finalizer runs.
  kubectl --context "${ctx}" -n "${namespace}" delete hpa nifi-hpa --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster hpa --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster hpa --ignore-not-found --wait=false >/dev/null 2>&1 || true
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

echo "Installing metrics-server (with --kubelet-insecure-tls for kind)..."
kubectl --context "${ctx}" apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml >/dev/null
kubectl --context "${ctx}" -n kube-system patch deployment metrics-server --type=json \
  -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null 2>&1 || true

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
  > /tmp/nificontrol-hpa-operator.log 2>&1 &
operator_pid=$!
echo "operator pid ${operator_pid}"

echo "Creating a clustered NiFiCluster (the operator will provision it)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: hpa}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 2
  coordination:
    zookeeperConnectString: zookeeper.nifi-hpa.svc:2181
    zookeeperRootNode: /hpa
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 200m, memory: 1Gi}
  deletionPolicy: Delete
YAML

echo "Waiting for the operator to create the StatefulSet and its pods to become ready..."
for _ in $(seq 1 60); do
  kubectl --context "${ctx}" -n "${namespace}" get statefulset hpa-nifi >/dev/null 2>&1 && break
  sleep 5
done
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/hpa-nifi --timeout=900s

echo "Checking the scale subresource is populated..."
for _ in $(seq 1 60); do
  reps="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster hpa --subresource=scale -o jsonpath='{.status.replicas}' 2>/dev/null || true)"
  sel="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster hpa --subresource=scale -o jsonpath='{.status.selector}' 2>/dev/null || true)"
  if [ "${reps:-0}" -ge 2 ] 2>/dev/null && [ -n "${sel}" ]; then
    echo "  scale subresource: status.replicas=${reps}, selector=${sel}"
    break
  fi
  sleep 5
done
[ "${reps:-0}" -ge 2 ] 2>/dev/null || { echo "scale subresource was not populated" >&2; exit 1; }

echo "Verifying the scale write path (kubectl scale -> operator -> StatefulSet)..."
kubectl --context "${ctx}" -n "${namespace}" scale nificluster/hpa --replicas=3 >/dev/null
for _ in $(seq 1 24); do
  want="$(kubectl --context "${ctx}" -n "${namespace}" get statefulset hpa-nifi -o jsonpath='{.spec.replicas}' 2>/dev/null || true)"
  [ "${want}" = "3" ] && break
  sleep 5
done
[ "${want}" = "3" ] || { echo "operator did not scale the StatefulSet to 3" >&2; exit 1; }
echo "  StatefulSet scaled to 3 via the scale subresource."

echo "Creating a CPU HorizontalPodAutoscaler targeting the NiFiCluster..."
kubectl --context "${ctx}" -n "${namespace}" autoscale nificluster/hpa --cpu-percent=80 --min=2 --max=4 >/dev/null

echo "Waiting for the HPA to read the scale subresource and a live CPU metric..."
for _ in $(seq 1 40); do
  if kubectl --context "${ctx}" -n "${namespace}" get hpa hpa --no-headers 2>/dev/null | grep -qE "cpu: [0-9]+%/"; then
    able="$(kubectl --context "${ctx}" -n "${namespace}" get hpa hpa -o jsonpath='{.status.conditions[?(@.type=="AbleToScale")].status}' 2>/dev/null)"
    active="$(kubectl --context "${ctx}" -n "${namespace}" get hpa hpa -o jsonpath='{.status.conditions[?(@.type=="ScalingActive")].status}' 2>/dev/null)"
    if [ "${able}" = "True" ] && [ "${active}" = "True" ]; then
      echo "  $(kubectl --context "${ctx}" -n "${namespace}" get hpa hpa --no-headers 2>/dev/null)"
      echo "  AbleToScale=${able}, ScalingActive=${active}"
      echo "PASS: a plain HorizontalPodAutoscaler drives the NiFiCluster scale subresource."
      exit 0
    fi
  fi
  sleep 6
done

echo "HPA did not become active against the NiFiCluster scale subresource" >&2
kubectl --context "${ctx}" -n "${namespace}" describe hpa hpa 2>&1 | tail -25 || true
exit 1
