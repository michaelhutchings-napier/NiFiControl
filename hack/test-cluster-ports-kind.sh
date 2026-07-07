#!/usr/bin/env bash
# End-to-end test that a real two-node Apache NiFi 2.10 cluster forms over CUSTOM cluster
# ports (spec.ports.clusterProtocol and spec.ports.loadBalance) in kind. Single-node
# harnesses cannot prove this: the cluster-protocol port only matters once nodes actually
# talk to each other, so a wrong container-port/property mapping would only surface here.
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. deploy a standalone ZooKeeper,
#   3. provision a clustered (replicas: 2) NiFiCluster with a non-default cluster-protocol
#      port (12443) and load-balance port (6343),
#   4. wait for both StatefulSet pods to roll out, then assert the cluster forms with two
#      CONNECTED nodes via the NiFi cluster API — reachable only if the nodes clustered over
#      the custom protocol port,
#   5. assert the persisted nifi.properties and the headless Service both carry the custom
#      cluster-protocol and load-balance ports.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-clusterports-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-clusterports}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-clusterports"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster cp --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster cp --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

conf_grep() {
  kubectl --context "${ctx}" -n "${namespace}" exec cp-nifi-0 -c nifi -- \
    grep -- "${1}" "/opt/nifi/nifi-current/conf/nifi.properties" >/dev/null 2>&1
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi
for img in apache/nifi:2.10.0 zookeeper:3.9; do
  docker image inspect "${img}" >/dev/null 2>&1 && kind load docker-image "${img}" --name "${cluster}" >/dev/null 2>&1 || true
done

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

echo "Phase 1: clustered NiFiCluster with custom cluster-protocol and load-balance ports..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: cp}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 2
  coordination:
    zookeeperConnectString: zookeeper.nifi-clusterports.svc:2181
    zookeeperRootNode: /cp
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 200m, memory: 1Gi}
  ports:
    clusterProtocol: 12443
    loadBalance: 6343
  deletionPolicy: Delete
YAML

echo "Waiting for both StatefulSet pods to roll out (first run also pulls the image)..."
for _ in $(seq 1 60); do
  kubectl --context "${ctx}" -n "${namespace}" get statefulset cp-nifi >/dev/null 2>&1 && break
  sleep 5
done
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/cp-nifi --timeout=900s

echo "Phase 2: the two nodes form a cluster over the custom protocol port..."
fqdn="cp-nifi-0.cp-nifi-headless.${namespace}.svc"
formed=0
for _ in $(seq 1 90); do
  body="$(kubectl --context "${ctx}" -n "${namespace}" exec cp-nifi-0 -c nifi -- \
    curl -fsS "http://${fqdn}:8080/nifi-api/controller/cluster" 2>/dev/null || true)"
  # grep exits non-zero before any node connects; under set -e/pipefail that would abort
  # the whole loop, so mask it and let the counter fall back to 0.
  connected="$(printf '%s' "${body}" | grep -o '"status":"CONNECTED"' | wc -l || true)"
  if [ "${connected}" -ge 2 ]; then
    echo "  cluster formed with ${connected} connected nodes over cluster-protocol port 12443:"
    printf '%s' "${body}" | grep -o '"address":"[^"]*"' | sed 's/^/    /'
    formed=1
    break
  fi
  sleep 5
done
[ "${formed}" = "1" ] || { echo "cluster did not reach 2 connected nodes over the custom protocol port" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods -o wide >&2; exit 1; }

echo "Phase 3: persisted nifi.properties and the headless Service carry the custom ports..."
conf_grep '^nifi\.cluster\.node\.protocol\.port=12443$' || { echo "nifi.cluster.node.protocol.port not set to 12443" >&2; exit 1; }
conf_grep '^nifi\.cluster\.load\.balance\.port=6343$' || { echo "nifi.cluster.load.balance.port not set to 6343" >&2; exit 1; }
cluster_port="$(kubectl --context "${ctx}" -n "${namespace}" get svc cp-nifi-headless -o jsonpath='{.spec.ports[?(@.name=="cluster")].port}')"
lb_port="$(kubectl --context "${ctx}" -n "${namespace}" get svc cp-nifi-headless -o jsonpath='{.spec.ports[?(@.name=="load-balance")].port}')"
[ "${cluster_port}" = "12443" ] || { echo "headless cluster port = ${cluster_port}, want 12443" >&2; exit 1; }
[ "${lb_port}" = "6343" ] || { echo "headless load-balance port = ${lb_port}, want 6343" >&2; exit 1; }
echo "  nifi.properties and the headless Service carry cluster=12443, load-balance=6343."

echo "PASS: a two-node NiFi cluster forms and load-balances over custom cluster ports."
