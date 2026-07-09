#!/usr/bin/env bash
# End-to-end test that spec.coordination.mode=Kubernetes forms a clustered NiFi with NO
# ZooKeeper — using NiFi 2.x's native Lease-based leader election and ConfigMap state
# provider, and the RBAC the operator provisions for the node pods:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision a replicas: 2 NiFiCluster with coordination.mode=Kubernetes and NO ZooKeeper
#      deployed anywhere, and wait for both pods to roll out,
#   3. assert the operator provisioned the ServiceAccount + Role (leases/configmaps) +
#      RoleBinding, and that the pods run under that ServiceAccount,
#   4. assert the two nodes FORM A CLUSTER (2 connected nodes) — which they cannot do without
#      working Lease-based leader election, so cluster formation proves the RBAC and the
#      Kubernetes leader election are functioning,
#   5. assert Kubernetes Leases exist for the cluster and the running nifi.properties selects
#      the Kubernetes providers with an EMPTY ZooKeeper connect string.
#
# Requires kind, kubectl, docker, go, helm.
# Run: make integration-k8scoord-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-k8scoord}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-k8scoord"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster kc --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster kc --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

conf_grep() {
  kubectl --context "${ctx}" -n "${namespace}" exec kc-nifi-0 -c nifi -- \
    grep -- "${1}" "/opt/nifi/nifi-current/conf/nifi.properties" >/dev/null 2>&1
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

echo "Provisioning a replicas=2 NiFiCluster in Kubernetes coordination mode (no ZooKeeper)..."
kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: kc}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 2
  coordination:
    mode: Kubernetes
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 200m, memory: 1Gi}}
  deletionPolicy: Delete
YAML

echo "Waiting for both StatefulSet pods to roll out (first run also pulls the image)..."
for _ in $(seq 1 60); do
  kubectl --context "${ctx}" -n "${namespace}" get statefulset kc-nifi >/dev/null 2>&1 && break
  sleep 5
done
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/kc-nifi --timeout=900s

echo "Phase 1: the operator provisioned the coordination RBAC..."
kubectl --context "${ctx}" -n "${namespace}" get serviceaccount kc-nifi >/dev/null 2>&1 || { echo "ServiceAccount kc-nifi missing" >&2; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" get rolebinding kc-nifi >/dev/null 2>&1 || { echo "RoleBinding kc-nifi missing" >&2; exit 1; }
role_res="$(kubectl --context "${ctx}" -n "${namespace}" get role kc-nifi -o jsonpath='{.rules[*].resources[*]}' 2>/dev/null || true)"
echo "${role_res}" | grep -q "leases" && echo "${role_res}" | grep -q "configmaps" || { echo "Role kc-nifi missing leases/configmaps: ${role_res}" >&2; exit 1; }
pod_sa="$(kubectl --context "${ctx}" -n "${namespace}" get pod kc-nifi-0 -o jsonpath='{.spec.serviceAccountName}')"
[ "${pod_sa}" = "kc-nifi" ] || { echo "pod ServiceAccount = ${pod_sa}, want kc-nifi" >&2; exit 1; }
echo "  ServiceAccount + Role(leases,configmaps) + RoleBinding present; pods run as kc-nifi."

echo "Phase 2: the two nodes form a cluster (proving Lease-based leader election works)..."
fqdn="kc-nifi-0.kc-nifi-headless.${namespace}.svc"
formed=0
for _ in $(seq 1 90); do
  body="$(kubectl --context "${ctx}" -n "${namespace}" exec kc-nifi-0 -c nifi -- \
    curl -fsS "http://${fqdn}:8080/nifi-api/controller/cluster" 2>/dev/null || true)"
  connected="$(printf '%s' "${body}" | grep -o '"status":"CONNECTED"' | wc -l || true)"
  if [ "${connected}" -ge 2 ]; then
    echo "  cluster formed with ${connected} connected nodes over Kubernetes coordination:"
    printf '%s' "${body}" | grep -o '"address":"[^"]*"' | sed 's/^/    /'
    formed=1
    break
  fi
  sleep 5
done
[ "${formed}" = "1" ] || { echo "cluster did not reach 2 connected nodes" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods -o wide >&2; exit 1; }

echo "Phase 3: Kubernetes Leases exist and nifi.properties is ZooKeeper-free..."
leases="$(kubectl --context "${ctx}" -n "${namespace}" get leases -o jsonpath='{.items[*].metadata.name}' 2>/dev/null || true)"
echo "${leases}" | grep -q "kc-nifi" || { echo "no NiFi Leases found in ${namespace}: '${leases}'" >&2; exit 1; }
echo "  Leases in ${namespace}: ${leases}"
conf_grep '^nifi\.state\.management\.provider\.cluster=kubernetes-provider$' || { echo "cluster state provider not kubernetes-provider" >&2; exit 1; }
conf_grep '^nifi\.cluster\.leader\.election\.implementation=KubernetesLeaderElectionManager$' || { echo "leader election not KubernetesLeaderElectionManager" >&2; exit 1; }
conf_grep '^nifi\.zookeeper\.connect\.string=$' || { echo "zookeeper connect string is not empty" >&2; exit 1; }
echo "  nifi.properties: kubernetes-provider + KubernetesLeaderElectionManager + empty ZooKeeper connect string."

echo "PASS: a two-node NiFi cluster coordinates through Kubernetes Leases/ConfigMaps with no ZooKeeper."
