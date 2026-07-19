#!/usr/bin/env bash
# End-to-end test of the authorization subsystem (tenants + access policies) against a real
# secured Apache NiFi 2.10 in kind. Access policies only exist on a secured NiFi with a managed
# authorizer, and the operator must reach NiFi over mTLS as the initial admin, so unlike the
# other harnesses this one runs the operator IN-cluster:
#
#   1. build the operator image and load it into kind,
#   2. install cert-manager (operator-managed internalTLS uses it),
#   3. helm install the operator (CRDs + RBAC + deployment),
#   4. provision a secured single-node NiFiCluster (internalTLS selfSigned),
#   5. create a NiFiUser and a NiFiPolicy granting it /flow read,
#   6. confirm the operator created the tenant (status.nifiId) and the policy (Ready) via NiFi's
#      /tenants and /policies APIs as the initial admin.
#
# Requires kind, kubectl, docker, go, helm, and internet access. Run:
#   make integration-authz-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-authz}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-authz"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifipolicy,nifiuser,nifiusergroup --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster secure --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster secure --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== NiFiUser status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifiuser scraper -o yaml 2>&1 >&2 || true
  echo "==== NiFiPolicy status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifipolicy scraper-read-flow -o yaml 2>&1 >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
  echo "==== NiFi pod ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods 2>&1 >&2 || true
  echo "==== NiFi container logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" logs secure-nifi-0 -c nifi --tail=120 2>&1 >&2 || true
  echo "==== NiFi authorization log hints ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- sh -c 'grep -iE "authoriz|access|certificate|identity|forbidden|403|denied" /opt/nifi/nifi-current/logs/nifi-app.log | tail -80' 2>&1 >&2 || true
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

# Best-effort side-load of the host NiFi image.
if ! docker exec "${cluster}-control-plane" crictl images 2>/dev/null | grep -q 'apache/nifi'; then
  if docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1; then
    archive="$(mktemp --suffix=.tar)"
    docker save apache/nifi:2.10.0 -o "${archive}"
    kind load image-archive "${archive}" --name "${cluster}" || true
    rm -f "${archive}"
  fi
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

echo "Installing cert-manager..."
kubectl --context "${ctx}" apply -f "${certmanager}" >/dev/null
kubectl --context "${ctx}" -n cert-manager rollout status deploy/cert-manager-webhook --timeout=300s
kubectl --context "${ctx}" -n cert-manager rollout status deploy/cert-manager --timeout=300s
kubectl --context "${ctx}" -n cert-manager rollout status deploy/cert-manager-cainjector --timeout=300s

echo "Installing the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Provisioning a secured single-node NiFiCluster (internalTLS selfSigned)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: secure}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
  internalTLS:
    enabled: true
    selfSigned: {}
  deletionPolicy: Delete
YAML

echo "Waiting for the secured NiFiCluster to become Ready (cert issuance + secured boot)..."
ready=0
for _ in $(seq 1 120); do
  if [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ]; then
    echo "  NiFiCluster is Ready."
    ready=1
    break
  fi
  sleep 10
done
[ "${ready}" = "1" ] || { echo "secured NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }

echo "Creating a NiFiUser and a NiFiPolicy..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata: {name: scraper}
spec:
  clusterRef: {name: secure}
  identity: "CN=prometheus, O=monitoring"
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: scraper-read-flow}
spec:
  clusterRef: {name: secure}
  resource: /flow
  action: read
  userRefs:
    - name: scraper
YAML

echo "Waiting for the NiFiUser tenant to be created in NiFi..."
userok=0
for _ in $(seq 1 30); do
  id="$(kubectl --context "${ctx}" -n "${namespace}" get nifiuser scraper -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiuser scraper -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  if [ -n "${id}" ] && [ "${ready}" = "true" ]; then
    echo "  NiFiUser created as NiFi tenant ${id}."
    userok=1
    break
  fi
  sleep 5
done
[ "${userok}" = "1" ] || { echo "NiFiUser tenant was not created" >&2; dump_diagnostics; exit 1; }

echo "Waiting for the NiFiPolicy to be created in NiFi..."
policyok=0
for _ in $(seq 1 30); do
  pid="$(kubectl --context "${ctx}" -n "${namespace}" get nifipolicy scraper-read-flow -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  pready="$(kubectl --context "${ctx}" -n "${namespace}" get nifipolicy scraper-read-flow -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  uids="$(kubectl --context "${ctx}" -n "${namespace}" get nifipolicy scraper-read-flow -o jsonpath='{.status.userIds[0]}' 2>/dev/null || true)"
  if [ -n "${pid}" ] && [ "${pready}" = "true" ] && [ -n "${uids}" ]; then
    echo "  NiFiPolicy created as access policy ${pid}, granting user ${uids} read on /flow."
    policyok=1
    break
  fi
  sleep 5
done
[ "${policyok}" = "1" ] || { echo "NiFiPolicy was not created" >&2; dump_diagnostics; exit 1; }

echo "PASS: on a secured NiFi, the operator created a user tenant and an access policy via NiFi's managed authorizer."
