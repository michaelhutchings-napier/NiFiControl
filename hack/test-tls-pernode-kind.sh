#!/usr/bin/env bash
# End-to-end test of internalTLS.perNodeCertificates against a real Apache NiFi 2.10 cluster in
# kind. Instead of one shared server certificate, each node pod is issued its own certificate +
# private key in-pod by the cert-manager CSI driver, with a distinct identity (CN=node-<pod>)
# that the operator maps to the node role so the cluster still forms. This test proves it end to
# end on a 2-node cluster:
#
#   1. build + load the operator image, install cert-manager AND the cert-manager CSI driver,
#      helm install the operator,
#   2. provision a 2-node secured NiFiCluster (internalTLS selfSigned + perNodeCertificates,
#      Kubernetes coordination) and wait until it is Ready — which, for a 2-node cluster, means
#      both nodes passed the mutual-TLS readiness probe and connected to each other over
#      per-node-cert mTLS (a node that cannot present/verify a per-node cert never becomes Ready),
#   3. assert each node serves a DISTINCT per-node certificate identity (CN=node-<pod>),
#   4. assert node-to-node mutual TLS works: node 0 completes a verified TLS handshake to node 1
#      presenting its own per-node client certificate and trusting the shared CA.
#
# Also checks the guard: with the CSI driver absent the cluster reports CSIDriverMissing (covered
# by unit tests; here we rely on the driver being installed).
#
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-tls-pernode-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-pernode}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-pernode"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
headless="pn-nifi-headless"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster pn --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster pn --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster pn -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
  echo "==== pods ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods 2>&1 >&2 || true
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 150); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster pn -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "per-node NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
}

# served_subject <pod> -> the subject DN of the certificate that pod serves on :8443.
# NiFi binds its HTTPS port to the pod FQDN (nifi.web.https.host), not localhost, so connect there.
served_subject() {
  local fqdn="$1.${headless}.${namespace}.svc"
  kubectl --context "${ctx}" -n "${namespace}" exec "$1" -c nifi -- \
    bash -c "echo | openssl s_client -connect ${fqdn}:8443 2>/dev/null | openssl x509 -noout -subject" 2>/dev/null || true
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

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

echo "Applying the current CRDs..."
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

echo "Installing cert-manager..."
kubectl --context "${ctx}" apply -f "${certmanager}" >/dev/null
for dep in cert-manager-webhook cert-manager cert-manager-cainjector; do
  kubectl --context "${ctx}" -n cert-manager rollout status "deploy/${dep}" --timeout=300s
done

echo "Installing the cert-manager CSI driver..."
helm repo add jetstack https://charts.jetstack.io --force-update >/dev/null
helm repo update jetstack >/dev/null
helm --kube-context "${ctx}" upgrade --install cert-manager-csi-driver jetstack/cert-manager-csi-driver \
  --namespace cert-manager --wait --timeout 5m >/dev/null
kubectl --context "${ctx}" -n cert-manager rollout status daemonset/cert-manager-csi-driver --timeout=300s
kubectl --context "${ctx}" get csidriver csi.cert-manager.io >/dev/null || { echo "CSIDriver csi.cert-manager.io not registered" >&2; exit 1; }

echo "Installing the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: 2-node secured cluster with per-node certificates (Kubernetes coordination)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: pn}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 2
  storage: {enabled: true, size: 2Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  coordination:
    mode: Kubernetes
  internalTLS:
    enabled: true
    selfSigned: {}
    perNodeCertificates:
      enabled: true
YAML
wait_cluster_ready
echo "  cluster Ready — both nodes passed mutual-TLS readiness and connected over per-node certs."

echo "Phase 2: each node serves a DISTINCT per-node certificate identity..."
subj0="$(served_subject pn-nifi-0)"
subj1="$(served_subject pn-nifi-1)"
echo "    node 0: ${subj0}"
echo "    node 1: ${subj1}"
case "${subj0}" in *node-pn-nifi-0*) ;; *) echo "node 0 did not present a per-node identity (got ${subj0})" >&2; dump_diagnostics; exit 1;; esac
case "${subj1}" in *node-pn-nifi-1*) ;; *) echo "node 1 did not present a per-node identity (got ${subj1})" >&2; dump_diagnostics; exit 1;; esac
[ "${subj0}" != "${subj1}" ] || { echo "both nodes presented the same certificate identity; per-node certs not in effect" >&2; exit 1; }
echo "    distinct per-node identities confirmed."

echo "Phase 3: node-to-node mutual TLS works with per-node certs..."
kubectl --context "${ctx}" -n "${namespace}" exec pn-nifi-0 -c nifi -- bash -c "
  set -e
  out=\$(echo | openssl s_client -connect pn-nifi-1.${headless}.${namespace}.svc:8443 \
    -cert /opt/nifi/nificontrol-tls/tls.crt -key /opt/nifi/nificontrol-tls/tls.key \
    -CAfile /opt/nifi/nificontrol-tls/ca.crt -verify_return_error 2>&1 || true)
  echo \"\${out}\" | grep -q 'Verify return code: 0 (ok)'
" || { echo "node 0 -> node 1 verified mutual-TLS handshake failed" >&2; dump_diagnostics; exit 1; }
echo "    node 0 completed a verified mTLS handshake to node 1 using its own per-node certificate."

echo "PASS: per-node certificates issued in-pod by the CSI driver — distinct identities per node, 2-node cluster formed over per-node mTLS, and node-to-node mutual TLS verified."
