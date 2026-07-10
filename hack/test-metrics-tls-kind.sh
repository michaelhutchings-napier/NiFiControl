#!/usr/bin/env bash
# End-to-end test that NiFi's Prometheus metrics endpoint is scrapeable on a *secured* cluster
# using the operator's own client identity — the identity the rendered ServiceMonitor scrapes as.
# On a TLS cluster NiFi serves /nifi-api/flow/metrics/prometheus over HTTPS and requires
# authentication + authorization (read on /flow). The operator's client certificate is NiFi's
# Initial Admin, which NiFi seeds with /flow read, so scraping works with no extra policy. This
# proves that end to end:
#
#   1. build + load the operator image, install cert-manager, helm install the operator,
#   2. provision a secured single-node NiFiCluster (internalTLS selfSigned),
#   3. GET /nifi-api/flow/metrics/prometheus with the operator client certificate -> HTTP 200 and
#      a Prometheus exposition body (proves the operator identity is authorized to scrape),
#   4. the same GET with NO client certificate is rejected (mutual TLS is enforced).
#
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-metrics-tls-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-metricstls}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-metricstls"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
headless="mt-nifi-headless"
metrics_path="/nifi-api/flow/metrics/prometheus"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster mt --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster mt --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster mt -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
  echo "==== pods / certs ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods,certificate,secret 2>&1 >&2 || true
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 120); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster mt -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "secured NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
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

echo "Installing the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: secured single-node cluster (internalTLS selfSigned)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: mt}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: true, size: 2Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  internalTLS:
    enabled: true
    selfSigned: {}
YAML
wait_cluster_ready

client_secret="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster mt -o jsonpath='{.status.tls.clientSecretName}')"
[ -n "${client_secret}" ] || { echo "could not resolve operator client TLS Secret name" >&2; dump_diagnostics; exit 1; }
admin_identity="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster mt -o jsonpath='{.status.tls.initialAdminIdentity}')"
echo "  operator client Secret=${client_secret}, initial admin identity=${admin_identity}"

echo "Phase 2: scrape ${metrics_path} with the operator client certificate (expect HTTP 200)..."
# NiFi binds its HTTPS port to the pod FQDN, so scrape there (as the ServiceMonitor's endpoint does).
fqdn="mt-nifi-0.${headless}.${namespace}.svc"
tmp="$(mktemp -d)"
for key in tls.crt tls.key ca.crt; do
  kubectl --context "${ctx}" -n "${namespace}" get secret "${client_secret}" -o "jsonpath={.data.${key//./\\.}}" | base64 -d > "${tmp}/${key}"
done
kubectl --context "${ctx}" -n "${namespace}" cp "${tmp}/tls.crt" "mt-nifi-0:/tmp/op.crt" -c nifi
kubectl --context "${ctx}" -n "${namespace}" cp "${tmp}/tls.key" "mt-nifi-0:/tmp/op.key" -c nifi
kubectl --context "${ctx}" -n "${namespace}" cp "${tmp}/ca.crt" "mt-nifi-0:/tmp/op-ca.crt" -c nifi
rm -rf "${tmp}"

code=""
body_head=""
for _ in $(seq 1 12); do
  code="$(kubectl --context "${ctx}" -n "${namespace}" exec mt-nifi-0 -c nifi -- \
    bash -c "curl -sS -o /tmp/metrics.out -w '%{http_code}' --cert /tmp/op.crt --key /tmp/op.key --cacert /tmp/op-ca.crt https://${fqdn}:8443${metrics_path}" 2>/dev/null || true)"
  [ "${code}" = "200" ] && break
  sleep 5
done
[ "${code}" = "200" ] || { echo "metrics scrape as operator identity returned HTTP '${code:-<none>}', expected 200" >&2; dump_diagnostics; exit 1; }
# Confirm the body is a Prometheus exposition (NiFi emits nifi_* metrics with HELP/TYPE headers).
body_head="$(kubectl --context "${ctx}" -n "${namespace}" exec mt-nifi-0 -c nifi -- bash -c 'grep -m1 -E "^# (HELP|TYPE) nifi_|^nifi_" /tmp/metrics.out' 2>/dev/null || true)"
[ -n "${body_head}" ] || { echo "HTTP 200 but body was not Prometheus metrics (no nifi_* series)" >&2; dump_diagnostics; exit 1; }
echo "    HTTP 200 as ${admin_identity}; body is Prometheus exposition (e.g. ${body_head})."

echo "Phase 3: the same scrape with NO client certificate is rejected (mutual TLS enforced)..."
anon_code="$(kubectl --context "${ctx}" -n "${namespace}" exec mt-nifi-0 -c nifi -- \
  bash -c "curl -sS -o /dev/null -w '%{http_code}' --cacert /tmp/op-ca.crt https://${fqdn}:8443${metrics_path}" 2>/dev/null || true)"
# A missing client cert fails the mutual-TLS handshake (curl exits non-zero -> empty code) or is
# refused at the app layer (401/403). Any non-200 is a rejection; 200 would mean it is wide open.
[ "${anon_code}" != "200" ] || { echo "metrics endpoint served content with NO client certificate; auth is not enforced" >&2; dump_diagnostics; exit 1; }
echo "    rejected without a client certificate (code='${anon_code:-handshake-failed}')."

echo "PASS: NiFi Prometheus metrics are scrapeable on a secured cluster with the operator client identity (HTTP 200, Prometheus body), and rejected without a client certificate."
