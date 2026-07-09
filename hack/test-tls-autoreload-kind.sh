#!/usr/bin/env bash
# End-to-end test of internalTLS.autoReload against a real Apache NiFi 2.10 in kind. Without
# auto-reload the operator rolls the StatefulSet whenever the server certificate changes; with
# it, NiFi rescans the mounted keystore/truststore and reloads the SSL context in place, so a
# rotated leaf certificate is served WITHOUT restarting the pod. This test proves that end to
# end:
#
#   1. build + load the operator image, install cert-manager, helm install the operator,
#   2. provision a secured single-node NiFiCluster (internalTLS selfSigned) with
#      internalTLS.autoReload.enabled=true, and confirm nifi.security.autoreload.enabled=true
#      landed in the running nifi.properties,
#   3. capture the served certificate serial on :8443, the pod UID/restartCount, and the
#      StatefulSet tls-checksum annotation,
#   4. force a leaf rotation by deleting the cert-manager server Secret (it re-issues a new leaf
#      under the same CA),
#   5. assert NiFi serves the NEW certificate serial (auto-reload is live) while the pod was NOT
#      restarted (same UID, same restartCount) and the operator did NOT roll the StatefulSet
#      (tls-checksum annotation unchanged) — i.e. the certificate rotated with zero downtime.
#
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-tls-autoreload-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-tlsar}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-tlsar"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
conf="/opt/nifi/nifi-current/conf"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster ar --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster ar --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster ar -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
  echo "==== pods / certs ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods,certificate,secret 2>&1 >&2 || true
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 120); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster ar -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "secured NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
}

# served_serial -> the serial of the certificate NiFi is actively serving on :8443 (proves the
# in-memory SSL context, not just the file on disk).
served_serial() {
  kubectl --context "${ctx}" -n "${namespace}" exec ar-nifi-0 -c nifi -- \
    bash -c 'echo | openssl s_client -connect localhost:8443 2>/dev/null | openssl x509 -noout -serial' 2>/dev/null || true
}

pod_uid()      { kubectl --context "${ctx}" -n "${namespace}" get pod ar-nifi-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true; }
restart_count(){ kubectl --context "${ctx}" -n "${namespace}" get pod ar-nifi-0 -o "jsonpath={.status.containerStatuses[?(@.name=='nifi')].restartCount}" 2>/dev/null || true; }
tls_checksum() {
  kubectl --context "${ctx}" -n "${namespace}" get statefulset ar-nifi \
    -o "go-template={{index .spec.template.metadata.annotations \"nifi.controlnifi.io/tls-checksum\"}}" 2>/dev/null || true
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

echo "Phase 1: secured single-node cluster with internalTLS.autoReload enabled..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: ar}
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
    autoReload:
      enabled: true
      interval: "5 secs"
YAML
wait_cluster_ready

echo "  autoreload property landed in the running nifi.properties..."
kubectl --context "${ctx}" -n "${namespace}" exec ar-nifi-0 -c nifi -- \
  grep -q '^nifi.security.autoreload.enabled=true$' "${conf}/nifi.properties" \
  || { echo "nifi.security.autoreload.enabled=true not applied" >&2; dump_diagnostics; exit 1; }
echo "    autoreload enabled in nifi.properties."

server_secret="$(kubectl --context "${ctx}" -n "${namespace}" get nificluster ar -o jsonpath='{.status.tls.serverSecretName}')"
[ -n "${server_secret}" ] || { echo "could not resolve server TLS Secret name" >&2; dump_diagnostics; exit 1; }

serial_before="$(served_serial)"
uid_before="$(pod_uid)"
restarts_before="$(restart_count)"
checksum_before="$(tls_checksum)"
[ -n "${serial_before}" ] || { echo "could not read the served certificate serial" >&2; dump_diagnostics; exit 1; }
echo "  before rotation: ${serial_before}, pod ${uid_before} (restarts=${restarts_before}), checksum=${checksum_before}"

echo "Phase 2: force a leaf rotation (delete the cert-manager server Secret; it re-issues)..."
kubectl --context "${ctx}" -n "${namespace}" delete secret "${server_secret}" >/dev/null

echo "  waiting for NiFi to serve the rotated certificate (in-place reload)..."
serial_after=""
for _ in $(seq 1 48); do
  serial_after="$(served_serial)"
  if [ -n "${serial_after}" ] && [ "${serial_after}" != "${serial_before}" ]; then
    break
  fi
  sleep 5
done
[ -n "${serial_after}" ] && [ "${serial_after}" != "${serial_before}" ] \
  || { echo "NiFi never served the rotated certificate; auto-reload was not effective (still ${serial_before})" >&2; dump_diagnostics; exit 1; }
echo "    NiFi now serves the rotated certificate: ${serial_after}"

echo "Phase 3: assert this happened WITHOUT a pod restart or a StatefulSet roll..."
uid_after="$(pod_uid)"
restarts_after="$(restart_count)"
checksum_after="$(tls_checksum)"
[ "${uid_after}" = "${uid_before}" ] || { echo "pod was recreated (${uid_before} -> ${uid_after}); this was a roll, not auto-reload" >&2; dump_diagnostics; exit 1; }
[ "${restarts_after}" = "${restarts_before}" ] || { echo "nifi container restarted (${restarts_before} -> ${restarts_after}); not an in-place reload" >&2; dump_diagnostics; exit 1; }
[ "${checksum_after}" = "${checksum_before}" ] || { echo "operator rolled the StatefulSet on leaf rotation (checksum ${checksum_before} -> ${checksum_after})" >&2; dump_diagnostics; exit 1; }
echo "    same pod ${uid_after} (restarts=${restarts_after}), tls-checksum unchanged."

echo "PASS: internalTLS.autoReload rotated the certificate in place — new cert served, no pod restart, no StatefulSet roll."
