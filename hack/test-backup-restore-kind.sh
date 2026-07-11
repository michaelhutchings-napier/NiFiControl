#!/usr/bin/env bash
# End-to-end test of NiFiBackup and NiFiRestore against a real Apache NiFi 2.10 in kind. NiFiBackup
# downloads a process group's flow from NiFi into a ConfigMap; NiFiRestore reads that snapshot back
# and imports it into a target process group. This proves the round trip against a live NiFi and a
# self-contained ConfigMap store (no external object storage):
#
#   1. build a source flow (a NiFiProcessGroup containing a NiFiLabel) so there is real content,
#   2. NiFiBackup that process group -> a ConfigMap holds the captured flow snapshot (with the
#      label) and the backup reports Succeeded with a digest,
#   3. NiFiRestore (mode Import, source the backup) -> a new process group is created from the
#      snapshot and contains the same label (the backup round-trips),
#   4. delete the backup/restore CRs -> they clear and the owned ConfigMap is garbage-collected.
#
# Requires kind, kubectl, docker, go, helm. Loads the host's cached NiFi image. Run:
#   make integration-backup-restore-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-backup}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-backup"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
base="http://bk-nifi-0.bk-nifi-headless.${namespace}.svc:8080/nifi-api"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifirestore,nifibackup --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nifilabel,nifiprocessgroup --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster bk --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster bk --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

nifi_get() { kubectl --context "${ctx}" -n "${namespace}" exec bk-nifi-0 -c nifi -- curl -fsS "${base}$1" 2>/dev/null || true; }
cr_field() {
  local v=""
  for _ in $(seq 1 30); do
    v="$(kubectl --context "${ctx}" -n "${namespace}" get "$1" "$2" -o jsonpath="$3" 2>/dev/null || true)"
    [ -n "${v}" ] && { echo "${v}"; return 0; }
    sleep 5
  done
  echo ""; return 1
}
pg_has_label() { nifi_get "/flow/process-groups/$1" | grep -q "\"label\":\"$2\""; }

diag() {
  echo "==== backup/restore CR status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifibackup,nifirestore -o wide 2>&1 >&2 || true
  for k in nifibackup nifirestore; do
    for n in $(kubectl --context "${ctx}" -n "${namespace}" get "${k}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      kubectl --context "${ctx}" -n "${namespace}" get "${k}" "${n}" -o jsonpath='{range .status.conditions[*]}    {.type}={.status} {.reason}: {.message}{"\n"}{end}' 2>/dev/null >&2 || true
    done
  done
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 | grep -vE "goroutine|\.go:[0-9]|controller-runtime@|^\s*>" >&2 || true
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi
if ! docker exec "${cluster}-control-plane" crictl images 2>/dev/null | grep -q 'apache/nifi'; then
  if docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1; then
    archive="$(mktemp --suffix=.tar)"; docker save apache/nifi:2.10.0 -o "${archive}"
    kind load image-archive "${archive}" --name "${cluster}" || true; rm -f "${archive}"
  fi
fi

echo "Building the operator image and loading it into kind..."
( cd "${repo_root}" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/nificontrol-manager ./cmd/manager )
build_dir="$(mktemp -d)"; cp /tmp/nificontrol-manager "${build_dir}/manager"
cat > "${build_dir}/Dockerfile" <<'DOCKER'
FROM gcr.io/distroless/static:nonroot
WORKDIR /
COPY manager /manager
USER 65532:65532
ENTRYPOINT ["/manager"]
DOCKER
docker build -t "${image}" "${build_dir}" >/dev/null; rm -rf "${build_dir}"
kind load docker-image "${image}" --name "${cluster}"

echo "Applying the current CRDs..."
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

echo "Deploying the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Provisioning an insecure single-node NiFiCluster..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: bk}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
YAML
ready=0
for _ in $(seq 1 90); do
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster bk -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

echo "Phase 1: build a source flow (process group + label)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessGroup
metadata: {name: source}
spec:
  clusterRef: {name: bk}
  parentProcessGroupRef: {root: true}
  displayName: backup-source
  deletionPolicy: Delete
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiLabel
metadata: {name: marker}
spec:
  clusterRef: {name: bk}
  parentProcessGroupRef: {name: source}
  text: backup-marker
  position: {x: 0, "y": 0}
  width: 200
  height: 80
  deletionPolicy: Delete
YAML
src_pg="$(cr_field nifiprocessgroup source '{.status.nifiId}')" || { echo "source PG not ready" >&2; diag; exit 1; }
[ "$(cr_field nifilabel marker '{.status.ready}')" = "true" ] || { echo "source label not ready" >&2; diag; exit 1; }
pg_has_label "${src_pg}" "backup-marker" || { echo "source PG ${src_pg} is missing its label" >&2; diag; exit 1; }
echo "    source process group ${src_pg} contains the label backup-marker."

echo "Phase 2: NiFiBackup -> a ConfigMap holds the captured flow snapshot..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiBackup
metadata: {name: snap}
spec:
  clusterRef: {name: bk}
  processGroupId: ${src_pg}
  storage: {type: ConfigMap}
YAML
[ "$(cr_field nifibackup snap '{.status.phase}')" = "Succeeded" ] || { echo "backup did not reach Succeeded" >&2; diag; exit 1; }
cm="$(cr_field nifibackup snap '{.status.storageRef}')" || { echo "backup has no storageRef" >&2; diag; exit 1; }
digest="$(cr_field nifibackup snap '{.status.digest}')" || { echo "backup has no digest" >&2; diag; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" get configmap "${cm}" -o jsonpath='{.binaryData.flow\.json}' 2>/dev/null | base64 -d 2>/dev/null | grep -q '"label":"backup-marker"' \
  || { echo "backup ConfigMap ${cm} does not contain the source flow content (label)" >&2; diag; exit 1; }
echo "    backup Succeeded; ConfigMap ${cm} holds the flow snapshot (digest ${digest}, contains the label)."

echo "Phase 3: NiFiRestore (Import) -> a new process group is recreated from the snapshot..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRestore
metadata: {name: rest}
spec:
  clusterRef: {name: bk}
  mode: Import
  source: {backupRef: snap}
YAML
[ "$(cr_field nifirestore rest '{.status.phase}')" = "Succeeded" ] || { echo "restore did not reach Succeeded" >&2; diag; exit 1; }
restored_pg="$(cr_field nifirestore rest '{.status.restoredProcessGroupId}')" || { echo "restore has no restoredProcessGroupId" >&2; diag; exit 1; }
[ "${restored_pg}" != "${src_pg}" ] || { echo "restore did not create a new process group" >&2; diag; exit 1; }
pg_has_label "${restored_pg}" "backup-marker" || { echo "restored PG ${restored_pg} is missing the backed-up label" >&2; diag; exit 1; }
echo "    restored process group ${restored_pg} recreated from the snapshot with the label (round trip verified)."

echo "Phase 4: delete the backup/restore CRs -> they clear and the ConfigMap is garbage-collected..."
kubectl --context "${ctx}" -n "${namespace}" delete nifirestore/rest nifibackup/snap --wait=true --timeout=60s >/dev/null
gone=0
for _ in $(seq 1 12); do
  kubectl --context "${ctx}" -n "${namespace}" get configmap "${cm}" >/dev/null 2>&1 || { gone=1; break; }
  sleep 5
done
[ "${gone}" = "1" ] || { echo "backup ConfigMap ${cm} was not garbage-collected after the backup CR was deleted" >&2; diag; exit 1; }
echo "    backup/restore CRs deleted; owned ConfigMap garbage-collected."

echo "PASS: NiFiBackup captures a process group's flow into a ConfigMap and NiFiRestore imports it back into NiFi 2.10 (round trip verified), with clean deletion."
