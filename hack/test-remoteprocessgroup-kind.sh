#!/usr/bin/env bash
# End-to-end test of NiFiRemoteProcessGroup support against a real Apache NiFi 2.10 in kind. The
# main risk this catches is a mismatch between the property/field names the operator sends and the
# names NiFi's RemoteProcessGroupDTO actually exposes: NiFi only echoes back a field under a key it
# recognises, so reading the created RPG back confirms the names are correct (independent of whether
# the fake site-to-site target is reachable). It also exercises the update path (config change
# requires the RPG stopped first) and the stop-before-delete finalizer path.
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster,
#   3. create a remote process group (HTTP transport) -> confirm NiFi stored targetUris,
#      transportProtocol, communicationsTimeout and yieldDuration under the expected keys,
#   4. change yieldDuration -> confirm NiFi stored the new value (update round-trip),
#   5. delete the RPG (deletionPolicy: Delete) -> confirm NiFi removed it and the finalizer cleared.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-remoteprocessgroup-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-authz}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-rpg"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifiremoteprocessgroup --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster rt --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster rt --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# rpg_json <id> -> the remote process group entity JSON as seen by NiFi.
rpg_json() {
  kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
    curl -fsS "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/remote-process-groups/${1}" 2>/dev/null
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

# Helm installs CRDs only on first install and never upgrades them, so apply the current CRDs
# explicitly — otherwise a warm cluster keeps an older CRD without the NiFiRemoteProcessGroup kind.
# --force-conflicts because the CRDs were first installed by helm (which owns some fields).
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
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
YAML
ready=0
for _ in $(seq 1 90); do
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster rt -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

echo "Phase 1: remote process group -> NiFi stores the expected field names..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRemoteProcessGroup
metadata: {name: central}
spec:
  clusterRef: {name: rt}
  parentProcessGroupRef: {root: true}
  targetUris:
    - https://central.example.com:8443/nifi
  transportProtocol: HTTP
  communicationsTimeout: 45 sec
  yieldDuration: 12 sec
  comments: managed by NiFiControl
  position: {x: 300, "y": 200} # "y" quoted: YAML 1.1 treats the bare key y as boolean true
  deletionPolicy: Delete
YAML
id=""
for _ in $(seq 1 30); do
  id="$(kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup central -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup central -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ -n "${id}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ -n "${id}" ] && [ "${ready}" = "true" ] || { echo "RPG not created/ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup central -o yaml >&2; exit 1; }
rpg="$(rpg_json "${id}")"
echo "${rpg}" | grep -q '"targetUris":"https://central.example.com:8443/nifi"' || { echo "NiFi did not store 'targetUris'" >&2; echo "${rpg}" >&2; exit 1; }
echo "${rpg}" | grep -q '"transportProtocol":"HTTP"' || { echo "NiFi did not store 'transportProtocol'=HTTP" >&2; echo "${rpg}" >&2; exit 1; }
echo "${rpg}" | grep -q '"communicationsTimeout":"45 sec"' || { echo "NiFi did not store 'communicationsTimeout'=45 sec" >&2; echo "${rpg}" >&2; exit 1; }
echo "${rpg}" | grep -q '"yieldDuration":"12 sec"' || { echo "NiFi did not store 'yieldDuration'=12 sec" >&2; echo "${rpg}" >&2; exit 1; }
echo "  RPG ${id} stored targetUris/transportProtocol/communicationsTimeout/yieldDuration under the expected keys."

echo "Phase 2: update yieldDuration -> NiFi stores the new value..."
kubectl --context "${ctx}" -n "${namespace}" patch nifiremoteprocessgroup central --type=merge -p '{"spec":{"yieldDuration":"7 sec"}}' >/dev/null
updated=0
for _ in $(seq 1 30); do
  rpg="$(rpg_json "${id}")"
  echo "${rpg}" | grep -q '"yieldDuration":"7 sec"' && { updated=1; break; }
  sleep 5
done
[ "${updated}" = "1" ] || { echo "NiFi did not apply the updated yieldDuration=7 sec" >&2; echo "${rpg}" >&2; exit 1; }
echo "  RPG ${id} updated yieldDuration to 7 sec."

echo "Phase 3: delete the RPG -> NiFi removes it and the finalizer clears..."
kubectl --context "${ctx}" -n "${namespace}" delete nifiremoteprocessgroup central --timeout=60s >/dev/null
if rpg_json "${id}" >/dev/null 2>&1; then
  echo "RPG ${id} still exists in NiFi after deletion" >&2
  exit 1
fi
echo "  RPG ${id} removed from NiFi and the resource finalizer cleared."

echo "Phase 4: adopt an existing RPG by NiFi id..."
# Create an RPG directly in NiFi (bypassing the operator), then adopt it by id from a CR. If the
# operator adopted correctly the CR's status.nifiId is the pre-existing id (a create would yield a
# new one), proving no duplicate was made.
aid="$(kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
  curl -fsS -X POST -H 'Content-Type: application/json' \
  -d '{"revision":{"version":0},"component":{"targetUris":"https://adopt.example.com:8443/nifi","transportProtocol":"HTTP"}}' \
  "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/process-groups/root/remote-process-groups" \
  2>/dev/null | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4)"
[ -n "${aid}" ] || { echo "failed to create an RPG directly in NiFi" >&2; exit 1; }
echo "  created RPG ${aid} directly in NiFi; adopting it from a CR..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRemoteProcessGroup
metadata: {name: adopted}
spec:
  clusterRef: {name: rt}
  parentProcessGroupRef: {root: true}
  targetUris:
    - https://adopt.example.com:8443/nifi
  transportProtocol: HTTP
  adoptionPolicy: {mode: AdoptById, nifiId: ${aid}}
  deletionPolicy: Orphan
YAML
adopted=""
for _ in $(seq 1 30); do
  adopted="$(kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup adopted -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup adopted -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ "${adopted}" = "${aid}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ "${adopted}" = "${aid}" ] && [ "${ready}" = "true" ] || { echo "CR did not adopt RPG ${aid} (got nifiId='${adopted}', ready='${ready}')" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifiremoteprocessgroup adopted -o yaml >&2; exit 1; }
echo "  CR 'adopted' took over existing RPG ${aid} (no duplicate created)."

echo "PASS: NiFi accepts the RemoteProcessGroup field names the operator sends, and the update, stop-before-delete, and adopt-by-id paths work."
