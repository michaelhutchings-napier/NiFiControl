#!/usr/bin/env bash
# End-to-end test of declarative NiFi site-to-site: two insecure Apache NiFi 2.10 clusters in kind
# ("central" and "edge") wired together purely through NiFiControl CRDs, with no NiFi UI action.
#
# The user-chosen depth is a config + transmission round-trip (not a data-delivery assertion):
#   1. central: a root input port "from-edge" (site-to-site accessible) feeding a funnel, RUNNING.
#   2. edge: a NiFiRemoteProcessGroup targeting central over HTTP, discovering "from-edge".
#   3. edge: GenerateFlowFile -> NiFiConnection(RemoteInputPort from-edge) connects the remote port.
#   4. edge: the RPG configures the port (concurrency, compression) and turns transmission on.
#   5. assert (reading edge's NiFi REST API) that the remote port is connected=true,
#      transmitting=true, and carries the concurrency/compression the operator sent.
#
# Insecure HTTP site-to-site works because the operator sets nifi.remote.input.secure=false and
# nifi.remote.input.http.enabled defaults true; additionalEnv pins a resolvable remote-input host
# and whitelists the cross-pod Host header (nifi.web.proxy.host).
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-remoteprocessgroup-s2s-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-authz}"
ctx="kind-${cluster}"
opns="nificontrol-system"
ns="nifi-s2s"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

central_fqdn="central-nifi-0.central-nifi-headless.${ns}.svc"
edge_fqdn="edge-nifi-0.edge-nifi-headless.${ns}.svc"

# reset_namespace force-clears finalizers on any leftover NiFiControl resources and deletes the
# namespace. Canvas resources whose NiFiCluster is deleted first can otherwise wedge (their delete
# path waits on the now-gone cluster), so a plain delete would leave stuck objects across reruns.
# This is a disposable test namespace, so orphaning NiFi components is fine — the pods die with it.
reset_namespace() {
  local waitflag="$1"
  kubectl --context "${ctx}" get ns "${ns}" >/dev/null 2>&1 || return 0
  for kind in nificonnection nifiremoteprocessgroup nifiprocessor nifiinputport nifioutputport nififunnel nifilabel nificontrollerservice nifiprocessgroup nificluster; do
    for r in $(kubectl --context "${ctx}" -n "${ns}" get "${kind}" -o name 2>/dev/null); do
      kubectl --context "${ctx}" -n "${ns}" patch "${r}" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
    done
  done
  kubectl --context "${ctx}" delete namespace "${ns}" --wait="${waitflag}" --timeout=150s >/dev/null 2>&1 || true
}

cleanup() {
  if [ "${KEEP:-0}" = "1" ]; then
    echo "KEEP=1: leaving namespace ${ns} in place for inspection." >&2
    return 0
  fi
  reset_namespace false
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# nifi_curl <pod> <path> -> GET the given NiFi API path from inside the named NiFi pod.
nifi_curl() {
  local pod="$1" host="$2" path="$3"
  kubectl --context "${ctx}" -n "${ns}" exec "${pod}" -c nifi -- \
    curl -fsS "http://${host}:8080/nifi-api/${path}" 2>/dev/null
}

wait_ready() {
  local kind="$1" name="$2" tries="${3:-60}"
  for _ in $(seq 1 "${tries}"); do
    [ "$(kubectl --context "${ctx}" -n "${ns}" get "${kind}" "${name}" -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && return 0
    sleep 5
  done
  echo "${kind}/${name} did not become Ready" >&2
  kubectl --context "${ctx}" -n "${ns}" get "${kind}" "${name}" -o yaml >&2
  return 1
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

echo "Applying the current CRDs..."
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

echo "Deploying the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

# Start from a pristine namespace so a previous killed/failed run cannot contaminate this one with
# stuck resources holding stale NiFi ids.
echo "Ensuring a clean ${ns} namespace..."
reset_namespace true
kubectl --context "${ctx}" create namespace "${ns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Provisioning two insecure NiFiClusters (central + edge)..."
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: central}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  additionalEnv:
    - {name: NIFI_REMOTE_INPUT_HOST, value: ${central_fqdn}}
    - {name: NIFI_WEB_PROXY_HOST, value: "${central_fqdn}:8080,central-nifi.${ns}.svc:8080,${central_fqdn},central-nifi.${ns}.svc"}
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: edge}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  additionalEnv:
    - {name: NIFI_REMOTE_INPUT_HOST, value: ${edge_fqdn}}
    - {name: NIFI_WEB_PROXY_HOST, value: "${edge_fqdn}:8080,edge-nifi.${ns}.svc:8080,${edge_fqdn},edge-nifi.${ns}.svc"}
YAML
wait_ready nificluster central 90
wait_ready nificluster edge 90
echo "  both NiFiClusters are Ready."

echo "central: creating a site-to-site input port 'from-edge' feeding a funnel..."
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFunnel
metadata: {name: central-sink}
spec:
  clusterRef: {name: central}
  parentProcessGroupRef: {root: true}
  position: {x: 600, "y": 200}
  deletionPolicy: Delete
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiInputPort
metadata: {name: from-edge}
spec:
  clusterRef: {name: central}
  parentProcessGroupRef: {root: true}
  position: {x: 200, "y": 200}
  state: Stopped
  deletionPolicy: Delete
YAML
wait_ready nifiinputport from-edge 30
wait_ready nififunnel central-sink 30
# Connect the input port to the funnel so it is valid, then start it.
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiConnection
metadata: {name: central-ingest}
spec:
  clusterRef: {name: central}
  parentProcessGroupRef: {root: true}
  source: {type: InputPort, name: from-edge}
  destination: {type: Funnel, name: central-sink}
  deletionPolicy: Delete
YAML
wait_ready nificonnection central-ingest 30
kubectl --context "${ctx}" -n "${ns}" patch nifiinputport from-edge --type=merge -p '{"spec":{"state":"Running"}}' >/dev/null
wait_ready nifiinputport from-edge 30
echo "  central input port 'from-edge' is Running."

echo "edge: creating a GenerateFlowFile source and a remote process group targeting central..."
kubectl --context "${ctx}" -n "${ns}" apply -f - <<YAML >/dev/null
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessor
metadata: {name: generate}
spec:
  clusterRef: {name: edge}
  parentProcessGroupRef: {root: true}
  type: org.apache.nifi.processors.standard.GenerateFlowFile
  position: {x: 0, "y": 200}
  scheduling: {period: 60 sec}
  state: Stopped
  deletionPolicy: Delete
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRemoteProcessGroup
metadata: {name: to-central}
spec:
  clusterRef: {name: edge}
  parentProcessGroupRef: {root: true}
  targetUris: ["http://${central_fqdn}:8080/nifi"]
  transportProtocol: HTTP
  position: {x: 400, "y": 200}
  inputPorts:
    - name: from-edge
      transmitting: true
      concurrentTasks: 2
      useCompression: true
      batchCount: 500
      batchSize: 4 MB
      batchDuration: 5 sec
  deletionPolicy: Delete
YAML
wait_ready nifiprocessor generate 30

echo "edge: waiting for the RPG to discover central's 'from-edge' port..."
rid=""
for _ in $(seq 1 40); do
  rid="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  disc="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.discoveredInputPorts[?(@.name=="from-edge")].nifiId}' 2>/dev/null || true)"
  [ -n "${rid}" ] && [ -n "${disc}" ] && break
  sleep 5
done
[ -n "${disc}" ] || { echo "RPG did not discover the 'from-edge' remote input port" >&2; kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o yaml >&2; exit 1; }
echo "  RPG ${rid} discovered remote input port 'from-edge' (${disc})."

echo "edge: connecting GenerateFlowFile -> remote input port 'from-edge'..."
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiConnection
metadata: {name: to-remote}
spec:
  clusterRef: {name: edge}
  parentProcessGroupRef: {root: true}
  source: {type: Processor, name: generate}
  destination: {type: RemoteInputPort, name: to-central, portName: from-edge}
  selectedRelationships: [success]
  deletionPolicy: Delete
YAML
wait_ready nificonnection to-remote 40
echo "  edge connection to the remote input port is Ready."

echo "edge: waiting for the RPG to become Ready (port configured + transmitting)..."
wait_ready nifiremoteprocessgroup to-central 40

echo "Asserting the remote port config + transmission round-tripped (read from edge's NiFi API)..."
ok=0
for _ in $(seq 1 24); do
  port_json="$(nifi_curl edge-nifi-0 "${edge_fqdn}" "remote-process-groups/${rid}" | tr ',' '\n' | grep -A0 . || true)"
  full="$(nifi_curl edge-nifi-0 "${edge_fqdn}" "remote-process-groups/${rid}" || true)"
  connected="$(echo "${full}" | grep -o '"from-edge"[^}]*"connected":[a-z]*' | grep -o '"connected":[a-z]*' | head -1 | cut -d: -f2)"
  # Fallback simple checks on the whole entity JSON for the input port block.
  echo "${full}" | grep -q '"useCompression":true' && \
  echo "${full}" | grep -q '"concurrentlySchedulableTaskCount":2' && \
  echo "${full}" | grep -q '"count":500' && \
  echo "${full}" | grep -q '"size":"4 MB"' && \
  echo "${full}" | grep -q '"duration":"5 sec"' && \
  echo "${full}" | grep -q '"transmitting":true' && \
  echo "${full}" | grep -q '"connected":true' && { ok=1; break; }
  sleep 5
done
if [ "${ok}" != "1" ]; then
  echo "remote input port did not reach the expected connected/transmitting/config state" >&2
  nifi_curl edge-nifi-0 "${edge_fqdn}" "remote-process-groups/${rid}" >&2 || true
  exit 1
fi
echo "  remote input port 'from-edge' is connected=true, transmitting=true, concurrentTasks=2, useCompression=true, batch=500/4 MB/5 sec."

echo "PASS: NiFiControl wired site-to-site end-to-end (discover -> configure -> connect -> transmit) with no NiFi UI action."
