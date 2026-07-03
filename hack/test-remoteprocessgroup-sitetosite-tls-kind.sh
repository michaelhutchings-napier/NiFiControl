#!/usr/bin/env bash
# End-to-end test of declarative NiFi site-to-site over MUTUAL TLS. Two operator-managed NiFi 2.10
# clusters ("central" and "edge") run with internalTLS (HTTPS + certificate auth), share a single
# cert-manager CA, and are wired together purely through NiFiControl CRDs — no NiFi UI action, and
# no insecure HTTP anywhere. This is the production-shaped counterpart of
# test-remoteprocessgroup-sitetosite-kind.sh (which uses plaintext HTTP).
#
# Why a shared CA: in self-signed mode each cluster mints its own CA, so neither would trust the
# other's server/node certificate. Instead both clusters reference one cert-manager CA ClusterIssuer
# (internalTLS.issuerRef), so every server/node/operator certificate chains to the same root and
# each cluster's truststore trusts the other automatically.
#
# What proves the secure path (beyond the insecure test):
#   1. Cross-cluster mTLS trust — edge's NiFi node authenticates to central over HTTPS with its own
#      client certificate (CN=edge-node), verified against the shared CA.
#   2. Declarative S2S authorization — on central, NiFiControl grants CN=edge-node the two NiFi
#      site-to-site policies (retrieve site-to-site details + receive data on the input port). The
#      RPG can only DISCOVER central's "from-edge" port (status.discoveredInputPorts[].exists=true)
#      if that mTLS handshake AND authorization both succeed — so discovery itself is the proof.
#   3. Actual encrypted data transfer — GenerateFlowFile on edge transmits over secure S2S; the test
#      asserts (via an mTLS call using central's operator client certificate) that flow files landed
#      in central's ingest queue.
#
# Requires kind, kubectl, docker, go, helm, curl, base64. Run: make integration-remoteprocessgroup-s2s-tls-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1; keep the test
# namespace for inspection with KEEP=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-s2s-tls}"
ctx="kind-${cluster}"
opns="nificontrol-system"
ns="nifi-s2s-tls"
cmns="cert-manager"
cm_version="${CERT_MANAGER_VERSION:-v1.16.2}"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

# central is reached by both the operator (mTLS REST) and edge's NiFi node (secure S2S) at its
# Service DNS name, which is covered by the server certificate SANs and the nifi.web.proxy.host
# allow-list the operator renders for TLS clusters.
central_svc="central-nifi.${ns}.svc"

reset_namespace() {
  local waitflag="$1"
  kubectl --context "${ctx}" get ns "${ns}" >/dev/null 2>&1 || return 0
  for kind in nificonnection nifiremoteprocessgroup nifiprocessor nifiinputport nifioutputport nififunnel nifilabel nificontrollerservice nifiprocessgroup nifipolicy nifiusergroup nifiuser nificluster; do
    for r in $(kubectl --context "${ctx}" -n "${ns}" get "${kind}" -o name 2>/dev/null); do
      kubectl --context "${ctx}" -n "${ns}" patch "${r}" --type=merge -p '{"metadata":{"finalizers":null}}' >/dev/null 2>&1 || true
    done
  done
  kubectl --context "${ctx}" delete namespace "${ns}" --wait="${waitflag}" --timeout=180s >/dev/null 2>&1 || true
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

# inject_operator_client_cert copies a cluster's operator client certificate (the NiFi initial
# admin identity, PEM form) into the given NiFi pod so we can make authenticated mTLS calls to the
# NiFi REST API from inside the cluster, exactly as the operator does. The PEM material is decoded
# on the host and copied with `kubectl cp` (piping decoded bytes through `kubectl exec -i` truncates
# them).
inject_operator_client_cert() {
  local cname="$1" pod="$2"
  local secret tmp
  secret="$(kubectl --context "${ctx}" -n "${ns}" get nificluster "${cname}" -o jsonpath='{.status.tls.clientSecretName}')"
  [ -n "${secret}" ] || { echo "cluster ${cname} has no status.tls.clientSecretName" >&2; return 1; }
  tmp="$(mktemp -d)"
  for key in tls.crt tls.key ca.crt; do
    # go-template (not jsonpath) so the dotted secret key "tls.crt" is read as one key rather than a
    # nested path tls -> crt, which would yield an empty file and an unusable client certificate.
    kubectl --context "${ctx}" -n "${ns}" get secret "${secret}" -o "go-template={{index .data \"${key}\"}}" | base64 -d > "${tmp}/${key}"
    kubectl --context "${ctx}" -n "${ns}" cp "${tmp}/${key}" "${pod}:/tmp/op-${key}" -c nifi >/dev/null
  done
  rm -rf "${tmp}"
}

# mtls_curl <pod> <host> <path> -> authenticated GET against the NiFi HTTPS API using the operator
# client certificate previously injected by inject_operator_client_cert.
mtls_curl() {
  local pod="$1" host="$2" path="$3"
  kubectl --context "${ctx}" -n "${ns}" exec "${pod}" -c nifi -- \
    curl -fsS --cacert /tmp/op-ca.crt --cert /tmp/op-tls.crt --key /tmp/op-tls.key \
    "https://${host}:8443/nifi-api/${path}" 2>/dev/null
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

echo "Installing cert-manager ${cm_version} (managed internal TLS depends on it)..."
kubectl --context "${ctx}" apply -f "https://github.com/cert-manager/cert-manager/releases/download/${cm_version}/cert-manager.yaml" >/dev/null
for dep in cert-manager cert-manager-webhook cert-manager-cainjector; do
  kubectl --context "${ctx}" -n "${cmns}" rollout status "deploy/${dep}" --timeout=180s
done

echo "Creating one shared cert-manager CA so both clusters trust each other..."
kubectl --context "${ctx}" apply -f - >/dev/null <<YAML
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: s2s-selfsigned}
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: s2s-ca, namespace: ${cmns}}
spec:
  isCA: true
  commonName: nificontrol-s2s-ca
  secretName: s2s-ca-tls
  duration: 8760h
  privateKey: {algorithm: RSA, size: 2048}
  issuerRef: {name: s2s-selfsigned, kind: ClusterIssuer, group: cert-manager.io}
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata: {name: s2s-ca}
spec:
  ca: {secretName: s2s-ca-tls}
YAML
# Wait for the shared CA to be issued before clusters ask it to sign their certificates.
kubectl --context "${ctx}" -n "${cmns}" wait --for=condition=Ready certificate/s2s-ca --timeout=120s

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

echo "Ensuring a clean ${ns} namespace..."
reset_namespace true
kubectl --context "${ctx}" create namespace "${ns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Provisioning two internal-TLS NiFiClusters (central + edge) sharing the CA..."
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
  internalTLS:
    enabled: true
    issuerRef: {name: s2s-ca, kind: ClusterIssuer, group: cert-manager.io}
  additionalEnv:
    # For HTTP-over-HTTPS site-to-site the peer host central advertises to edge must be resolvable
    # and inside central's proxy-host allow-list; the Service DNS is both.
    - {name: NIFI_REMOTE_INPUT_HOST, value: ${central_svc}}
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
  internalTLS:
    enabled: true
    issuerRef: {name: s2s-ca, kind: ClusterIssuer, group: cert-manager.io}
YAML
wait_ready nificluster central 180
wait_ready nificluster edge 180
echo "  both internal-TLS NiFiClusters are Ready."

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
wait_ready nifiinputport from-edge 40
wait_ready nififunnel central-sink 40
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
wait_ready nificonnection central-ingest 40
kubectl --context "${ctx}" -n "${ns}" patch nifiinputport from-edge --type=merge -p '{"spec":{"state":"Running"}}' >/dev/null
wait_ready nifiinputport from-edge 40
central_port_id="$(kubectl --context "${ctx}" -n "${ns}" get nifiinputport from-edge -o jsonpath='{.status.nifiId}')"
[ -n "${central_port_id}" ] || { echo "central input port has no status.nifiId" >&2; exit 1; }
echo "  central input port 'from-edge' (${central_port_id}) is Running."

echo "central: authorizing edge's node identity (CN=edge-node) for site-to-site..."
# Two NiFi policies are required for an input-port S2S receive:
#   /site-to-site (read)                          -> "retrieve site-to-site details"
#   /data-transfer/input-ports/<id> (write)       -> "receive data via site-to-site"
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata: {name: edge-node}
spec:
  clusterRef: {name: central}
  identity: "CN=edge-node"
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: edge-s2s-details}
spec:
  clusterRef: {name: central}
  resource: /site-to-site
  action: read
  userRefs: [{name: edge-node}]
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: edge-receive-from-edge}
spec:
  clusterRef: {name: central}
  resource: /data-transfer/input-ports/${central_port_id}
  action: write
  userRefs: [{name: edge-node}]
YAML
wait_ready nifiuser edge-node 40
wait_ready nifipolicy edge-s2s-details 40
wait_ready nifipolicy edge-receive-from-edge 40
echo "  CN=edge-node is authorized to retrieve site-to-site details and receive data on 'from-edge'."

echo "edge: creating a GenerateFlowFile source and a remote process group targeting central over HTTPS..."
kubectl --context "${ctx}" -n "${ns}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessor
metadata: {name: generate}
spec:
  clusterRef: {name: edge}
  parentProcessGroupRef: {root: true}
  type: org.apache.nifi.processors.standard.GenerateFlowFile
  position: {x: 0, "y": 200}
  scheduling: {period: 1 sec}
  state: Stopped
  deletionPolicy: Delete
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRemoteProcessGroup
metadata: {name: to-central}
spec:
  clusterRef: {name: edge}
  parentProcessGroupRef: {root: true}
  targetUris: ["https://${central_svc}:8443/nifi"]
  transportProtocol: HTTP
  position: {x: 400, "y": 200}
  inputPorts:
    - name: from-edge
      transmitting: true
      concurrentTasks: 2
      useCompression: true
  deletionPolicy: Delete
YAML
wait_ready nifiprocessor generate 40

echo "edge: waiting for the RPG to discover central's 'from-edge' port over secure S2S..."
rid=""
exists=""
for _ in $(seq 1 48); do
  rid="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  exists="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.discoveredInputPorts[?(@.name=="from-edge")].exists}' 2>/dev/null || true)"
  [ -n "${rid}" ] && [ "${exists}" = "true" ] && break
  sleep 5
done
[ "${exists}" = "true" ] || { echo "RPG did not discover 'from-edge' over secure S2S (mTLS trust or authorization failed)" >&2; kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o yaml >&2; exit 1; }
echo "  RPG ${rid} discovered remote input port 'from-edge' (exists=true) — mTLS handshake + authorization succeeded."

echo "edge: connecting GenerateFlowFile -> remote input port 'from-edge' and starting the source..."
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
kubectl --context "${ctx}" -n "${ns}" patch nifiprocessor generate --type=merge -p '{"spec":{"state":"Running"}}' >/dev/null
wait_ready nifiprocessor generate 40
echo "  edge connection to the remote input port is Ready and GenerateFlowFile is Running."

echo "edge: waiting for the RPG to become Ready (port configured + transmitting)..."
wait_ready nifiremoteprocessgroup to-central 48

echo "Asserting connected + transmitting from the RPG status..."
ok=0
for _ in $(seq 1 24); do
  connected="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.discoveredInputPorts[?(@.name=="from-edge")].connected}' 2>/dev/null || true)"
  transmitting="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.discoveredInputPorts[?(@.name=="from-edge")].transmitting}' 2>/dev/null || true)"
  tstatus="$(kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o jsonpath='{.status.transmissionStatus}' 2>/dev/null || true)"
  [ "${connected}" = "true" ] && [ "${transmitting}" = "true" ] && [ "${tstatus}" = "Transmitting" ] && { ok=1; break; }
  sleep 5
done
[ "${ok}" = "1" ] || { echo "remote input port did not reach connected=true / transmitting=true" >&2; kubectl --context "${ctx}" -n "${ns}" get nifiremoteprocessgroup to-central -o yaml >&2; exit 1; }
echo "  remote input port 'from-edge' is connected=true, transmitting=true, transmissionStatus=Transmitting."

echo "Asserting flow files actually crossed the secure link (reading central over mTLS)..."
inject_operator_client_cert central central-nifi-0
ingest_id="$(kubectl --context "${ctx}" -n "${ns}" get nificonnection central-ingest -o jsonpath='{.status.nifiId}')"
[ -n "${ingest_id}" ] || { echo "central-ingest connection has no status.nifiId" >&2; exit 1; }
delivered=0
for _ in $(seq 1 36); do
  queued="$(mtls_curl central-nifi-0 "${central_svc}" "connections/${ingest_id}" | tr ',' '\n' | grep -o '"flowFilesQueued":[0-9]*' | head -1 | cut -d: -f2 || true)"
  [ -n "${queued}" ] && [ "${queued}" -gt 0 ] 2>/dev/null && { delivered="${queued}"; break; }
  sleep 5
done
[ "${delivered}" -gt 0 ] 2>/dev/null || { echo "no flow files were received on central over secure S2S" >&2; mtls_curl central-nifi-0 "${central_svc}" "connections/${ingest_id}" >&2 || true; exit 1; }
echo "  central received ${delivered} flow file(s) via mutual-TLS site-to-site."

echo "PASS: NiFiControl wired site-to-site over mutual TLS end-to-end (shared CA -> authorize -> discover -> connect -> transmit -> deliver) with no NiFi UI action."
