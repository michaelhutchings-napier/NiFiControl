#!/usr/bin/env bash
# End-to-end test of the canvas/tenant CRDs against a real Apache NiFi 2.10 in kind. These kinds
# reconcile by driving NiFi's REST API (create/configure/enable components on the canvas), which
# is exactly where unit tests are weakest — REST contract drift, enable/disable ordering, cross-CR
# reference resolution, and ID/revision handling only show up against a live NiFi. This one harness
# exercises several kinds on a single insecure single-node cluster (the operator runs in-cluster so
# it can reach NiFi over cluster DNS), asserting each against NiFi's own REST API:
#
#   1. NiFiParameterContext -> created in NiFi with its parameters,
#   2. NiFiProcessGroup at root, bound to that parameter context (cross-CR reference),
#   3. NiFiControllerService (a record reader) in that group: created DISABLED, then Enabled -> ENABLED,
#   4. NiFiLabel at root -> created with its text,
#   5. NiFiOutputPort in that group -> created STOPPED,
#   6. delete every canvas CR -> all removed from NiFi and finalizers clear (no deadlock).
#
# Requires kind, kubectl, docker, go, helm. Loads the host's cached NiFi image into the node. Run:
#   make integration-canvas-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-canvas}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-canvas"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
base="http://cv-nifi-0.cv-nifi-headless.${namespace}.svc:8080/nifi-api"
cs_type="org.apache.nifi.json.JsonTreeReader"

cleanup() {
  for k in nifioutputport nifilabel nificontrollerservice nifiprocessgroup nifiparametercontext; do
    kubectl --context "${ctx}" -n "${namespace}" delete "${k}" --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster cv --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster cv --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# nifi_get <api-path> -> raw JSON from NiFi (queried from inside the pod over cluster DNS).
nifi_get() { kubectl --context "${ctx}" -n "${namespace}" exec cv-nifi-0 -c nifi -- curl -fsS "${base}$1" 2>/dev/null || true; }
# nifi_field <api-path> <json-key> -> first "key":"VALUE" scalar (mirrors the reportingtask harness).
nifi_field() { nifi_get "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }
nifi_code() { kubectl --context "${ctx}" -n "${namespace}" exec cv-nifi-0 -c nifi -- curl -s -o /dev/null -w '%{http_code}' "${base}$1" 2>/dev/null || true; }

# cr_ready <kind> <name> -> waits for .status.ready=true and echoes .status.nifiId.
cr_ready() {
  local kind="$1" name="$2" id="" ready=""
  for _ in $(seq 1 30); do
    id="$(kubectl --context "${ctx}" -n "${namespace}" get "${kind}" "${name}" -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
    ready="$(kubectl --context "${ctx}" -n "${namespace}" get "${kind}" "${name}" -o jsonpath='{.status.ready}' 2>/dev/null || true)"
    [ -n "${id}" ] && [ "${ready}" = "true" ] && { echo "${id}"; return 0; }
    sleep 5
  done
  echo ""; return 1
}

canvas_diag() {
  echo "==== canvas CR status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifiparametercontext,nifiprocessgroup,nificontrollerservice,nifilabel,nifioutputport -o wide 2>&1 >&2 || true
  echo "==== canvas CR conditions (reason: message) ====" >&2
  for k in nifiparametercontext nifiprocessgroup nificontrollerservice nifilabel nifioutputport; do
    for n in $(kubectl --context "${ctx}" -n "${namespace}" get "${k}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      echo "  ${k}/${n}:" >&2
      kubectl --context "${ctx}" -n "${namespace}" get "${k}" "${n}" \
        -o jsonpath='{range .status.conditions[*]}    {.type}={.status} {.reason}: {.message}{"\n"}{end}' 2>/dev/null >&2 || true
    done
  done
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

echo "Deploying the operator (helm)..."
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
metadata: {name: cv}
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
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster cv -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

echo "Phase 1: NiFiParameterContext -> created in NiFi with its parameters..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterContext
metadata: {name: pcx}
spec:
  clusterRef: {name: cv}
  description: canvas suite parameter context
  parameters:
    - {name: batchSize, value: "500"}
    - {name: greeting, value: "hello"}
  deletionPolicy: Delete
YAML
pc_id="$(cr_ready nifiparametercontext pcx)" || { echo "parameter context not ready" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/parameter-contexts/${pc_id}" name)" = "pcx" ] || { echo "parameter context name mismatch in NiFi" >&2; canvas_diag; exit 1; }
nifi_get "/parameter-contexts/${pc_id}" | grep -q '"name":"batchSize"' || { echo "parameter batchSize not present in NiFi" >&2; canvas_diag; exit 1; }
echo "    parameter context ${pc_id} present in NiFi with its parameters."

echo "Phase 2: NiFiProcessGroup at root, bound to the parameter context..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - <<'YAML' >/dev/null
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessGroup
metadata: {name: grp}
spec:
  clusterRef: {name: cv}
  parentProcessGroupRef: {root: true}
  displayName: canvas-suite-group
  parameterContextRef: {name: pcx}
  deletionPolicy: Delete
YAML
pg_id="$(cr_ready nifiprocessgroup grp)" || { echo "process group not ready" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/process-groups/${pg_id}" name)" = "canvas-suite-group" ] || { echo "process group name mismatch in NiFi" >&2; canvas_diag; exit 1; }
bound="$(nifi_get "/process-groups/${pg_id}" | grep -o "\"parameterContext\":{[^}]*\"id\":\"${pc_id}\"" | head -1)"
[ -n "${bound}" ] || { echo "process group not bound to parameter context ${pc_id}" >&2; canvas_diag; exit 1; }
echo "    process group ${pg_id} present and bound to parameter context ${pc_id}."

echo "Phase 3: NiFiControllerService in that group: DISABLED, then Enabled -> ENABLED..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - <<YAML >/dev/null
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiControllerService
metadata: {name: reader}
spec:
  clusterRef: {name: cv}
  parentProcessGroupRef: {name: grp}
  type: ${cs_type}
  state: Disabled
  deletionPolicy: Delete
YAML
cs_id="$(cr_ready nificontrollerservice reader)" || { echo "controller service not ready" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/controller-services/${cs_id}" state)" = "DISABLED" ] || { echo "controller service not DISABLED at create" >&2; canvas_diag; exit 1; }
echo "    controller service ${cs_id} created DISABLED; enabling..."
kubectl --context "${ctx}" -n "${namespace}" patch nificontrollerservice reader --type=merge -p '{"spec":{"state":"Enabled"}}' >/dev/null
enabled=0
for _ in $(seq 1 24); do
  [ "$(nifi_field "/controller-services/${cs_id}" state)" = "ENABLED" ] && { enabled=1; break; }
  sleep 5
done
[ "${enabled}" = "1" ] || { echo "controller service did not reach ENABLED" >&2; canvas_diag; exit 1; }
echo "    controller service ${cs_id} is ENABLED."

echo "Phase 4: NiFiLabel at root -> created with its text..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - <<'YAML' >/dev/null
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiLabel
metadata: {name: note}
spec:
  clusterRef: {name: cv}
  parentProcessGroupRef: {root: true}
  text: "canvas suite label"
  position: {x: 100, "y": 100}   # quote y: YAML 1.1 reads a bare y as boolean true
  width: 200
  height: 80
  deletionPolicy: Delete
YAML
lb_id="$(cr_ready nifilabel note)" || { echo "label not ready" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/labels/${lb_id}" label)" = "canvas suite label" ] || { echo "label text mismatch in NiFi" >&2; canvas_diag; exit 1; }
echo "    label ${lb_id} present with its text."

echo "Phase 5: NiFiOutputPort in that group -> created STOPPED..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - <<'YAML' >/dev/null
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiOutputPort
metadata: {name: out}
spec:
  clusterRef: {name: cv}
  parentProcessGroupRef: {name: grp}
  displayName: canvas-suite-out
  position: {x: 300, "y": 300}   # quote y: YAML 1.1 reads a bare y as boolean true
  state: Stopped
  deletionPolicy: Delete
YAML
op_id="$(cr_ready nifioutputport out)" || { echo "output port not ready" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/output-ports/${op_id}" name)" = "canvas-suite-out" ] || { echo "output port name mismatch in NiFi" >&2; canvas_diag; exit 1; }
[ "$(nifi_field "/output-ports/${op_id}" state)" = "STOPPED" ] || { echo "output port not STOPPED" >&2; canvas_diag; exit 1; }
echo "    output port ${op_id} present and STOPPED."

echo "Phase 6: delete every canvas CR -> all removed from NiFi, finalizers clear..."
# Disable the controller service so it can be deleted; remove children before the group, and the
# group before the parameter context it binds. Delete non-blocking, then verify via NiFi + the CR
# finalizers so a stuck deletion surfaces diagnostics instead of an opaque kubectl timeout.
kubectl --context "${ctx}" -n "${namespace}" patch nificontrollerservice reader --type=merge -p '{"spec":{"state":"Disabled"}}' >/dev/null
sleep 5
kubectl --context "${ctx}" -n "${namespace}" delete nifioutputport/out nificontrollerservice/reader nifilabel/note --wait=false >/dev/null
kubectl --context "${ctx}" -n "${namespace}" delete nifiprocessgroup grp --wait=false >/dev/null
kubectl --context "${ctx}" -n "${namespace}" delete nifiparametercontext pcx --wait=false >/dev/null
declare -A want=( ["/output-ports/${op_id}"]=nifioutputport/out ["/controller-services/${cs_id}"]=nificontrollerservice/reader ["/labels/${lb_id}"]=nifilabel/note ["/process-groups/${pg_id}"]=nifiprocessgroup/grp ["/parameter-contexts/${pc_id}"]=nifiparametercontext/pcx )
for api in "${!want[@]}"; do
  cr="${want[$api]}"
  gone=0
  for _ in $(seq 1 24); do
    code="$(nifi_code "${api}")"
    cr_present="$(kubectl --context "${ctx}" -n "${namespace}" get "${cr}" --ignore-not-found -o name 2>/dev/null || true)"
    [ "${code}" = "404" ] && [ -z "${cr_present}" ] && { gone=1; break; }
    sleep 5
  done
  [ "${gone}" = "1" ] || { echo "deletion stuck: ${cr} (NiFi ${api} http=${code:-?}, CR present='${cr_present:-no}')" >&2; canvas_diag; exit 1; }
done
echo "    all canvas components removed from NiFi; CR finalizers cleared."

echo "PASS: canvas/tenant CRDs (ParameterContext, ProcessGroup, ControllerService, Label, OutputPort) create, configure, enable, cross-reference, and delete cleanly against real NiFi 2.10."
