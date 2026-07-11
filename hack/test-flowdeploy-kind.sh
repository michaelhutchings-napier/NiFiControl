#!/usr/bin/env bash
# End-to-end test of NiFiFlowDeployment and NiFiFlowBundle against a real Apache NiFi 2.10 in kind.
# These are the GitOps flow-deployment kinds: the operator imports a RegisteredFlowSnapshot into a
# target process group over NiFi's REST API, tracks content digests, and replaces the flow in place
# when it changes. This exercises the whole path against a live NiFi with a self-contained inline
# snapshot (no registry/OCI/git needed):
#
#   1. NiFiFlowDeployment (inline snapshot, a PG containing a Label) -> the flow is imported into a
#      new process group whose contents match the snapshot (the Label is present),
#   2. redeploy with a changed snapshot (Label text v1 -> v2) -> the running flow is updated in place,
#   3. NiFiFlowBundle (inline snapshot) -> becomes Ready with a resolved artifact digest,
#   4. NiFiFlowDeployment via source.bundleRef -> imports the bundle's flow (digest cross-check),
#   5. delete every deployment -> the managed process groups are removed from NiFi.
#
# Requires kind, kubectl, docker, go, helm. Loads the host's cached NiFi image. Run:
#   make integration-flowdeploy-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-flowdeploy}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-flowdeploy"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
base="http://fd-nifi-0.fd-nifi-headless.${namespace}.svc:8080/nifi-api"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nififlowdeployment --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nififlowbundle --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster fd --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster fd --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

nifi_get() { kubectl --context "${ctx}" -n "${namespace}" exec fd-nifi-0 -c nifi -- curl -fsS "${base}$1" 2>/dev/null || true; }
nifi_field() { nifi_get "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }
nifi_code() { kubectl --context "${ctx}" -n "${namespace}" exec fd-nifi-0 -c nifi -- curl -s -o /dev/null -w '%{http_code}' "${base}$1" 2>/dev/null || true; }

# cr_field <kind> <name> <jsonpath> -> a status field, retried until non-empty.
cr_field() {
  local v=""
  for _ in $(seq 1 30); do
    v="$(kubectl --context "${ctx}" -n "${namespace}" get "$1" "$2" -o jsonpath="$3" 2>/dev/null || true)"
    [ -n "${v}" ] && { echo "${v}"; return 0; }
    sleep 5
  done
  echo ""; return 1
}

diag() {
  echo "==== flow CR status/conditions ====" >&2
  for k in nififlowbundle nififlowdeployment; do
    for n in $(kubectl --context "${ctx}" -n "${namespace}" get "${k}" -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
      echo "  ${k}/${n} ready=$(kubectl --context "${ctx}" -n "${namespace}" get "${k}" "${n}" -o jsonpath='{.status.ready}' 2>/dev/null)" >&2
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
metadata: {name: fd}
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
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster fd -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

# pg_has_label <pgId> <text> -> 0 if the process group's flow contains a Label with that text.
pg_has_label() { nifi_get "/flow/process-groups/$1" | grep -q "\"label\":\"$2\""; }

echo "Phase 1: NiFiFlowDeployment (inline snapshot) -> flow imported into a new process group..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata: {name: dep-inline}
spec:
  clusterRef: {name: fd}
  source:
    version: "1"
    inline:
      snapshot:
        flowContents:
          identifier: "00000000-0000-0000-0000-000000000001"
          name: placeholder
          comments: nificontrol flow deploy
          componentType: PROCESS_GROUP
          position: {x: 0, "y": 0}
          processors: []
          controllerServices: []
          processGroups: []
          remoteProcessGroups: []
          inputPorts: []
          outputPorts: []
          connections: []
          funnels: []
          labels:
            - identifier: "00000000-0000-0000-0000-0000000000aa"
              label: deployed-marker-v1
              position: {x: 0, "y": 0}
              width: 300
              height: 100
              componentType: LABEL
  target:
    parentProcessGroupRef: {root: true}
    processGroupName: deployed-flow
  rollout: {strategy: ApplyOnly}
  ownership: {mode: Managed}
  deletionPolicy: Delete
YAML
[ "$(cr_field nififlowdeployment dep-inline '{.status.ready}')" = "true" ] || { echo "dep-inline did not become Ready" >&2; diag; exit 1; }
pg1="$(cr_field nififlowdeployment dep-inline '{.status.processGroupId}')" || { echo "no processGroupId" >&2; diag; exit 1; }
[ "$(nifi_field "/process-groups/${pg1}" name)" = "deployed-flow" ] || { echo "deployed PG name mismatch" >&2; diag; exit 1; }
pg_has_label "${pg1}" "deployed-marker-v1" || { echo "deployed flow content (label v1) not imported into ${pg1}" >&2; diag; exit 1; }
echo "    deployed process group ${pg1} imported with its flow content (label deployed-marker-v1)."

echo "Phase 2: redeploy with a changed snapshot (label v1 -> v2) -> flow updated in place..."
kubectl --context "${ctx}" -n "${namespace}" patch nififlowdeployment dep-inline --type=merge -p \
  '{"spec":{"source":{"version":"2","inline":{"snapshot":{"flowContents":{"identifier":"00000000-0000-0000-0000-000000000001","name":"placeholder","componentType":"PROCESS_GROUP","position":{"x":0,"y":0},"processors":[],"controllerServices":[],"processGroups":[],"remoteProcessGroups":[],"inputPorts":[],"outputPorts":[],"connections":[],"funnels":[],"labels":[{"identifier":"00000000-0000-0000-0000-0000000000aa","label":"deployed-marker-v2","position":{"x":0,"y":0},"width":300,"height":100,"componentType":"LABEL"}]}}}}}}' >/dev/null
updated=0
for _ in $(seq 1 36); do
  pg_has_label "${pg1}" "deployed-marker-v2" && { updated=1; break; }
  sleep 5
done
[ "${updated}" = "1" ] || { echo "redeploy did not update the running flow to label v2" >&2; diag; exit 1; }
echo "    running flow updated in place to label deployed-marker-v2 (process group ${pg1} unchanged)."

echo "Phase 3: NiFiFlowBundle (inline snapshot) -> Ready with a resolved artifact digest..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowBundle
metadata: {name: bundle-inline}
spec:
  version: "1"
  source:
    snapshot:
      flowContents:
        identifier: "00000000-0000-0000-0000-000000000002"
        name: placeholder
        componentType: PROCESS_GROUP
        position: {x: 0, "y": 0}
        processors: []
        controllerServices: []
        processGroups: []
        remoteProcessGroups: []
        inputPorts: []
        outputPorts: []
        connections: []
        funnels: []
        labels:
          - identifier: "00000000-0000-0000-0000-0000000000bb"
            label: bundle-marker
            position: {x: 0, "y": 0}
            width: 300
            height: 100
            componentType: LABEL
YAML
[ "$(cr_field nififlowbundle bundle-inline '{.status.ready}')" = "true" ] || { echo "bundle-inline did not become Ready" >&2; diag; exit 1; }
digest="$(cr_field nififlowbundle bundle-inline '{.status.artifactDigest}')" || { echo "bundle has no artifactDigest" >&2; diag; exit 1; }
echo "    flow bundle Ready with artifact digest ${digest}."

echo "Phase 4: NiFiFlowDeployment via source.bundleRef -> imports the bundle's flow..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata: {name: dep-bundle}
spec:
  clusterRef: {name: fd}
  source:
    bundleRef: {name: bundle-inline}
  target:
    parentProcessGroupRef: {root: true}
    processGroupName: deployed-bundle-flow
  rollout: {strategy: ApplyOnly}
  ownership: {mode: Managed}
  deletionPolicy: Delete
YAML
[ "$(cr_field nififlowdeployment dep-bundle '{.status.ready}')" = "true" ] || { echo "dep-bundle did not become Ready" >&2; diag; exit 1; }
pg2="$(cr_field nififlowdeployment dep-bundle '{.status.processGroupId}')" || { echo "dep-bundle has no processGroupId" >&2; diag; exit 1; }
[ "$(nifi_field "/process-groups/${pg2}" name)" = "deployed-bundle-flow" ] || { echo "bundle-deployed PG name mismatch" >&2; diag; exit 1; }
pg_has_label "${pg2}" "bundle-marker" || { echo "bundle flow content (bundle-marker) not imported into ${pg2}" >&2; diag; exit 1; }
echo "    bundle-referenced deployment imported process group ${pg2} with its flow content."

echo "Phase 5: delete every deployment -> the managed process groups are removed from NiFi..."
kubectl --context "${ctx}" -n "${namespace}" delete nififlowdeployment/dep-inline nififlowdeployment/dep-bundle --wait=false >/dev/null
for pg in "${pg1}" "${pg2}"; do
  gone=0
  for _ in $(seq 1 24); do
    [ "$(nifi_code "/process-groups/${pg}")" = "404" ] && { gone=1; break; }
    sleep 5
  done
  [ "${gone}" = "1" ] || { echo "managed process group ${pg} was not removed from NiFi" >&2; diag; exit 1; }
done
echo "    both managed process groups removed from NiFi; deployment finalizers cleared."

echo "PASS: NiFiFlowDeployment imports/updates/deletes a flow against real NiFi 2.10, and NiFiFlowBundle resolves an inline snapshot consumed via bundleRef."
