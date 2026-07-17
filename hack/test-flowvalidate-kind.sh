#!/usr/bin/env bash
# End-to-end test of NiFiFlowDeployment spec.validateOnly (dry-run flow validation) against a real
# Apache NiFi 2.10 in kind. A validate-only deployment imports the resolved flow into a temporary,
# detached process group, enables its controller services, inspects component validity, records the
# result in status.validationResult, and deletes the temporary group -- without wiring anything into
# the live flow. This exercises the whole path live:
#
#   1. validateOnly with an INVALID flow (a PutFile with its required Directory unset) -> Ready=false,
#      status.validationResult.valid=false, the offending component reported, no live PG created, and
#      the temporary validation process group is cleaned up (no leak),
#   2. validateOnly with a VALID flow (a GenerateFlowFile with its relationship auto-terminated) ->
#      Ready=true, status.validationResult.valid=true, again no live PG and no leaked temporary group,
#   3. re-reconcile of the valid deployment -> no new temporary group is imported (churn guard).
#
# Requires kind, kubectl, docker, go, helm. Loads the host's cached NiFi image. Run:
#   make integration-flowvalidate-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-flowvalidate}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-flowvalidate"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
base="http://val-nifi-0.val-nifi-headless.${namespace}.svc:8080/nifi-api"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nififlowdeployment --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster val --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster val --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

nifi_get() { kubectl --context "${ctx}" -n "${namespace}" exec val-nifi-0 -c nifi -- curl -fsS "${base}$1" 2>/dev/null || true; }
nifi_field() { nifi_get "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | cut -d'"' -f4; }

# cr_field <kind> <name> <jsonpath> -> a status field, retried until non-empty.
cr_field() {
  local v=""
  for _ in $(seq 1 40); do
    v="$(kubectl --context "${ctx}" -n "${namespace}" get "$1" "$2" -o jsonpath="$3" 2>/dev/null || true)"
    [ -n "${v}" ] && { echo "${v}"; return 0; }
    sleep 5
  done
  echo ""; return 1
}

# root_has_pg_named <name> -> 0 if a process group with that name exists directly under root.
root_has_pg_named() { nifi_get "/flow/process-groups/root" | grep -q "\"name\":\"$1\""; }

diag() {
  echo "==== flow deployment status/conditions ====" >&2
  for n in $(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment -o jsonpath='{.items[*].metadata.name}' 2>/dev/null); do
    echo "  nififlowdeployment/${n} ready=$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment "${n}" -o jsonpath='{.status.ready}' 2>/dev/null) phase=$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment "${n}" -o jsonpath='{.status.validationPhase}' 2>/dev/null)" >&2
    kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment "${n}" -o jsonpath='{"    result: "}{.status.validationResult}{"\n"}' 2>/dev/null >&2 || true
    kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment "${n}" -o jsonpath='{range .status.conditions[*]}    {.type}={.status} {.reason}: {.message}{"\n"}{end}' 2>/dev/null >&2 || true
  done
  echo "==== root process groups ====" >&2
  nifi_get "/flow/process-groups/root" | grep -o '"name":"[^"]*"' >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 | grep -vE "goroutine|\.go:[0-9]|controller-runtime@|^\s*>" >&2 || true
  if [ -n "${DIAG_FULL_LOG:-}" ]; then
    kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=200 > "${DIAG_FULL_LOG}" 2>&1 || true
    kubectl --context "${ctx}" -n "${opns}" get pods -o wide >> "${DIAG_FULL_LOG}" 2>&1 || true
  fi
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
metadata: {name: val}
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
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster val -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

root_children_before="$(nifi_get "/flow/process-groups/root" | grep -o '"processGroupFlow"' | wc -l)"

echo "Phase 1: validateOnly with an INVALID flow (PutFile with no Directory) -> reported invalid, no leak..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata: {name: validate-invalid}
spec:
  clusterRef: {name: val}
  validateOnly: true
  source:
    version: "1"
    inline:
      snapshot:
        flowContents:
          identifier: "00000000-0000-0000-0000-000000000101"
          name: placeholder
          componentType: PROCESS_GROUP
          position: {x: 0, "y": 0}
          controllerServices: []
          processGroups: []
          remoteProcessGroups: []
          inputPorts: []
          outputPorts: []
          connections: []
          funnels: []
          labels: []
          processors:
            # A complete VersionedProcessor (NiFi 500s the import if scheduling/style/descriptor
            # fields are missing). PutFile's required "Directory" is unset -> INVALID.
            - identifier: "00000000-0000-0000-0000-0000000001aa"
              name: put-no-dir
              type: org.apache.nifi.processors.standard.PutFile
              bundle: {group: org.apache.nifi, artifact: nifi-standard-nar, version: 2.10.0}
              scheduledState: ENABLED
              componentType: PROCESSOR
              position: {x: 0, "y": 0}
              properties: {}
              propertyDescriptors: {}
              style: {}
              autoTerminatedRelationships: [success, failure]
              concurrentlySchedulableTaskCount: 1
              schedulingStrategy: TIMER_DRIVEN
              schedulingPeriod: "0 sec"
              executionNode: ALL
              penaltyDuration: "30 sec"
              yieldDuration: "1 sec"
              bulletinLevel: WARN
              runDurationMillis: 0
  target:
    parentProcessGroupRef: {root: true}
    processGroupName: should-never-exist-invalid
  deletionPolicy: Orphan
YAML
# The Ready condition is expected to go False with reason ValidationFailed once validation completes.
result_valid=""
for _ in $(seq 1 40); do
  result_valid="$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-invalid -o jsonpath='{.status.validationResult.valid}' 2>/dev/null || true)"
  [ -n "${result_valid}" ] && break
  sleep 5
done
[ "${result_valid}" = "false" ] || { echo "invalid flow: validationResult.valid=${result_valid:-<empty>}, want false" >&2; diag; exit 1; }
# .status.ready is omitempty (absent when false), so assert the Ready condition is False.
[ "$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-invalid -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}')" = "False" ] || { echo "invalid flow's Ready condition should be False" >&2; diag; exit 1; }
invalid_count="$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-invalid -o jsonpath='{.status.validationResult.invalidCount}')"
[ "${invalid_count:-0}" -ge 1 ] || { echo "invalid flow: invalidCount=${invalid_count}, want >=1" >&2; diag; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-invalid -o jsonpath='{.status.validationResult.invalidComponents[*].type}' | grep -q "PutFile" || { echo "invalid component (PutFile) not reported" >&2; diag; exit 1; }
[ -z "$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-invalid -o jsonpath='{.status.processGroupId}')" ] || { echo "validateOnly must not create a live process group" >&2; diag; exit 1; }
! root_has_pg_named "should-never-exist-invalid" || { echo "validateOnly created a live process group under root" >&2; diag; exit 1; }
# The temporary validation group must be cleaned up.
gone=0
for _ in $(seq 1 24); do
  ! root_has_pg_named "nificontrol-validate-validate-invalid" && { gone=1; break; }
  sleep 5
done
[ "${gone}" = "1" ] || { echo "temporary validation process group was not cleaned up (leak)" >&2; diag; exit 1; }
echo "    invalid flow reported (invalidCount=${invalid_count}, PutFile flagged), no live PG, temporary group cleaned up."

echo "Phase 2: validateOnly with a VALID flow (GenerateFlowFile, relationship auto-terminated) -> Ready, no leak..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata: {name: validate-valid}
spec:
  clusterRef: {name: val}
  validateOnly: true
  source:
    version: "1"
    inline:
      snapshot:
        flowContents:
          identifier: "00000000-0000-0000-0000-000000000201"
          name: placeholder
          componentType: PROCESS_GROUP
          position: {x: 0, "y": 0}
          controllerServices: []
          processGroups: []
          remoteProcessGroups: []
          inputPorts: []
          outputPorts: []
          connections: []
          funnels: []
          labels: []
          processors:
            # GenerateFlowFile with its only relationship auto-terminated and all properties at
            # their defaults -> VALID.
            - identifier: "00000000-0000-0000-0000-0000000002aa"
              name: gen
              type: org.apache.nifi.processors.standard.GenerateFlowFile
              bundle: {group: org.apache.nifi, artifact: nifi-standard-nar, version: 2.10.0}
              scheduledState: ENABLED
              componentType: PROCESSOR
              position: {x: 0, "y": 0}
              properties: {}
              propertyDescriptors: {}
              style: {}
              autoTerminatedRelationships: [success]
              concurrentlySchedulableTaskCount: 1
              schedulingStrategy: TIMER_DRIVEN
              schedulingPeriod: "0 sec"
              executionNode: ALL
              penaltyDuration: "30 sec"
              yieldDuration: "1 sec"
              bulletinLevel: WARN
              runDurationMillis: 0
  target:
    parentProcessGroupRef: {root: true}
    processGroupName: should-never-exist-valid
  deletionPolicy: Orphan
YAML
[ "$(cr_field nififlowdeployment validate-valid '{.status.ready}')" = "true" ] || { echo "valid flow did not become Ready" >&2; diag; exit 1; }
[ "$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-valid -o jsonpath='{.status.validationResult.valid}')" = "true" ] || { echo "valid flow: validationResult.valid != true" >&2; diag; exit 1; }
[ "$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-valid -o jsonpath='{.status.validationResult.invalidCount}')" = "0" ] || { echo "valid flow: invalidCount != 0" >&2; diag; exit 1; }
[ -z "$(kubectl --context "${ctx}" -n "${namespace}" get nififlowdeployment validate-valid -o jsonpath='{.status.processGroupId}')" ] || { echo "validateOnly must not create a live process group" >&2; diag; exit 1; }
! root_has_pg_named "should-never-exist-valid" || { echo "validateOnly created a live process group under root" >&2; diag; exit 1; }
gone=0
for _ in $(seq 1 24); do
  ! root_has_pg_named "nificontrol-validate-validate-valid" && { gone=1; break; }
  sleep 5
done
[ "${gone}" = "1" ] || { echo "temporary validation process group was not cleaned up (leak)" >&2; diag; exit 1; }
echo "    valid flow reported Ready with validationResult.valid=true, no live PG, temporary group cleaned up."

echo "Phase 3: churn guard -> a settled validation does not re-import on subsequent reconciles..."
# Nudge a reconcile (annotate) and confirm no temporary validation group reappears under root.
kubectl --context "${ctx}" -n "${namespace}" annotate nififlowdeployment validate-valid nificontrol.test/nudge="$(date +%s)" --overwrite >/dev/null
leaked=0
for _ in $(seq 1 6); do
  root_has_pg_named "nificontrol-validate-validate-valid" && { leaked=1; break; }
  sleep 5
done
[ "${leaked}" = "0" ] || { echo "churn guard failed: a temporary validation group reappeared after re-reconcile" >&2; diag; exit 1; }
root_children_after="$(nifi_get "/flow/process-groups/root" | grep -o '"id"' | wc -l)"
echo "    no re-import; root canvas is free of temporary validation groups."

echo "PASS: spec.validateOnly validates a flow in a throwaway process group against real NiFi 2.10 -- invalid flows are reported (Ready=false) and valid flows pass (Ready=true), with no live process group and no leaked temporary group."
