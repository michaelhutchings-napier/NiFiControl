#!/usr/bin/env bash
# End-to-end test of NiFiParameterProvider against a real Apache NiFi 2.10 in kind. Parameter
# providers are controller-level components the operator must create/update/delete over NiFi's REST
# API, so (like the reportingtask harness) the operator runs IN-cluster where it can reach NiFi via
# cluster DNS. An insecure NiFi is used so the harness can read the provider directly:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster,
#   3. create a NiFiParameterProvider -> operator creates it in NiFi (VALID),
#   4. patch a property -> operator reconciles the drift into NiFi,
#   5. delete the CR -> operator deletes it in NiFi,
#   6. deletion-deadlock check: with a provider still present, delete the CLUSTER first and confirm
#      the provider's finalizer clears instead of blocking forever.
#
# The EnvironmentVariableParameterProvider ships with NiFi, so no external service is needed.
#
# Requires kind, kubectl, docker, go, helm. Reuses the NiFi image cached on the node. Run:
#   make integration-paramprovider-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-paramprovider}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-pp"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
pp_type="org.apache.nifi.parameter.EnvironmentVariableParameterProvider"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifiparameterprovider --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster pp --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster pp --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

pp_url() { echo "http://pp-nifi-0.pp-nifi-headless.${namespace}.svc:8080/nifi-api/parameter-providers/$1"; }

# pp_get <id> -> raw provider JSON as seen by NiFi (via the pod).
pp_get() {
  kubectl --context "${ctx}" -n "${namespace}" exec pp-nifi-0 -c nifi -- \
    curl -fsS "$(pp_url "$1")" 2>/dev/null
}

# pp_prop <id> <property-name> -> the value NiFi holds for that property.
pp_prop() {
  pp_get "$1" | grep -o "\"$2\":\"[^\"]*\"" | head -1 | sed "s/\"$2\":\"//;s/\"$//"
}

# pp_validation <id> -> the component's validation status as seen by NiFi (VALID/VALIDATING/INVALID).
pp_validation() {
  pp_get "$1" | grep -o '"validationStatus":"[A-Z]*"' | head -1 | cut -d'"' -f4
}

pp_diag() {
  local id="$1"
  echo "==== parameter provider ${id} as seen by NiFi ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" exec pp-nifi-0 -c nifi -- \
    curl -fsS "$(pp_url "${id}")" 2>&1 >&2 || true
  echo "==== NiFiParameterProvider status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider -o wide 2>&1 >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
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
metadata: {name: pp}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
  deletionPolicy: Delete
YAML

echo "Waiting for the NiFiCluster to become Ready..."
ready=0
for _ in $(seq 1 90); do
  if [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster pp -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ]; then
    ready=1; break
  fi
  sleep 10
done
[ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }
echo "  NiFiCluster is Ready."

echo "Phase 1: create a parameter provider -> created in NiFi, VALID..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterProvider
metadata: {name: env}
spec:
  clusterRef: {name: pp}
  type: ${pp_type}
  properties:
    parameter-group-name: environment
  deletionPolicy: Delete
YAML

id=""
for _ in $(seq 1 30); do
  id="$(kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider env -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider env -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ -n "${id}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ -n "${id}" ] && [ "${ready}" = "true" ] || { echo "parameter provider was not created/ready" >&2; pp_diag "${id}"; exit 1; }
echo "  created parameter provider ${id}."

group="$(pp_prop "${id}" "parameter-group-name")"
[ "${group}" = "environment" ] || { echo "expected group 'environment' in NiFi, got '${group}'" >&2; pp_diag "${id}"; exit 1; }
# NiFi validates the component asynchronously; poll it (VALIDATING -> VALID) to prove the operator
# configured a genuinely usable provider, not just a created-but-invalid one.
valid=0
for _ in $(seq 1 18); do
  [ "$(pp_validation "${id}")" = "VALID" ] && { valid=1; break; }
  sleep 5
done
[ "${valid}" = "1" ] || { echo "provider did not reach VALID in NiFi (status='$(pp_validation "${id}")')" >&2; pp_diag "${id}"; exit 1; }
echo "  NiFi has the provider with group='${group}', validationStatus=VALID."

echo "Phase 2: patch a property -> reconciled into NiFi..."
kubectl --context "${ctx}" -n "${namespace}" patch nifiparameterprovider env --type=merge \
  -p '{"spec":{"properties":{"parameter-group-name":"prod-env"}}}' >/dev/null
updated=0
for _ in $(seq 1 24); do
  [ "$(pp_prop "${id}" "parameter-group-name")" = "prod-env" ] && { updated=1; break; }
  sleep 5
done
[ "${updated}" = "1" ] || { echo "property change was not reconciled into NiFi" >&2; pp_diag "${id}"; exit 1; }
echo "  NiFi now has group='prod-env'."

echo "Phase 3: delete the CR -> removed from NiFi..."
kubectl --context "${ctx}" -n "${namespace}" delete nifiparameterprovider env --wait=true --timeout=60s >/dev/null
gone=0
for _ in $(seq 1 12); do
  code="$(kubectl --context "${ctx}" -n "${namespace}" exec pp-nifi-0 -c nifi -- \
    curl -s -o /dev/null -w '%{http_code}' "$(pp_url "${id}")" 2>/dev/null || true)"
  [ "${code}" = "404" ] && { gone=1; break; }
  sleep 5
done
[ "${gone}" = "1" ] || { echo "parameter provider was not deleted from NiFi" >&2; pp_diag "${id}"; exit 1; }
echo "  parameter provider ${id} removed from NiFi."

echo "Phase 4: deletion-deadlock check (delete the cluster while a provider exists)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterProvider
metadata: {name: env2}
spec:
  clusterRef: {name: pp}
  type: ${pp_type}
  properties: {parameter-group-name: environment}
  deletionPolicy: Delete
YAML
for _ in $(seq 1 20); do
  [ -n "$(kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider env2 -o jsonpath='{.status.nifiId}' 2>/dev/null || true)" ] && break
  sleep 5
done
# Delete the cluster first; the provider references it with deletionPolicy: Delete.
kubectl --context "${ctx}" -n "${namespace}" delete nificluster pp --wait=false >/dev/null
# The provider must not hang forever waiting for a cluster that is gone.
kubectl --context "${ctx}" -n "${namespace}" delete nifiparameterprovider env2 --wait=false >/dev/null
freed=0
for _ in $(seq 1 24); do
  kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider env2 >/dev/null 2>&1 || { freed=1; break; }
  sleep 5
done
[ "${freed}" = "1" ] || { echo "provider finalizer deadlocked after cluster deletion" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifiparameterprovider env2 -o yaml >&2; exit 1; }
echo "  provider finalizer cleared after the cluster was deleted (no deadlock)."

echo "PASS: NiFiParameterProvider create/update/delete works against real NiFi, and the deletion finalizer does not deadlock when the cluster is gone."
