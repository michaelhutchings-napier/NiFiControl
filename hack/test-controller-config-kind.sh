#!/usr/bin/env bash
# End-to-end test of spec.maxTimerDrivenThreadCount against a real Apache NiFi 2.10 in kind.
# This exercises the operator applying a flow-level controller setting through the NiFi API
# (nifi-api/controller/config) and enforcing it declaratively:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster with maxTimerDrivenThreadCount: 25 ->
#      the cluster becomes Ready only once the operator has applied it (readiness is gated
#      on the controller-config reconcile),
#   3. read nifi-api/controller/config and confirm the live value is 25,
#   4. change the spec to 40 -> the operator re-applies it; the live value converges to 40,
#   5. drift: change the value to 99 directly through the NiFi API, then poke the CR to force
#      a reconcile -> the operator resets it back to the spec value (40), proving declarative
#      enforcement.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-controllerconfig-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-ctrlcfg}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-ctrlcfg"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster cc --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster cc --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# live_count -> the maxTimerDrivenThreadCount NiFi currently reports ("" on failure).
live_count() {
  kubectl --context "${ctx}" -n "${namespace}" exec cc-nifi-0 -c nifi -- \
    curl -fsS "http://localhost:8080/nifi-api/controller/config" 2>/dev/null \
    | grep -o '"maxTimerDrivenThreadCount":[0-9]*' | head -1 | cut -d: -f2 || true
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster cc -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get nificluster cc -o yaml >&2; exit 1; }
}

# wait_live <want>: poll the NiFi API until the live thread count equals <want>.
wait_live() {
  local want="$1"
  for _ in $(seq 1 60); do
    [ "$(live_count)" = "${want}" ] && return 0
    sleep 5
  done
  echo "live maxTimerDrivenThreadCount never reached ${want} (last: $(live_count))" >&2
  exit 1
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi
docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1 && kind load docker-image apache/nifi:2.10.0 --name "${cluster}" >/dev/null 2>&1 || true

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

echo "Deploying the operator (helm) with the freshly built image..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: cluster with maxTimerDrivenThreadCount: 25 becomes Ready..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: cc}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  maxTimerDrivenThreadCount: 25
  deletionPolicy: Delete
YAML
wait_cluster_ready
echo "  cluster Ready."

echo "Phase 2: the live NiFi controller config reports 25..."
wait_live 25
echo "  live maxTimerDrivenThreadCount = 25."

echo "Phase 3: changing the spec to 40 re-applies through the API..."
kubectl --context "${ctx}" -n "${namespace}" patch nificluster cc --type=merge -p '{"spec":{"maxTimerDrivenThreadCount":40}}' >/dev/null
wait_live 40
echo "  live maxTimerDrivenThreadCount converged to 40."

echo "Phase 4: drift set through the NiFi API is reverted to the spec value..."
rev="$(kubectl --context "${ctx}" -n "${namespace}" exec cc-nifi-0 -c nifi -- \
  curl -fsS "http://localhost:8080/nifi-api/controller/config" 2>/dev/null \
  | grep -o '"version":[0-9]*' | head -1 | cut -d: -f2)"
kubectl --context "${ctx}" -n "${namespace}" exec cc-nifi-0 -c nifi -- \
  curl -fsS -X PUT -H 'Content-Type: application/json' \
  -d "{\"revision\":{\"version\":${rev}},\"component\":{\"maxTimerDrivenThreadCount\":99}}" \
  "http://localhost:8080/nifi-api/controller/config" >/dev/null
[ "$(live_count)" = "99" ] || { echo "failed to set drift value 99 (got $(live_count))" >&2; exit 1; }
echo "  drifted the live value to 99; poking the CR to force a reconcile..."
kubectl --context "${ctx}" -n "${namespace}" annotate nificluster cc nificontrol.test/poke="$(date +%s)" --overwrite >/dev/null
wait_live 40
echo "  operator reverted the drift back to 40."

echo "PASS: maxTimerDrivenThreadCount is applied, re-applied on change, and drift is enforced."
