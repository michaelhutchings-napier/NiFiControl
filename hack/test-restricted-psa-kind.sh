#!/usr/bin/env bash
# End-to-end test that a managed NiFi cluster runs in a namespace enforcing the restricted
# Pod Security Admission profile, using spec.pod.securityContext (pod-level runAsNonRoot +
# seccompProfile) and spec.pod.containerSecurityContext (allowPrivilegeEscalation: false,
# capabilities drop ALL) on the operator's own containers:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. create a namespace that ENFORCES the restricted PSA profile, and prove enforcement is
#      real: a bare pod with no security context is rejected at admission,
#   3. provision an insecure single-node NiFiCluster with the restricted-PSA security context
#      set -> it is admitted and becomes Ready (proving the hardened context both satisfies
#      the policy and keeps NiFi working on the stock image),
#   4. assert the NiFi container and the initialize-data init container both carry
#      allowPrivilegeEscalation: false and capabilities drop ALL.
#
# Requires kind (k8s >= 1.25 for PSA), kubectl, docker, go, helm.
# Run: make integration-restrictedpsa-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-restricted}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-restricted"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster sec --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster sec --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster sec -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get events --sort-by=.lastTimestamp >&2; exit 1; }
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

echo "Phase 0: create a namespace that ENFORCES the restricted PSA profile..."
kubectl --context "${ctx}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: Namespace
metadata:
  name: ${namespace}
  labels:
    pod-security.kubernetes.io/enforce: restricted
    pod-security.kubernetes.io/enforce-version: latest
YAML
# Prove the namespace really enforces: a bare pod with no security context must be rejected.
if kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null 2>&1 <<'YAML'
apiVersion: v1
kind: Pod
metadata: {name: psa-probe}
spec:
  containers: [{name: c, image: busybox:1.36, command: ["sleep", "1"]}]
YAML
then
  echo "restricted PSA is not actually enforcing (a bare pod was admitted)" >&2
  kubectl --context "${ctx}" -n "${namespace}" delete pod psa-probe --ignore-not-found >/dev/null 2>&1 || true
  exit 1
fi
echo "  restricted PSA is enforcing (bare pod rejected)."

echo "Phase 1: hardened NiFiCluster is admitted and becomes Ready under restricted PSA..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: sec}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  pod:
    securityContext:
      runAsNonRoot: true
      seccompProfile: {type: RuntimeDefault}
    containerSecurityContext:
      allowPrivilegeEscalation: false
      capabilities: {drop: [ALL]}
YAML
wait_cluster_ready
echo "  cluster admitted and Ready in the restricted namespace."

echo "Phase 2: the NiFi and init containers carry the hardened security context..."
nifi_ape="$(kubectl --context "${ctx}" -n "${namespace}" get pod sec-nifi-0 -o jsonpath='{.spec.containers[?(@.name=="nifi")].securityContext.allowPrivilegeEscalation}')"
nifi_drop="$(kubectl --context "${ctx}" -n "${namespace}" get pod sec-nifi-0 -o jsonpath='{.spec.containers[?(@.name=="nifi")].securityContext.capabilities.drop[0]}')"
init_ape="$(kubectl --context "${ctx}" -n "${namespace}" get pod sec-nifi-0 -o jsonpath='{.spec.initContainers[?(@.name=="initialize-data")].securityContext.allowPrivilegeEscalation}')"
init_drop="$(kubectl --context "${ctx}" -n "${namespace}" get pod sec-nifi-0 -o jsonpath='{.spec.initContainers[?(@.name=="initialize-data")].securityContext.capabilities.drop[0]}')"
[ "${nifi_ape}" = "false" ] && [ "${nifi_drop}" = "ALL" ] || { echo "nifi container security context wrong: ape=${nifi_ape} drop=${nifi_drop}" >&2; exit 1; }
[ "${init_ape}" = "false" ] && [ "${init_drop}" = "ALL" ] || { echo "init container security context wrong: ape=${init_ape} drop=${init_drop}" >&2; exit 1; }
echo "  both containers carry allowPrivilegeEscalation=false and capabilities drop ALL."

echo "PASS: a managed NiFi cluster runs under the restricted Pod Security Admission profile."
