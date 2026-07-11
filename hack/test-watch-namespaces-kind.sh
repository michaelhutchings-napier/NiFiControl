#!/usr/bin/env bash
# End-to-end test of the operator's namespace-scoping (watchNamespaces / --watch-namespaces) in
# kind. When the operator is restricted to a namespace, it must reconcile NiFiClusters only there
# and ignore identical resources elsewhere. This is a security/isolation property, so it is checked
# against a real cluster. It does NOT need a running NiFi — it only asserts whether the operator
# creates the managed StatefulSet:
#
#   1. deploy the operator with watchNamespaces={watched},
#   2. create an identical NiFiCluster in the watched namespace and in an unwatched one,
#   3. assert the StatefulSet is created (and status is populated) in the watched namespace,
#   4. assert NOTHING is created and status stays empty in the unwatched namespace.
#
# Requires kind, kubectl, docker, go, helm. No NiFi image needed. Run:
#   make integration-watchns-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-watchns}"
ctx="kind-${cluster}"
opns="nificontrol-system"
watched="team-a"
unwatched="team-b"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  for ns in "${watched}" "${unwatched}"; do
    kubectl --context "${ctx}" -n "${ns}" patch nificluster demo --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${ns}" delete nificluster demo --ignore-not-found --wait=false >/dev/null 2>&1 || true
  done
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

cluster_manifest() {
  cat <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: demo}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 100m, memory: 512Mi}}
  deletionPolicy: Delete
YAML
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
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

for ns in "${opns}" "${watched}" "${unwatched}"; do
  kubectl --context "${ctx}" create namespace "${ns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
done

echo "Deploying the operator scoped to namespace '${watched}'..."
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false \
  --set "watchNamespaces={${watched}}" >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s
# Confirm the scoping flag actually reached the container.
kubectl --context "${ctx}" -n "${opns}" get deploy/nificontrol -o jsonpath='{.spec.template.spec.containers[0].args}' | grep -q "watch-namespaces=${watched}" \
  || { echo "operator was not started with --watch-namespaces=${watched}" >&2; exit 1; }

echo "Creating an identical NiFiCluster in '${watched}' and '${unwatched}'..."
cluster_manifest | kubectl --context "${ctx}" -n "${watched}" apply -f - >/dev/null
cluster_manifest | kubectl --context "${ctx}" -n "${unwatched}" apply -f - >/dev/null

echo "Asserting the operator reconciled ONLY the watched namespace..."
created=0
for _ in $(seq 1 30); do
  kubectl --context "${ctx}" -n "${watched}" get statefulset demo-nifi >/dev/null 2>&1 && { created=1; break; }
  sleep 5
done
[ "${created}" = "1" ] || { echo "operator did not create the StatefulSet in the watched namespace '${watched}'" >&2; kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=30 >&2; exit 1; }
echo "  watched '${watched}': StatefulSet demo-nifi created, status.ready=$(kubectl --context "${ctx}" -n "${watched}" get nificluster demo -o jsonpath='{.status.ready}' 2>/dev/null)."

# Give any (erroneous) cross-namespace reconcile ample time to act, then assert nothing happened.
sleep 20
if kubectl --context "${ctx}" -n "${unwatched}" get statefulset demo-nifi >/dev/null 2>&1; then
  echo "ISOLATION FAILURE: operator created a StatefulSet in the UNWATCHED namespace '${unwatched}'" >&2; exit 1
fi
status_unwatched="$(kubectl --context "${ctx}" -n "${unwatched}" get nificluster demo -o jsonpath='{.status}' 2>/dev/null || true)"
[ -z "${status_unwatched}" ] || { echo "ISOLATION FAILURE: operator wrote status on the unwatched NiFiCluster: ${status_unwatched}" >&2; exit 1; }
echo "  unwatched '${unwatched}': no StatefulSet, empty status — the operator ignored it."

echo "PASS: the operator reconciles only its watchNamespaces and ignores NiFiClusters elsewhere."
