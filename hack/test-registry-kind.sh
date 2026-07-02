#!/usr/bin/env bash
# End-to-end test of GitHub/GitLab NiFiRegistryClient support against a real Apache NiFi 2.10 in
# kind. The main risk this catches is a mismatch between the property display names the operator
# sends and the names NiFi's GitHubFlowRegistryClient/GitLabFlowRegistryClient actually expose:
# NiFi only echoes back a property under a key it recognises, so reading the created client back
# confirms the names are correct (independent of GitHub/GitLab connectivity or credentials).
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster,
#   3. create a GitHub registry client -> confirm NiFi stored Repository Owner/Name and the
#      GitHub API URL under the expected keys,
#   4. create a GitLab registry client (token from a Secret) -> confirm Repository Namespace and
#      Authentication Type=ACCESS_TOKEN are stored under the expected keys.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-registry-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-authz}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-registry"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifiregistryclient --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster rt --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster rt --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# rc_json <id> -> the registry client entity JSON as seen by NiFi.
rc_json() {
  kubectl --context "${ctx}" -n "${namespace}" exec rt-nifi-0 -c nifi -- \
    curl -fsS "http://rt-nifi-0.rt-nifi-headless.${namespace}.svc:8080/nifi-api/controller/registry-clients/${1}" 2>/dev/null
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
# explicitly — otherwise a warm cluster keeps an older NiFiRegistryClient CRD that rejects the
# new spec.github/spec.gitlab fields.
echo "Applying the current CRDs..."
# --force-conflicts because the CRDs were first installed by helm (which owns some fields).
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

echo "Creating the GitLab token Secret..."
kubectl --context "${ctx}" -n "${namespace}" create secret generic gitlab-token --from-literal=token=glpat-example --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: GitHub registry client -> NiFi stores the expected property names..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRegistryClient
metadata: {name: github-flows}
spec:
  clusterRef: {name: rt}
  type: GitHub
  github: {repositoryOwner: apache, repositoryName: nifi, defaultBranch: main}
  deletionPolicy: Delete
YAML
gid=""
for _ in $(seq 1 30); do
  gid="$(kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient github-flows -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient github-flows -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ -n "${gid}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ -n "${gid}" ] && [ "${ready}" = "true" ] || { echo "GitHub registry client not created/ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient github-flows -o yaml >&2; exit 1; }
gh="$(rc_json "${gid}")"
echo "${gh}" | grep -q '"Repository Owner":"apache"' || { echo "NiFi did not store 'Repository Owner'=apache" >&2; echo "${gh}" >&2; exit 1; }
echo "${gh}" | grep -q '"Repository Name":"nifi"' || { echo "NiFi did not store 'Repository Name'=nifi" >&2; echo "${gh}" >&2; exit 1; }
echo "${gh}" | grep -q '"GitHub API URL":"https://api.github.com/"' || { echo "NiFi did not store 'GitHub API URL'" >&2; echo "${gh}" >&2; exit 1; }
vstatus="$(echo "${gh}" | grep -o '"validationStatus":"[A-Z]*"' | head -1 | cut -d'"' -f4)"
echo "  GitHub client ${gid} stored the expected property names (validationStatus=${vstatus})."

echo "Phase 2: GitLab registry client -> NiFi stores the expected property names..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRegistryClient
metadata: {name: gitlab-flows}
spec:
  clusterRef: {name: rt}
  type: GitLab
  gitlab:
    repositoryNamespace: acme-group
    repositoryName: dataflows
    accessTokenSecretRef: {name: gitlab-token, key: token}
  deletionPolicy: Delete
YAML
lid=""
for _ in $(seq 1 30); do
  lid="$(kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient gitlab-flows -o jsonpath='{.status.nifiId}' 2>/dev/null || true)"
  ready="$(kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient gitlab-flows -o jsonpath='{.status.ready}' 2>/dev/null || true)"
  [ -n "${lid}" ] && [ "${ready}" = "true" ] && break
  sleep 5
done
[ -n "${lid}" ] && [ "${ready}" = "true" ] || { echo "GitLab registry client not created/ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get nifiregistryclient gitlab-flows -o yaml >&2; exit 1; }
gl="$(rc_json "${lid}")"
echo "${gl}" | grep -q '"Repository Namespace":"acme-group"' || { echo "NiFi did not store 'Repository Namespace'=acme-group" >&2; echo "${gl}" >&2; exit 1; }
echo "${gl}" | grep -q '"Authentication Type":"ACCESS_TOKEN"' || { echo "NiFi did not store 'Authentication Type'=ACCESS_TOKEN" >&2; echo "${gl}" >&2; exit 1; }
echo "  GitLab client ${lid} stored the expected property names."

echo "PASS: NiFi accepts the GitHub/GitLab flow registry client property names the operator sends."
