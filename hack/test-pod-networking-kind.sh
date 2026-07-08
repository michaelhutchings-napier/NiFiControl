#!/usr/bin/env bash
# End-to-end test that spec.pod hostAliases / dnsConfig / dnsPolicy actually reach the running
# NiFi pod's /etc/hosts and /etc/resolv.conf — the mechanism used to resolve private hostnames
# (an LDAP or OIDC endpoint, an on-prem service) that cluster DNS does not serve:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision a single-node NiFiCluster with hostAliases + a supplemental dnsConfig (search
#      domain, resolver option, extra nameserver) and dnsPolicy ClusterFirst, then wait for
#      Ready (proving the supplemental DNS config does not break NiFi's own resolution/boot),
#   3. assert the pod carries hostAliases/dnsConfig/dnsPolicy AND that they took effect inside
#      the container: the host alias appears in /etc/hosts (and resolves), and the search
#      domain + ndots option appear in /etc/resolv.conf.
#
# Requires kind, kubectl, docker, go, helm.
# Run: make integration-podnet-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-podnet}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-podnet"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster net --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster net --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster net -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
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

echo "Provisioning a single-node NiFiCluster with hostAliases + dnsConfig..."
kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: net}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  pod:
    hostAliases:
      - ip: 10.9.8.7
        hostnames: [ldap.internal.example.com, oidc.internal.example.com]
    dnsPolicy: ClusterFirst
    dnsConfig:
      nameservers: [10.9.8.53]
      searches: [internal.example.com]
      options:
        - {name: ndots, value: "2"}
YAML
wait_cluster_ready
echo "  cluster is Ready (supplemental DNS config did not break NiFi boot)."

echo "Phase 1: the pod spec carries hostAliases, dnsPolicy, and dnsConfig..."
jp() { kubectl --context "${ctx}" -n "${namespace}" get pod net-nifi-0 -o jsonpath="$1"; }
ha_ip="$(jp '{.spec.hostAliases[0].ip}')"
ha_host="$(jp '{.spec.hostAliases[0].hostnames[0]}')"
policy="$(jp '{.spec.dnsPolicy}')"
search="$(jp '{.spec.dnsConfig.searches[0]}')"
[ "${ha_ip}" = "10.9.8.7" ] && [ "${ha_host}" = "ldap.internal.example.com" ] || { echo "hostAliases not on pod spec: ip=${ha_ip} host=${ha_host}" >&2; exit 1; }
[ "${policy}" = "ClusterFirst" ] || { echo "dnsPolicy not on pod spec: ${policy}" >&2; exit 1; }
[ "${search}" = "internal.example.com" ] || { echo "dnsConfig search not on pod spec: ${search}" >&2; exit 1; }
echo "  pod spec has hostAliases=${ha_ip}/${ha_host}, dnsPolicy=${policy}, dnsConfig search=${search}."

echo "Phase 2: the settings took effect inside the container..."
hosts="$(kubectl --context "${ctx}" -n "${namespace}" exec net-nifi-0 -c nifi -- cat /etc/hosts 2>/dev/null)"
resolv="$(kubectl --context "${ctx}" -n "${namespace}" exec net-nifi-0 -c nifi -- cat /etc/resolv.conf 2>/dev/null)"
echo "${hosts}" | grep -qE '10\.9\.8\.7[[:space:]].*ldap\.internal\.example\.com' || { echo "host alias not in /etc/hosts" >&2; echo "${hosts}" >&2; exit 1; }
echo "  /etc/hosts contains the LDAP host alias:"
echo "${hosts}" | grep "ldap.internal.example.com" | sed 's/^/    /'
# Bonus: the alias resolves via the resolver (getent reads /etc/hosts).
resolved="$(kubectl --context "${ctx}" -n "${namespace}" exec net-nifi-0 -c nifi -- getent hosts ldap.internal.example.com 2>/dev/null || true)"
[ -n "${resolved}" ] && echo "    getent -> ${resolved}"
echo "${resolv}" | grep -q "internal.example.com" || { echo "custom search domain not in /etc/resolv.conf" >&2; echo "${resolv}" >&2; exit 1; }
echo "${resolv}" | grep -q "ndots:2" || { echo "custom ndots option not in /etc/resolv.conf" >&2; echo "${resolv}" >&2; exit 1; }
echo "  /etc/resolv.conf has the custom search domain and ndots option:"
echo "${resolv}" | grep -E "search|ndots" | sed 's/^/    /'

echo "PASS: spec.pod hostAliases/dnsConfig/dnsPolicy reach the NiFi pod's /etc/hosts and /etc/resolv.conf."
