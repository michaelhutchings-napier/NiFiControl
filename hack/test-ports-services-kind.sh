#!/usr/bin/env bash
# End-to-end test of spec.ports (port customization), spec.externalServices (extra Services),
# and spec.proxyHosts (additive proxy allow-list) against a real Apache NiFi 2.10 in kind:
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster with a custom HTTP port, remote-input and
#      load-balance ports, two extra proxy hosts, and a LoadBalancer external Service -> the
#      cluster becomes Ready. Ready is itself proof the custom HTTP port works: the readiness
#      probe targets the named web port, so NiFi must have bound 8090 and the probe followed it,
#   3. assert the persisted nifi.properties carries every custom port and both proxy hosts, the
#      pod's container ports match, and the external LoadBalancer Service exists with the right
#      type and port mapping,
#   4. change the HTTP port -> the node rolls and comes Ready on the new port (the probe tracks
#      the reconfigured port, not a hardcoded 8080), and nifi.properties carries the new value,
#   5. remove externalServices -> the extra Service is pruned.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-portsvc-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-portsvc}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-portsvc"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster ps --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster ps --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# conf_grep <pattern> -> succeeds when the node's persisted nifi.properties matches.
conf_grep() {
  kubectl --context "${ctx}" -n "${namespace}" exec ps-nifi-0 -c nifi -- \
    grep -- "${1}" "/opt/nifi/nifi-current/conf/nifi.properties" >/dev/null 2>&1
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster ps -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get nificluster ps -o yaml >&2; exit 1; }
}

sts_generation() {
  kubectl --context "${ctx}" -n "${namespace}" get statefulset ps-nifi -o jsonpath='{.metadata.generation}' 2>/dev/null || true
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

echo "Phase 1: cluster with custom ports, proxy hosts, and a LoadBalancer Service becomes Ready..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: ps}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: true, size: 2Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  ports:
    http: 8090
    remoteInput: 10001
    loadBalance: 6343
  proxyHosts:
    - nifi.example.com
    - nifi.example.com:8443
  externalServices:
    - name: ps-nifi-lb
      type: LoadBalancer
      annotations: {purpose: e2e}
      ports:
        - name: https
          port: 8443
          targetPort: web
        - name: s2s
          port: 10001
          targetPort: s2s
      externalTrafficPolicy: Local
YAML
wait_cluster_ready
echo "  cluster Ready on the custom HTTP port (probe targets the named web port)."

echo "Phase 2: persisted nifi.properties, container ports, and the external Service are correct..."
conf_grep '^nifi\.web\.http\.port=8090$' || { echo "nifi.web.http.port not set to 8090" >&2; exit 1; }
conf_grep '^nifi\.remote\.input\.socket\.port=10001$' || { echo "nifi.remote.input.socket.port not set to 10001" >&2; exit 1; }
conf_grep '^nifi\.cluster\.load\.balance\.port=6343$' || { echo "nifi.cluster.load.balance.port not set to 6343" >&2; exit 1; }
conf_grep '^nifi\.web\.proxy\.host=.*nifi\.example\.com' || { echo "proxy host allow-list missing nifi.example.com" >&2; exit 1; }
conf_grep '^nifi\.web\.proxy\.host=.*nifi\.example\.com:8443' || { echo "proxy host allow-list missing nifi.example.com:8443" >&2; exit 1; }
echo "  nifi.properties carries the custom ports and both proxy hosts."

web_port="$(kubectl --context "${ctx}" -n "${namespace}" get pod ps-nifi-0 -o jsonpath='{.spec.containers[?(@.name=="nifi")].ports[?(@.name=="web")].containerPort}')"
s2s_port="$(kubectl --context "${ctx}" -n "${namespace}" get pod ps-nifi-0 -o jsonpath='{.spec.containers[?(@.name=="nifi")].ports[?(@.name=="s2s")].containerPort}')"
lb_port="$(kubectl --context "${ctx}" -n "${namespace}" get pod ps-nifi-0 -o jsonpath='{.spec.containers[?(@.name=="nifi")].ports[?(@.name=="load-balance")].containerPort}')"
[ "${web_port}" = "8090" ] || { echo "container web port = ${web_port}, want 8090" >&2; exit 1; }
[ "${s2s_port}" = "10001" ] || { echo "container s2s port = ${s2s_port}, want 10001" >&2; exit 1; }
[ "${lb_port}" = "6343" ] || { echo "container load-balance port = ${lb_port}, want 6343" >&2; exit 1; }
echo "  pod container ports match (web=8090, s2s=10001, load-balance=6343)."

svc_type="$(kubectl --context "${ctx}" -n "${namespace}" get svc ps-nifi-lb -o jsonpath='{.spec.type}')"
svc_target="$(kubectl --context "${ctx}" -n "${namespace}" get svc ps-nifi-lb -o jsonpath='{.spec.ports[?(@.name=="https")].targetPort}')"
svc_selector="$(kubectl --context "${ctx}" -n "${namespace}" get svc ps-nifi-lb -o jsonpath='{.spec.selector.app\.kubernetes\.io/component}')"
[ "${svc_type}" = "LoadBalancer" ] || { echo "external Service type = ${svc_type}, want LoadBalancer" >&2; exit 1; }
[ "${svc_target}" = "web" ] || { echo "external Service https targetPort = ${svc_target}, want web" >&2; exit 1; }
[ "${svc_selector}" = "nifi-node" ] || { echo "external Service selector component = ${svc_selector}, want nifi-node" >&2; exit 1; }
echo "  external LoadBalancer Service present with the right type, target, and selector."

echo "Phase 3: changing the HTTP port rolls the node and it comes Ready on the new port..."
gen_before="$(sts_generation)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ps --type=merge \
  -p '{"spec":{"ports":{"http":8091,"remoteInput":10001,"loadBalance":6343}}}' >/dev/null
for _ in $(seq 1 60); do [ "$(sts_generation)" != "${gen_before}" ] && break; sleep 5; done
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/ps-nifi --timeout=600s >/dev/null
wait_cluster_ready
conf_grep '^nifi\.web\.http\.port=8091$' || { echo "nifi.web.http.port not updated to 8091 after rollout" >&2; exit 1; }
echo "  node rolled and Ready on the reconfigured HTTP port 8091."

echo "Phase 4: removing externalServices prunes the extra Service..."
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ps --type=merge -p '{"spec":{"externalServices":null}}' >/dev/null
pruned=0
for _ in $(seq 1 30); do
  kubectl --context "${ctx}" -n "${namespace}" get svc ps-nifi-lb >/dev/null 2>&1 || { pruned=1; break; }
  sleep 2
done
[ "${pruned}" = "1" ] || { echo "external Service ps-nifi-lb not pruned after removal from spec" >&2; exit 1; }
echo "  external Service pruned."

echo "PASS: port customization, additive proxy hosts, and external Services work end to end."
