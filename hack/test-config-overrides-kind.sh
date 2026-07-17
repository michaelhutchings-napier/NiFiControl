#!/usr/bin/env bash
# End-to-end test of spec.configOverrides against a real Apache NiFi 2.10 in kind. The risks this
# catches are in the start-script merge logic operating on *persisted* configuration (the conf
# directory lives on the PVC and carries the previous boot's mutations):
#
#   1. build + load the operator image and (re)deploy it via helm,
#   2. provision an insecure single-node NiFiCluster (persistent storage ON) with nifi.properties
#      and bootstrap.conf overrides -> confirm both files carry them and the cluster is Ready,
#   3. admission: an override of an operator-managed key (nifi.web.http.port) must be rejected
#      by the CRD's CEL validation,
#   4. change an override value -> the checksum annotation rolls the node -> new value applied.
#      This rollout is also the restart-a-persistent-standalone-node case that requires the
#      operator-provided sensitive properties key (NiFi otherwise self-generates one that the
#      next boot blanks, stranding the encrypted flow),
#   5. remove configOverrides entirely -> shipped defaults restored from the image copy, the
#      custom key is gone, and the overrides Secret is deleted,
#   6. add spec.pod (sidecar, annotation, extra volume/mount) and a logbackXml override ->
#      pod carries them and conf/logback.xml is replaced,
#   7. reserved sidecar names are rejected at admission,
#   8. remove spec.pod and the logback override -> sidecar gone, image logback.xml restored.
#
# Requires kind, kubectl, docker, go, helm. Run: make integration-configoverrides-kind
# Reuse a warm cluster with KIND_CLUSTER=<name>; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-overrides}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-overrides"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster ov --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

# conf_grep <file> <pattern> -> succeeds when the node's persisted conf file matches.
conf_grep() {
  kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- \
    grep -- "${2}" "/opt/nifi/nifi-current/conf/${1}" >/dev/null 2>&1
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 90); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster ov -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "NiFiCluster did not become Ready" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; kubectl --context "${ctx}" -n "${namespace}" get nificluster ov -o yaml >&2; exit 1; }
}

# sts_checksum -> the overrides checksum currently on the StatefulSet pod template ("" if unset).
sts_checksum() {
  kubectl --context "${ctx}" -n "${namespace}" get statefulset ov-nifi \
    -o "go-template={{index .spec.template.metadata.annotations \"nifi.controlnifi.io/config-overrides-checksum\"}}" 2>/dev/null || true
}

# wait_rollout <previous-checksum>: the operator stamps a new overrides checksum on the pod
# template before the StatefulSet rolls; waiting for the stamp first avoids racing rollout
# status against the pre-patch generation.
wait_rollout() {
  local previous="$1" changed=0
  for _ in $(seq 1 60); do
    [ "$(sts_checksum)" != "${previous}" ] && { changed=1; break; }
    sleep 5
  done
  [ "${changed}" = "1" ] || { echo "StatefulSet pod template never picked up the new overrides checksum" >&2; exit 1; }
  kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/ov-nifi --timeout=600s >/dev/null
  wait_cluster_ready
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

echo "Phase 1: cluster with nifi.properties + bootstrap.conf overrides becomes Ready and applies them..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: ov}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: true, size: 2Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  deletionPolicy: Delete
  configOverrides:
    nifiProperties:
      nifi.queue.swap.threshold: "15000"
      custom.nificontrol.test: "hello=world|x"
    bootstrapProperties:
      java.arg.nificontroltest: "-Dnificontrol.test=1"
YAML
wait_cluster_ready
conf_grep nifi.properties '^nifi\.queue\.swap\.threshold=15000$' || { echo "override nifi.queue.swap.threshold=15000 not applied" >&2; exit 1; }
conf_grep nifi.properties '^custom\.nificontrol\.test=hello=world|x$' || { echo "custom property override not applied verbatim" >&2; exit 1; }
conf_grep bootstrap.conf '^java\.arg\.nificontroltest=-Dnificontrol\.test=1$' || { echo "bootstrap.conf override not applied" >&2; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" get secret ov-nifi-config-overrides >/dev/null || { echo "overrides Secret missing" >&2; exit 1; }
echo "  overrides applied; cluster Ready."

echo "Phase 2: overriding an operator-managed key is rejected at admission..."
if kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge \
  -p '{"spec":{"configOverrides":{"nifiProperties":{"nifi.web.http.port":"9999"}}}}' >/dev/null 2>&1; then
  echo "expected the CEL denylist to reject nifi.web.http.port" >&2; exit 1
fi
echo "  denylisted key rejected."

echo "Phase 3: changing an override value rolls the node and applies the new value..."
checksum_before="$(sts_checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge \
  -p '{"spec":{"configOverrides":{"nifiProperties":{"nifi.queue.swap.threshold":"30000"}}}}' >/dev/null
wait_rollout "${checksum_before}"
conf_grep nifi.properties '^nifi\.queue\.swap\.threshold=30000$' || { echo "updated override not applied after rollout" >&2; exit 1; }
conf_grep nifi.properties '^custom\.nificontrol\.test=hello=world|x$' || { echo "unchanged override lost during rollout" >&2; exit 1; }
echo "  updated override applied."

echo "Phase 4: removing configOverrides restores the image defaults on the persisted conf..."
checksum_before="$(sts_checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge \
  -p '{"spec":{"configOverrides":null}}' >/dev/null
wait_rollout "${checksum_before}"
default_threshold="$(kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- \
  grep '^nifi\.queue\.swap\.threshold=' /opt/nifi/nifi-current/conf/nifi.properties.image-default)"
conf_grep nifi.properties "^${default_threshold}$" || { echo "nifi.queue.swap.threshold not restored to the image default (${default_threshold})" >&2; exit 1; }
if conf_grep nifi.properties '^custom\.nificontrol\.test='; then
  echo "custom property survived override removal" >&2; exit 1
fi
if conf_grep bootstrap.conf '^java\.arg\.nificontroltest='; then
  echo "bootstrap.conf override survived removal" >&2; exit 1
fi
if kubectl --context "${ctx}" -n "${namespace}" get secret ov-nifi-config-overrides >/dev/null 2>&1; then
  echo "overrides Secret not cleaned up" >&2; exit 1
fi
echo "  defaults restored; Secret cleaned up."

echo "Phase 5: spec.pod customization + logbackXml override..."
checksum_before="$(sts_checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge -p '{
  "spec": {
    "configOverrides": {"logbackXml": "<configuration><appender name=\"CONSOLE\" class=\"ch.qos.logback.core.ConsoleAppender\"><encoder><pattern>%date %level [%thread] %logger{40} %msg%n</pattern></encoder></appender><root level=\"WARN\"><appender-ref ref=\"CONSOLE\"/></root></configuration>"},
    "pod": {
      "annotations": {"nificontrol.test/marker": "e2e"},
      "extraVolumes": [{"name": "scratch", "emptyDir": {}}],
      "extraVolumeMounts": [{"name": "scratch", "mountPath": "/scratch"}],
      "extraContainers": [{"name": "sidecar", "image": "apache/nifi:2.10.0", "command": ["sleep", "infinity"]}]
    }
  }
}' >/dev/null
wait_rollout "${checksum_before}"
conf_grep logback.xml 'root level="WARN"' || { echo "logback.xml override not applied" >&2; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- test -f /opt/nifi/nifi-current/conf/.nificontrol-logback-overridden || { echo "logback override marker missing" >&2; exit 1; }
containers="$(kubectl --context "${ctx}" -n "${namespace}" get pod ov-nifi-0 -o jsonpath='{.spec.containers[*].name}')"
case "${containers}" in *sidecar*) ;; *) echo "sidecar container missing (${containers})" >&2; exit 1;; esac
annotation="$(kubectl --context "${ctx}" -n "${namespace}" get pod ov-nifi-0 -o jsonpath='{.metadata.annotations.nificontrol\.test/marker}')"
[ "${annotation}" = "e2e" ] || { echo "custom pod annotation missing" >&2; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- test -d /scratch || { echo "extra volume mount missing" >&2; exit 1; }
echo "  sidecar, annotation, extra mount, and logback override live."

echo "Phase 6: reserved container name is rejected at admission..."
if kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge \
  -p '{"spec":{"pod":{"extraContainers":[{"name":"nifi","image":"busybox:1.36"}]}}}' >/dev/null 2>&1; then
  echo "expected the CEL rule to reject a sidecar named nifi" >&2; exit 1
fi
echo "  reserved name rejected."

echo "Phase 7: removing spec.pod and the logback override restores the defaults..."
checksum_before="$(sts_checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster ov --type=merge \
  -p '{"spec":{"configOverrides":null,"pod":null}}' >/dev/null
wait_rollout "${checksum_before}"
kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- \
  cmp -s /opt/nifi/nifi-current/conf/logback.xml /opt/nifi/nifi-current/conf/logback.xml.image-default || { echo "logback.xml not restored to the image default" >&2; exit 1; }
if kubectl --context "${ctx}" -n "${namespace}" exec ov-nifi-0 -c nifi -- test -f /opt/nifi/nifi-current/conf/.nificontrol-logback-overridden 2>/dev/null; then
  echo "logback override marker survived removal" >&2; exit 1
fi
containers="$(kubectl --context "${ctx}" -n "${namespace}" get pod ov-nifi-0 -o jsonpath='{.spec.containers[*].name}')"
[ "${containers}" = "nifi" ] || { echo "sidecar survived spec.pod removal (${containers})" >&2; exit 1; }
echo "  sidecar removed; image logback.xml restored."

echo "PASS: configOverrides + pod customization applied, guarded, updated, and cleanly removed."
