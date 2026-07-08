#!/usr/bin/env bash
# End-to-end test that spec.scheduling.oneNifiNodePerNode enforces one NiFi node per
# Kubernetes node via a REQUIRED host anti-affinity — proving both that pods spread across
# nodes and that the constraint is hard (not best-effort):
#
#   1. create a kind cluster with two schedulable worker nodes (the control-plane is tainted
#      so exactly two nodes accept NiFi pods),
#   2. deploy a standalone ZooKeeper and the operator,
#   3. provision a clustered NiFiCluster with replicas: 3 and oneNifiNodePerNode: true,
#   4. assert every NiFi pod carries the required host anti-affinity, that exactly two pods
#      schedule onto two DISTINCT nodes, and that the third stays Pending/Unschedulable with
#      a pod-anti-affinity reason (there is no third node to place it on).
#
# Requires kind, kubectl, docker, go, helm.
# Run: make integration-onepernode-kind   Tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-onepernode}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-onepernode"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster onp --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster onp --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kindcfg="$(mktemp)"
  cat > "${kindcfg}" <<'KINDCFG'
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
nodes:
  - role: control-plane
  - role: worker
  - role: worker
KINDCFG
  kind create cluster --name "${cluster}" --config "${kindcfg}"
  rm -f "${kindcfg}"
fi
# Keep the control-plane off the schedulable set so exactly the two workers accept NiFi pods.
kubectl --context "${ctx}" taint node "${cluster}-control-plane" node-role.kubernetes.io/control-plane=:NoSchedule --overwrite >/dev/null 2>&1 || true
for img in apache/nifi:2.10.0 zookeeper:3.9; do
  docker image inspect "${img}" >/dev/null 2>&1 && kind load docker-image "${img}" --name "${cluster}" >/dev/null 2>&1 || true
done

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

echo "Deploying ZooKeeper..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: zookeeper, labels: {app: zookeeper}}
spec:
  replicas: 1
  selector: {matchLabels: {app: zookeeper}}
  template:
    metadata: {labels: {app: zookeeper}}
    spec:
      containers:
        - name: zookeeper
          image: zookeeper:3.9
          ports: [{containerPort: 2181, name: client}]
          readinessProbe: {tcpSocket: {port: client}, periodSeconds: 5, failureThreshold: 30}
---
apiVersion: v1
kind: Service
metadata: {name: zookeeper}
spec:
  selector: {app: zookeeper}
  ports: [{name: client, port: 2181, targetPort: client}]
YAML
kubectl --context "${ctx}" -n "${namespace}" rollout status deploy/zookeeper --timeout=120s

echo "Provisioning a replicas=3 NiFiCluster with oneNifiNodePerNode on two schedulable nodes..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: onp}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 3
  coordination:
    zookeeperConnectString: zookeeper.nifi-onepernode.svc:2181
    zookeeperRootNode: /onp
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 200m, memory: 1Gi}}
  scheduling:
    oneNifiNodePerNode: true
  deletionPolicy: Delete
YAML

echo "Waiting for the StatefulSet and for two pods to start running (one per node)..."
for _ in $(seq 1 60); do
  kubectl --context "${ctx}" -n "${namespace}" get statefulset onp-nifi >/dev/null 2>&1 && break
  sleep 5
done
running=0
# Generous budget: on a fresh multi-node cluster each node cold-pulls the ~1GB NiFi image.
for _ in $(seq 1 180); do
  running="$(kubectl --context "${ctx}" -n "${namespace}" get pods -l app.kubernetes.io/component=nifi-node --field-selector=status.phase=Running -o name 2>/dev/null | wc -l | tr -d ' ' || true)"
  [ "${running:-0}" -ge 2 ] && break
  sleep 10
done
[ "${running:-0}" -ge 2 ] || { echo "fewer than two NiFi pods reached Running: ${running}" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods -o wide >&2; exit 1; }

echo "Phase 1: every NiFi pod carries the required host anti-affinity..."
for p in onp-nifi-0 onp-nifi-1 onp-nifi-2; do
  tk="$(kubectl --context "${ctx}" -n "${namespace}" get pod "${p}" -o jsonpath='{.spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0].topologyKey}' 2>/dev/null || true)"
  [ "${tk}" = "kubernetes.io/hostname" ] || { echo "${p} missing host anti-affinity (topologyKey=${tk})" >&2; exit 1; }
done
echo "  all three pods carry a required kubernetes.io/hostname anti-affinity."

echo "Phase 2: exactly two pods are scheduled onto two distinct nodes; the third is Pending..."
scheduled_nodes=""
scheduled_count=0
pending_pod=""
pending_count=0
for p in onp-nifi-0 onp-nifi-1 onp-nifi-2; do
  nn="$(kubectl --context "${ctx}" -n "${namespace}" get pod "${p}" -o jsonpath='{.spec.nodeName}' 2>/dev/null || true)"
  if [ -n "${nn}" ]; then
    scheduled_nodes="${scheduled_nodes} ${nn}"
    scheduled_count=$((scheduled_count + 1))
    echo "  ${p} -> ${nn}"
  else
    pending_pod="${p}"
    pending_count=$((pending_count + 1))
    echo "  ${p} -> <unscheduled>"
  fi
done
[ "${scheduled_count}" = "2" ] || { echo "expected 2 scheduled pods, got ${scheduled_count}" >&2; exit 1; }
[ "${pending_count}" = "1" ] || { echo "expected 1 unscheduled pod, got ${pending_count}" >&2; exit 1; }
distinct="$(printf '%s' "${scheduled_nodes}" | tr ' ' '\n' | sed '/^$/d' | sort -u | grep -c . || true)"
[ "${distinct}" = "2" ] || { echo "the two scheduled pods are not on distinct nodes: ${scheduled_nodes}" >&2; exit 1; }
echo "  two pods run on two distinct nodes."

echo "Phase 3: the unschedulable pod is Pending because of the host anti-affinity..."
phase="$(kubectl --context "${ctx}" -n "${namespace}" get pod "${pending_pod}" -o jsonpath='{.status.phase}' 2>/dev/null || true)"
reason="$(kubectl --context "${ctx}" -n "${namespace}" get pod "${pending_pod}" -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].reason}' 2>/dev/null || true)"
message="$(kubectl --context "${ctx}" -n "${namespace}" get pod "${pending_pod}" -o jsonpath='{.status.conditions[?(@.type=="PodScheduled")].message}' 2>/dev/null || true)"
[ "${phase}" = "Pending" ] || { echo "${pending_pod} phase=${phase}, want Pending" >&2; exit 1; }
[ "${reason}" = "Unschedulable" ] || { echo "${pending_pod} PodScheduled reason=${reason}, want Unschedulable" >&2; exit 1; }
printf '%s' "${message}" | grep -qi "anti-affinity" || { echo "${pending_pod} not blocked by anti-affinity: ${message}" >&2; exit 1; }
echo "  ${pending_pod} is Unschedulable: ${message}"

echo "PASS: oneNifiNodePerNode places NiFi one-per-node and hard-blocks a pod when no distinct node is free."
