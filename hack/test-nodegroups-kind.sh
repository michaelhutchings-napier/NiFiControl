#!/usr/bin/env bash
# Verify the node-groups networking model against a real Apache NiFi 2.10 in kind: two
# separate StatefulSets (a "primary" pool and a "workers" pool) join ONE NiFi cluster through
# a shared headless Service, ZooKeeper, and sensitive-properties key — exactly what the
# operator produces for a NiFiCluster plus a NiFiNodeGroup.
#
# Requires kind, kubectl, docker, curl. Run with: make integration-nodegroups-kind
# The kind cluster and deployments are kept between runs; tear down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-nodegroups}"
ctx="kind-${cluster}"
namespace="nifi-nodegroups"
local_port="${NIFI_INTEGRATION_PORT:-18082}"
pf_pid=""

cleanup() {
  [ -n "${pf_pid}" ] && kill "${pf_pid}" >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

if ! docker exec "${cluster}-control-plane" crictl images 2>/dev/null | grep -q 'apache/nifi'; then
  if docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1; then
    archive="$(mktemp --suffix=.tar)"
    docker save apache/nifi:2.10.0 -o "${archive}"
    kind load image-archive "${archive}" --name "${cluster}" || true
    rm -f "${archive}"
  fi
fi

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f -

# A NiFi node StatefulSet for a given pool. $1 = StatefulSet name, $2 = node-pool label value.
render_pool() {
  local name="$1" pool="$2"
  cat <<YAML
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: ${name}
spec:
  serviceName: nifi-headless
  podManagementPolicy: Parallel
  replicas: 1
  selector: {matchLabels: {app: nifi, node-pool: ${pool}}}
  template:
    metadata: {labels: {app: nifi, node-pool: ${pool}}}
    spec:
      initContainers:
        - name: wait-for-zookeeper
          image: busybox:1.36
          command: ["/bin/sh", "-ec"]
          args: ["until nc -z zookeeper.${namespace}.svc 2181; do echo waiting-zk; sleep 3; done"]
      containers:
        - name: nifi
          image: apache/nifi:2.10.0
          imagePullPolicy: IfNotPresent
          env:
            - {name: POD_NAME, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
          command: ["/bin/bash", "-ec"]
          args:
            - |
              . /opt/nifi/scripts/common.sh
              prop_replace 'java.arg.2' '-Xms512m' "\${nifi_bootstrap_file}"
              prop_replace 'java.arg.3' '-Xmx1024m' "\${nifi_bootstrap_file}"
              # Advertise the routable pod DNS name as the web host (as the operator does for
              # clustered nodes), so /controller/cluster reports per-node addresses and the
              # cluster API can proxy between nodes. Binding 0.0.0.0 makes nodes report 0.0.0.0.
              prop_replace nifi.web.http.host "\${POD_NAME}.nifi-headless.${namespace}.svc"
              prop_replace nifi.web.http.port 8080
              # Allow the port-forward Host header (the test reaches NiFi via 127.0.0.1:<port>),
              # since binding the FQDN makes NiFi enforce the Host against its web/proxy hosts.
              prop_replace nifi.web.proxy.host "127.0.0.1:${local_port},localhost:${local_port},127.0.0.1,localhost"
              prop_replace nifi.web.https.host ""
              prop_replace nifi.web.https.port ""
              prop_replace nifi.security.keystore ""
              prop_replace nifi.security.truststore ""
              prop_replace nifi.remote.input.secure false
              prop_replace nifi.sensitive.props.key "nificontrol-nodegroups-dev-key"
              prop_replace nifi.cluster.is.node true
              prop_replace nifi.cluster.node.address "\${POD_NAME}.nifi-headless.${namespace}.svc"
              prop_replace nifi.cluster.node.protocol.port 11443
              prop_replace nifi.cluster.protocol.is.secure false
              prop_replace nifi.zookeeper.connect.string "zookeeper.${namespace}.svc:2181"
              prop_replace nifi.zookeeper.root.node /nifi
              prop_replace nifi.cluster.flow.election.max.wait.time "1 mins"
              prop_replace nifi.cluster.flow.election.max.candidates 1
              sed -i "s|<property name=\"Connect String\">[^<]*</property>|<property name=\"Connect String\">zookeeper.${namespace}.svc:2181</property>|" /opt/nifi/nifi-current/conf/state-management.xml
              exec /opt/nifi/nifi-current/bin/nifi.sh run
          ports:
            - {containerPort: 8080, name: web}
            - {containerPort: 11443, name: cluster}
          readinessProbe:
            httpGet: {path: /nifi-api/flow/about, port: web}
            periodSeconds: 10
            failureThreshold: 90
YAML
}

{
cat <<YAML
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
---
apiVersion: v1
kind: Service
metadata: {name: nifi-headless}
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector: {app: nifi}
  ports:
    - {name: web, port: 8080, targetPort: web}
    - {name: cluster, port: 11443, targetPort: cluster}
---
apiVersion: v1
kind: Service
metadata: {name: nifi}
spec:
  selector: {app: nifi}
  ports: [{name: web, port: 8080, targetPort: web}]
YAML
render_pool nifi primary
echo "---"
render_pool nifi-workers workers
} | kubectl --context "${ctx}" -n "${namespace}" apply -f -

echo "Waiting for both pools to become ready (the first run also pulls the ~2GB image)..."
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/nifi --timeout=900s
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/nifi-workers --timeout=900s

kubectl --context "${ctx}" -n "${namespace}" port-forward svc/nifi "${local_port}:8080" >/dev/null 2>&1 &
pf_pid=$!

base_uri="http://127.0.0.1:${local_port}"
echo "Waiting for both pools to join one cluster..."
for _ in $(seq 1 90); do
  count="$(curl -fsS "${base_uri}/nifi-api/controller/cluster" 2>/dev/null | grep -o '"status":"CONNECTED"' | wc -l || true)"
  if [ "${count}" -ge 2 ]; then
    echo "Cluster formed with ${count} connected nodes across both pools."
    NIFI_API_URI="${base_uri}" go test -count=1 -tags=integration ./internal/controller -run NodeGroupFormsOneCluster -v
    exit 0
  fi
  sleep 5
done

echo "Pools did not join one cluster with 2 connected nodes" >&2
kubectl --context "${ctx}" -n "${namespace}" get pods -o wide || true
exit 1
