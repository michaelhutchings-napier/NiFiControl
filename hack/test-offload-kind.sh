#!/usr/bin/env bash
# Run the NiFi node-offload integration test against a real, clustered Apache NiFi 2.10
# running inside a local kind cluster. A two-node NiFi StatefulSet plus a standalone
# ZooKeeper are deployed in controlled HTTP development mode; the test disconnects,
# offloads, and removes the highest-addressed node exactly as the operator does on a
# graceful scale-down.
#
# Requires kind, kubectl, docker, and curl. Run with: make integration-offload-kind
#
# As with the BlueGreen harness, the kind cluster and the NiFi/ZooKeeper deployments are
# KEPT between runs so the ~2GB apache/nifi:2.10.0 image is pulled only once. Tear it all
# down with KIND_DELETE=1:
#   KIND_DELETE=1 make integration-offload-kind
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-offload}"
ctx="kind-${cluster}"
namespace="nifi-cluster"
local_port="${NIFI_INTEGRATION_PORT:-18081}"
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

# Best-effort side-load of the host image to avoid an in-node pull (see test-bluegreen-kind.sh).
if ! docker exec "${cluster}-control-plane" crictl images 2>/dev/null | grep -q 'apache/nifi'; then
  if docker image inspect apache/nifi:2.10.0 >/dev/null 2>&1; then
    echo "Loading apache/nifi:2.10.0 into the kind node from the local image..."
    archive="$(mktemp --suffix=.tar)"
    docker save apache/nifi:2.10.0 -o "${archive}"
    kind load image-archive "${archive}" --name "${cluster}" || true
    rm -f "${archive}"
  fi
fi

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f -

kubectl --context "${ctx}" -n "${namespace}" apply -f - <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: zookeeper
  labels: {app: zookeeper}
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
          readinessProbe:
            tcpSocket: {port: client}
            periodSeconds: 5
            failureThreshold: 30
---
apiVersion: v1
kind: Service
metadata:
  name: zookeeper
spec:
  selector: {app: zookeeper}
  ports: [{name: client, port: 2181, targetPort: client}]
---
apiVersion: v1
kind: Service
metadata:
  name: nifi-headless
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
metadata:
  name: nifi
spec:
  selector: {app: nifi}
  ports: [{name: web, port: 8080, targetPort: web}]
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: nifi
spec:
  serviceName: nifi-headless
  podManagementPolicy: Parallel
  replicas: 2
  selector: {matchLabels: {app: nifi}}
  template:
    metadata: {labels: {app: nifi}}
    spec:
      initContainers:
        # Wait for ZooKeeper so the cluster election does not crash-loop on first start.
        - name: wait-for-zookeeper
          image: busybox:1.36
          command: ["/bin/sh", "-ec"]
          args:
            - |
              until nc -z "zookeeper.${POD_NAMESPACE}.svc" 2181; do
                echo "waiting for zookeeper..."; sleep 3
              done
          env:
            - {name: POD_NAMESPACE, valueFrom: {fieldRef: {fieldPath: metadata.namespace}}}
      containers:
        - name: nifi
          image: apache/nifi:2.10.0
          imagePullPolicy: IfNotPresent
          env:
            - {name: POD_NAME, valueFrom: {fieldRef: {fieldPath: metadata.name}}}
            - {name: POD_NAMESPACE, valueFrom: {fieldRef: {fieldPath: metadata.namespace}}}
          command: ["/bin/bash", "-ec"]
          args:
            - |
              . /opt/nifi/scripts/common.sh
              # Constrain heap: two NiFi JVMs plus ZooKeeper share one kind node.
              prop_replace 'java.arg.2' '-Xms512m' "${nifi_bootstrap_file}"
              prop_replace 'java.arg.3' '-Xmx1024m' "${nifi_bootstrap_file}"
              prop_replace nifi.web.http.host 0.0.0.0
              prop_replace nifi.web.http.port 8080
              prop_replace nifi.web.https.host ""
              prop_replace nifi.web.https.port ""
              prop_replace nifi.security.keystore ""
              prop_replace nifi.security.truststore ""
              prop_replace nifi.remote.input.secure false
              # NiFi 2.x requires a shared, explicit sensitive properties key on cluster nodes.
              prop_replace nifi.sensitive.props.key "nificontrol-offload-dev-key"
              prop_replace nifi.cluster.is.node true
              prop_replace nifi.cluster.node.address "${POD_NAME}.nifi-headless.${POD_NAMESPACE}.svc"
              prop_replace nifi.cluster.node.protocol.port 11443
              prop_replace nifi.cluster.protocol.is.secure false
              prop_replace nifi.zookeeper.connect.string "zookeeper.${POD_NAMESPACE}.svc:2181"
              prop_replace nifi.zookeeper.root.node /nifi
              # NiFi's clustered state provider reads its own Connect String from state-management.xml.
              sed -i "s|<property name=\"Connect String\">[^<]*</property>|<property name=\"Connect String\">zookeeper.${POD_NAMESPACE}.svc:2181</property>|" /opt/nifi/nifi-current/conf/state-management.xml
              prop_replace nifi.cluster.flow.election.max.wait.time "1 mins"
              prop_replace nifi.cluster.flow.election.max.candidates 1
              # Surface NiFi's real log on stdout so failures are visible via kubectl logs.
              ( tail -n +1 -F /opt/nifi/nifi-current/logs/nifi-app.log 2>/dev/null & )
              exec /opt/nifi/nifi-current/bin/nifi.sh run
          ports:
            - {containerPort: 8080, name: web}
            - {containerPort: 11443, name: cluster}
          readinessProbe:
            httpGet: {path: /nifi-api/flow/about, port: web}
            periodSeconds: 10
            failureThreshold: 90
YAML

echo "Waiting for both NiFi nodes to become ready (the first run also pulls the ~2GB image)..."
kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/nifi --timeout=900s

kubectl --context "${ctx}" -n "${namespace}" port-forward svc/nifi "${local_port}:8080" >/dev/null 2>&1 &
pf_pid=$!

base_uri="http://127.0.0.1:${local_port}"
echo "Waiting for the cluster to form with 2 connected nodes..."
for _ in $(seq 1 90); do
  count="$(curl -fsS "${base_uri}/nifi-api/controller/cluster" 2>/dev/null \
    | grep -o '"status":"CONNECTED"' | wc -l || true)"
  if [ "${count}" -ge 2 ]; then
    echo "Cluster formed with ${count} connected nodes."
    NIFI_API_URI="${base_uri}" go test -count=1 -tags=integration ./internal/controller -run NodeOffloadAgainstLiveCluster -v
    exit 0
  fi
  sleep 5
done

echo "NiFi cluster did not reach 2 connected nodes" >&2
kubectl --context "${ctx}" -n "${namespace}" get pods -o wide || true
kubectl --context "${ctx}" -n "${namespace}" logs statefulset/nifi --tail=60 || true
exit 1
