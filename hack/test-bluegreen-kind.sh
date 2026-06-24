#!/usr/bin/env bash
# Run the transactional BlueGreen integration test against a real Apache NiFi 2.10
# instance running inside a local kind cluster. NiFi runs in controlled HTTP development
# mode; the test builds boundary ports/connections and drives a real green promotion.
#
# Requires kind, kubectl, docker, and curl. Run with: make integration-bluegreen-kind
#
# By default the kind cluster and the NiFi deployment are KEPT between runs: the cluster's
# containerd caches the ~2GB apache/nifi:2.10.0 image after the first pull, and the already
# running NiFi is reused, so subsequent runs only execute the test. Tear everything down
# with KIND_DELETE=1:
#   KIND_DELETE=1 make integration-bluegreen-kind
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-bluegreen}"
ctx="kind-${cluster}"
namespace="nifi"
local_port="${NIFI_INTEGRATION_PORT:-18080}"
pf_pid=""

cleanup() {
  [ -n "${pf_pid}" ] && kill "${pf_pid}" >/dev/null 2>&1 || true
  # The cluster and NiFi are preserved between runs so the image is pulled only once.
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

# Best-effort side-load of the host image to avoid an in-node pull. This can fail with a
# containerd "content digest not found" error on Docker setups that use the containerd
# image store; the in-node pull (and the persistent cluster's image cache) is the fallback.
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
  name: nifi
  labels: {app: nifi}
spec:
  replicas: 1
  selector: {matchLabels: {app: nifi}}
  template:
    metadata:
      labels: {app: nifi}
    spec:
      containers:
        - name: nifi
          image: apache/nifi:2.10.0
          imagePullPolicy: IfNotPresent
          command: ["/bin/bash", "-ec"]
          args:
            - |
              . /opt/nifi/scripts/common.sh
              prop_replace nifi.web.http.host 0.0.0.0
              prop_replace nifi.web.http.port 8080
              prop_replace nifi.web.https.host ""
              prop_replace nifi.web.https.port ""
              prop_replace nifi.security.keystore ""
              prop_replace nifi.security.truststore ""
              prop_replace nifi.remote.input.secure false
              exec /opt/nifi/nifi-current/bin/nifi.sh run
          ports:
            - {containerPort: 8080, name: web}
          readinessProbe:
            httpGet: {path: /nifi-api/flow/about, port: web}
            periodSeconds: 10
            failureThreshold: 60
---
apiVersion: v1
kind: Service
metadata:
  name: nifi
spec:
  selector: {app: nifi}
  ports:
    - {name: web, port: 8080, targetPort: web}
YAML

echo "Waiting for NiFi to become ready in kind (the first run also pulls the ~2GB image)..."
kubectl --context "${ctx}" -n "${namespace}" rollout status deploy/nifi --timeout=600s

kubectl --context "${ctx}" -n "${namespace}" port-forward svc/nifi "${local_port}:8080" >/dev/null 2>&1 &
pf_pid=$!

base_uri="http://127.0.0.1:${local_port}"
for _ in $(seq 1 60); do
  if curl -fsS "${base_uri}/nifi-api/flow/about" >/dev/null 2>&1; then
    NIFI_API_URI="${base_uri}" go test -count=1 -tags=integration ./internal/controller -run BlueGreenSwitchAgainstLiveNiFi -v
    exit 0
  fi
  sleep 2
done

echo "NiFi API did not become reachable through the kind port-forward" >&2
kubectl --context "${ctx}" -n "${namespace}" logs deploy/nifi --tail=50 || true
exit 1
