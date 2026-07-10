#!/usr/bin/env bash
# End-to-end test of NiFiAutoscaler against a real Kubernetes API server in kind. The
# autoscaler reconciles off the target's CR (not a running NiFi), so this harness does not
# start NiFi — it focuses on the rendered backends:
#
#   1. install the CRDs and run the operator out-of-cluster against kind,
#   2. apply a minimal Internal NiFiCluster (the scale target),
#   3. Resource (cpu) metric -> the operator renders a native HorizontalPodAutoscaler
#      (no KEDA required); the autoscaler reports Ready, mode HPA,
#   4. Prometheus metric with KEDA absent -> Ready=False, reason KEDANotInstalled, no ScaledObject,
#   5. install the real KEDA CRDs -> the operator renders a ScaledObject that validates against
#      KEDA's own schema; the autoscaler reports Ready, mode KEDA,
#   6. Prometheus authentication -> the operator renders a KEDA TriggerAuthentication (validated
#      against KEDA's schema) and wires the trigger's authModes/authenticationRef.
#
# Requires kind, kubectl, docker, go, and internet access (for the KEDA CRDs). Run:
#   make integration-autoscaler-kind
# Reuse a warm cluster (e.g. the observability one) with KIND_CLUSTER=<name>. Tear down with
# KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-autoscaler}"
ctx="kind-${cluster}"
namespace="nifi-autoscaler"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
kubeconfig="$(mktemp)"
operator_bin="$(mktemp -u)"
operator_pid=""
keda_crds="https://github.com/kedacore/keda/releases/download/v2.14.0/keda-2.14.0-crds.yaml"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" delete nifiautoscaler --all --ignore-not-found --wait=false >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster as --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster as --ignore-not-found --wait=false >/dev/null 2>&1 || true
  sleep 3
  [ -n "${operator_pid}" ] && kill "${operator_pid}" >/dev/null 2>&1 || true
  rm -f "${kubeconfig}" "${operator_bin}"
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

echo "Installing CRDs..."
# Server-side apply: the nificlusters CRD exceeds the 262144-byte client-side last-applied
# annotation limit.
kubectl --context "${ctx}" apply --server-side --force-conflicts -f "${repo_root}/config/crd/bases/" >/dev/null

# Ensure KEDA is absent for the degradation check (it may linger from a prior run).
kubectl --context "${ctx}" delete crd scaledobjects.keda.sh --ignore-not-found >/dev/null 2>&1 || true

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Building and starting the operator out-of-cluster..."
( cd "${repo_root}" && go build -o "${operator_bin}" ./cmd/manager )
kind get kubeconfig --name "${cluster}" > "${kubeconfig}"
KUBECONFIG="${kubeconfig}" "${operator_bin}" --metrics-bind-address=0 --health-probe-bind-address=0 \
  > /tmp/nificontrol-autoscaler-operator.log 2>&1 &
operator_pid=$!
echo "operator pid ${operator_pid}"

echo "Applying a minimal Internal NiFiCluster (the scale target)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: as}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  deletionPolicy: Delete
YAML
# Wait only for the scale subresource to exist (no need to wait for NiFi to start).
for _ in $(seq 1 30); do
  kubectl --context "${ctx}" -n "${namespace}" get nificluster as >/dev/null 2>&1 && break
  sleep 2
done

echo "Phase 1: Resource (cpu) metric -> native HorizontalPodAutoscaler..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiAutoscaler
metadata: {name: as-cpu}
spec:
  target: {kind: NiFiCluster, name: as}
  minReplicas: 1
  maxReplicas: 6
  metrics:
    - type: Resource
      resource: {name: cpu, targetAverageUtilization: 70}
YAML
phase1_ok=0
for _ in $(seq 1 30); do
  mode="$(kubectl --context "${ctx}" -n "${namespace}" get nifiautoscaler as-cpu -o jsonpath='{.status.mode}' 2>/dev/null || true)"
  hpa="$(kubectl --context "${ctx}" -n "${namespace}" get hpa as-cpu-nifiautoscaler -o jsonpath='{.spec.scaleTargetRef.kind}' 2>/dev/null || true)"
  if [ "${mode}" = "HPA" ] && [ "${hpa}" = "NiFiCluster" ]; then
    echo "  HPA as-cpu-nifiautoscaler rendered (scaleTargetRef NiFiCluster); autoscaler mode=HPA."
    phase1_ok=1
    break
  fi
  sleep 3
done
[ "${phase1_ok}" = "1" ] || { echo "expected an HPA in mode=HPA (got mode='${mode:-<none>}', hpa='${hpa:-<none>}')" >&2; exit 1; }

echo "Phase 2: Prometheus metric with KEDA absent -> Ready=False (KEDANotInstalled)..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiAutoscaler
metadata: {name: as-queue}
spec:
  target: {kind: NiFiCluster, name: as}
  minReplicas: 1
  maxReplicas: 6
  metrics:
    - type: Prometheus
      prometheus:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(nifi_amount_items_queued)
        threshold: "10000"
YAML
phase2_ok=0
for _ in $(seq 1 30); do
  reason="$(kubectl --context "${ctx}" -n "${namespace}" get nifiautoscaler as-queue -o jsonpath='{.status.conditions[?(@.type=="Ready")].reason}' 2>/dev/null || true)"
  if [ "${reason}" = "KEDANotInstalled" ]; then
    echo "  Ready=False reason=KEDANotInstalled (graceful degradation)."
    phase2_ok=1
    break
  fi
  sleep 3
done
[ "${phase2_ok}" = "1" ] || { echo "expected Ready reason KEDANotInstalled, got '${reason:-<none>}'" >&2; exit 1; }

echo "Phase 3: installing the real KEDA CRDs -> ScaledObject rendered, mode KEDA..."
kubectl --context "${ctx}" apply --server-side -f "${keda_crds}" >/dev/null
kubectl --context "${ctx}" wait --for=condition=Established crd/scaledobjects.keda.sh --timeout=60s >/dev/null
kubectl --context "${ctx}" -n "${namespace}" annotate nifiautoscaler as-queue nifi.controlnifi.io/rereconcile="$(date +%s)" --overwrite >/dev/null
phase3_ok=0
for _ in $(seq 1 30); do
  mode="$(kubectl --context "${ctx}" -n "${namespace}" get nifiautoscaler as-queue -o jsonpath='{.status.mode}' 2>/dev/null || true)"
  so="$(kubectl --context "${ctx}" -n "${namespace}" get scaledobject as-queue-nifiautoscaler -o jsonpath='{.spec.scaleTargetRef.kind}' 2>/dev/null || true)"
  if [ "${mode}" = "KEDA" ] && [ "${so}" = "NiFiCluster" ]; then
    echo "  ScaledObject as-queue-nifiautoscaler validated against KEDA's schema; autoscaler mode=KEDA."
    kubectl --context "${ctx}" -n "${namespace}" get scaledobject as-queue-nifiautoscaler -o jsonpath='{.spec.triggers[0].type}={.spec.triggers[0].metadata.query}{"\n"}' || true
    phase3_ok=1
    break
  fi
  sleep 3
done
[ "${phase3_ok}" = "1" ] || { echo "operator did not render a ScaledObject in mode=KEDA (mode='${mode:-<none>}', so='${so:-<none>}')" >&2; exit 1; }

echo "Phase 4: Prometheus authentication -> TriggerAuthentication rendered and validated by KEDA..."
kubectl --context "${ctx}" -n "${namespace}" create secret generic prom-creds \
  --from-literal=bearerToken=dummy --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiAutoscaler
metadata: {name: as-secure}
spec:
  target: {kind: NiFiCluster, name: as}
  minReplicas: 1
  maxReplicas: 6
  metrics:
    - type: Prometheus
      prometheus:
        serverAddress: https://prometheus.monitoring.svc:9090
        query: sum(nifi_amount_items_queued)
        threshold: "10000"
        authentication:
          mode: Bearer
          secretName: prom-creds
YAML
phase4_ok=0
for _ in $(seq 1 30); do
  authmodes="$(kubectl --context "${ctx}" -n "${namespace}" get scaledobject as-secure-nifiautoscaler -o jsonpath='{.spec.triggers[0].metadata.authModes}' 2>/dev/null || true)"
  authref="$(kubectl --context "${ctx}" -n "${namespace}" get scaledobject as-secure-nifiautoscaler -o jsonpath='{.spec.triggers[0].authenticationRef.name}' 2>/dev/null || true)"
  taparam=""
  [ -n "${authref}" ] && taparam="$(kubectl --context "${ctx}" -n "${namespace}" get triggerauthentication "${authref}" -o jsonpath='{.spec.secretTargetRef[0].parameter}' 2>/dev/null || true)"
  if [ "${authmodes}" = "bearer" ] && [ -n "${authref}" ] && [ "${taparam}" = "bearerToken" ]; then
    echo "  trigger authModes=bearer -> TriggerAuthentication ${authref} validated by KEDA (secretTargetRef[0].parameter=bearerToken)."
    phase4_ok=1
    break
  fi
  sleep 3
done
[ "${phase4_ok}" = "1" ] || { echo "operator did not render a valid authenticated trigger + TriggerAuthentication (authModes='${authmodes:-<none>}', authRef='${authref:-<none>}', taParam='${taparam:-<none>}')" >&2; exit 1; }

echo "PASS: NiFiAutoscaler renders an HPA, degrades gracefully without KEDA, renders a valid KEDA ScaledObject, and renders a KEDA TriggerAuthentication for a secured Prometheus."
