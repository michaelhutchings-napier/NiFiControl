#!/usr/bin/env bash
# End-to-end test of user authentication (single-user + LDAP), managed admin grants,
# per-repository storage volumes, and Secret-sourced configuration overrides against a real
# secured Apache NiFi 2.10 in kind:
#
#   1. build + load the operator image, install cert-manager, helm install the operator,
#   2. deploy a bitnami/openldap directory seeded with user alice,
#   3. provision a secured single-node NiFiCluster (internalTLS selfSigned) with a dedicated
#      content-repository volume, a Secret-sourced nifi.properties override, single-user
#      authentication, and adminIdentities [admin],
#   4. assert the dedicated PVC exists and is mounted, the Secret-sourced property is applied,
#      and that a username/password token login works (client certs wanted, not needed) and the
#      admin grant authorizes /nifi-api/flow/about,
#   5. rotate the override Secret -> the nodes roll and apply the new value,
#   6. switch authentication to LDAP -> alice can log in but is unauthorized until she is added
#      to adminIdentities, then /flow/about succeeds.
#
# OIDC is rendered/unit-tested but not exercised live (needs an IdP); see docs.
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-secureauth-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-auth}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-auth"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
svc_host="secure-nifi.${namespace}.svc"

cleanup() {
  kubectl --context "${ctx}" -n "${namespace}" patch nificluster secure --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
  kubectl --context "${ctx}" -n "${namespace}" delete nificluster secure --ignore-not-found --wait=false >/dev/null 2>&1 || true
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=40 2>&1 >&2 || true
  echo "==== pods ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods,pvc 2>&1 >&2 || true
}

wait_cluster_ready() {
  local ready=0
  for _ in $(seq 1 120); do
    [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
    sleep 10
  done
  [ "${ready}" = "1" ] || { echo "secured NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
}

# sts_annotation <annotation> -> its value on the pod template ("" / <no value> when unset).
sts_annotation() {
  kubectl --context "${ctx}" -n "${namespace}" get statefulset secure-nifi \
    -o "go-template={{index .spec.template.metadata.annotations \"$1\"}}" 2>/dev/null || true
}

# wait_annotation_change <annotation> <previous>: the operator stamps a new checksum before the
# StatefulSet rolls; wait for the stamp, then for the rollout, then for Ready.
wait_annotation_change() {
  local annotation="$1" previous="$2" changed=0
  for _ in $(seq 1 60); do
    [ "$(sts_annotation "${annotation}")" != "${previous}" ] && { changed=1; break; }
    sleep 5
  done
  [ "${changed}" = "1" ] || { echo "pod template never picked up a new ${annotation}" >&2; dump_diagnostics; exit 1; }
  kubectl --context "${ctx}" -n "${namespace}" rollout status statefulset/secure-nifi --timeout=600s >/dev/null
  wait_cluster_ready
}

# nifi_token <username> <password> -> a NiFi JWT, or empty on failure.
nifi_token() {
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
    curl -sk -X POST "https://${svc_host}:8443/nifi-api/access/token" \
    --data-urlencode "username=$1" --data-urlencode "password=$2" 2>/dev/null || true
}

# flow_about_status <token> -> the HTTP status of GET /nifi-api/flow/about with that bearer token.
flow_about_status() {
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
    curl -sk -o /dev/null -w '%{http_code}' -H "Authorization: Bearer $1" \
    "https://${svc_host}:8443/nifi-api/flow/about" 2>/dev/null || true
}

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

echo "Installing cert-manager..."
kubectl --context "${ctx}" apply -f "${certmanager}" >/dev/null
for dep in cert-manager-webhook cert-manager cert-manager-cainjector; do
  kubectl --context "${ctx}" -n cert-manager rollout status "deploy/${dep}" --timeout=300s
done

echo "Installing the operator (helm)..."
kubectl --context "${ctx}" create namespace "${opns}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
helm --kube-context "${ctx}" upgrade --install nificontrol "${repo_root}/charts/nificontrol" \
  --namespace "${opns}" \
  --set image.repository=nificontrol --set image.tag=e2e --set image.pullPolicy=IfNotPresent \
  --set leaderElection.enabled=false >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout restart deploy/nificontrol >/dev/null
kubectl --context "${ctx}" -n "${opns}" rollout status deploy/nificontrol --timeout=180s

kubectl --context "${ctx}" create namespace "${namespace}" --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Deploying openldap (bitnami) with user alice..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata: {name: openldap}
spec:
  replicas: 1
  selector: {matchLabels: {app: openldap}}
  template:
    metadata: {labels: {app: openldap}}
    spec:
      containers:
        - name: openldap
          # bitnamilegacy: Bitnami's free openldap tags were removed from docker.io/bitnami
          # in the 2025 Broadcom catalog change; the legacy org still serves them.
          image: bitnamilegacy/openldap:2.6
          env:
            - {name: LDAP_ROOT, value: "dc=example,dc=org"}
            - {name: LDAP_ADMIN_USERNAME, value: admin}
            - {name: LDAP_ADMIN_PASSWORD, value: ldap-manager-pass}
            - {name: LDAP_USERS, value: alice}
            - {name: LDAP_PASSWORDS, value: alicelongpassword}
          ports: [{containerPort: 1389}]
---
apiVersion: v1
kind: Service
metadata: {name: openldap}
spec:
  selector: {app: openldap}
  ports: [{port: 389, targetPort: 1389}]
YAML

echo "Creating the credential and override Secrets..."
kubectl --context "${ctx}" -n "${namespace}" create secret generic admin-creds \
  --from-literal=username=admin --from-literal=password=adminlongpassword \
  --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" create secret generic ldap-manager \
  --from-literal=password=ldap-manager-pass \
  --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
kubectl --context "${ctx}" -n "${namespace}" create secret generic nifi-extra-properties \
  --from-literal=nifi.administrative.yield.duration="25 sec" \
  --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: secured cluster with a dedicated content-repository volume, a Secret-sourced override, and single-user auth..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<'YAML'
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: secure}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage:
    enabled: true
    size: 2Gi
    repositories:
      content: {size: 1Gi}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources:
    requests: {cpu: 250m, memory: 1Gi}
  internalTLS:
    enabled: true
    selfSigned: {}
  configOverrides:
    nifiPropertiesFrom:
      - name: nifi-extra-properties
  authentication:
    mode: SingleUser
    singleUser:
      credentialsSecretRef: {name: admin-creds}
    adminIdentities: [admin]
  deletionPolicy: Delete
YAML
wait_cluster_ready
echo "  cluster Ready."

echo "Phase 2: dedicated repository volume + Secret-sourced override are live..."
phase="$(kubectl --context "${ctx}" -n "${namespace}" get pvc content-repository-secure-nifi-0 -o jsonpath='{.status.phase}' 2>/dev/null || true)"
[ "${phase}" = "Bound" ] || { echo "dedicated content-repository PVC not Bound (${phase})" >&2; dump_diagnostics; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  mountpoint -q /opt/nifi/nifi-current/content_repository || { echo "content_repository is not its own mount" >&2; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  grep -q '^nifi.administrative.yield.duration=25 sec$' /opt/nifi/nifi-current/conf/nifi.properties || { echo "Secret-sourced override not applied" >&2; exit 1; }
echo "  dedicated volume mounted; Secret-sourced property applied."

echo "Phase 3: single-user token login + admin authorization..."
token="$(nifi_token admin adminlongpassword)"
[ -n "${token}" ] && [ "${#token}" -gt 40 ] || { echo "single-user token login failed (${token})" >&2; dump_diagnostics; exit 1; }
status="$(flow_about_status "${token}")"
[ "${status}" = "200" ] || { echo "admin identity is not authorized for /flow/about (HTTP ${status})" >&2; dump_diagnostics; exit 1; }
echo "  admin logged in with username/password and is authorized (HTTP 200)."

echo "Phase 4: rotating the override Secret rolls the node and applies the new value..."
overrides_before="$(sts_annotation nifi.controlnifi.io/config-overrides-checksum)"
kubectl --context "${ctx}" -n "${namespace}" create secret generic nifi-extra-properties \
  --from-literal=nifi.administrative.yield.duration="35 sec" \
  --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null
wait_annotation_change nifi.controlnifi.io/config-overrides-checksum "${overrides_before}"
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  grep -q '^nifi.administrative.yield.duration=35 sec$' /opt/nifi/nifi-current/conf/nifi.properties || { echo "rotated Secret value not applied" >&2; exit 1; }
echo "  Secret rotation rolled the node and applied the new value."

echo "Phase 5: switching authentication to LDAP..."
auth_before="$(sts_annotation nifi.controlnifi.io/auth-checksum)"
kubectl --context "${ctx}" -n "${namespace}" patch nificluster secure --type=merge -p '{
  "spec": {"authentication": {
    "mode": "LDAP",
    "singleUser": null,
    "ldap": {
      "url": "ldap://openldap.'"${namespace}"'.svc:389",
      "managerDN": "cn=admin,dc=example,dc=org",
      "managerPasswordSecretRef": {"name": "ldap-manager", "key": "password"},
      "userSearchBase": "ou=users,dc=example,dc=org",
      "userSearchFilter": "(uid={0})"
    },
    "adminIdentities": ["admin"]
  }}
}' >/dev/null
wait_annotation_change nifi.controlnifi.io/auth-checksum "${auth_before}"
token="$(nifi_token alice alicelongpassword)"
[ -n "${token}" ] && [ "${#token}" -gt 40 ] || { echo "LDAP login for alice failed (${token})" >&2; dump_diagnostics; exit 1; }
status="$(flow_about_status "${token}")"
[ "${status}" = "403" ] || { echo "expected alice to be unauthorized before the admin grant, got HTTP ${status}" >&2; exit 1; }
echo "  alice authenticated via LDAP and is (correctly) unauthorized: HTTP 403."

echo "Phase 6: adding alice to adminIdentities grants her the admin policy set..."
kubectl --context "${ctx}" -n "${namespace}" patch nificluster secure --type=merge \
  -p '{"spec":{"authentication":{"adminIdentities":["admin","alice"]}}}' >/dev/null
granted=0
for _ in $(seq 1 30); do
  status="$(flow_about_status "${token}")"
  [ "${status}" = "200" ] && { granted=1; break; }
  sleep 5
done
[ "${granted}" = "1" ] || { echo "alice was never granted the admin policy set (last HTTP ${status})" >&2; dump_diagnostics; exit 1; }
echo "  alice is authorized after the grant (HTTP 200)."

echo "PASS: dedicated repository volume, Secret-sourced overrides with rotation, single-user and LDAP authentication, and managed admin grants all verified live."
