#!/usr/bin/env bash
# End-to-end test of OIDC authentication over HTTPS with a PRIVATE CA
# (spec.authentication.oidc.caSecretRef) against a real dex and a real secured Apache NiFi
# 2.10 in kind. This exercises the operator's private-CA truststore path:
#
#   1. build + load the operator image, install cert-manager, helm install the operator,
#   2. mint a self-signed CA and a dex serving certificate (SAN dex.<ns>.svc) via cert-manager,
#   3. run dex over HTTPS using that certificate, issuer https://dex.<ns>.svc:5556/dex,
#   4. provision a secured NiFiCluster with OIDC pointing at dex's HTTPS discovery URL and
#      caSecretRef set to the CA Secret,
#   5. assert NiFi built its OIDC client from dex's discovery document — which it can only
#      fetch over HTTPS if it trusts the private CA, so the provider's existence proves the
#      operator added the CA to NiFi's truststore and set the OIDC truststore strategy to
#      NIFI. This is the PASS gate: without the CA, the HTTPS discovery fetch fails and no
#      provider is built,
#   6. assert NiFi initiates a real OIDC handshake to dex's HTTPS authorize endpoint.
#
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-oidc-tls-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-oidctls}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-oidctls"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
dex_image="dexidp/dex:v2.41.1"
svc_host="secure-nifi.${namespace}.svc"
dex_host="dex.${namespace}.svc"
dex_issuer="https://${dex_host}:5556/dex"

cleanup() {
  if [ "${KEEP:-0}" != "1" ]; then
    kubectl --context "${ctx}" -n "${namespace}" patch nificluster secure --type=merge -p '{"spec":{"deletionPolicy":"Delete"}}' >/dev/null 2>&1 || true
    kubectl --context "${ctx}" -n "${namespace}" delete nificluster secure --ignore-not-found --wait=false >/dev/null 2>&1 || true
  fi
  if [ "${KIND_DELETE:-0}" = "1" ]; then
    kind delete cluster --name "${cluster}" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

dump_diagnostics() {
  echo "==== NiFiCluster status ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.conditions}' 2>&1 | tr ',' '\n' >&2 || true
  echo "==== NiFi app log OIDC/trust lines ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- sh -c 'grep -iE "oidc|truststore|PKIX|unable to find|discovery" /opt/nifi/nifi-current/logs/nifi-app.log | tail -20' 2>&1 >&2 || true
  kubectl --context "${ctx}" -n "${namespace}" get pods 2>&1 >&2 || true
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi
for img in apache/nifi:2.10.0 "${dex_image}"; do
  docker image inspect "${img}" >/dev/null 2>&1 && kind load docker-image "${img}" --name "${cluster}" >/dev/null 2>&1 || true
done

echo "Building the operator image and loading it into kind..."
( cd "${repo_root}" && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /tmp/nificontrol-manager ./cmd/manager )
build_dir="$(mktemp -d)"; cp /tmp/nificontrol-manager "${build_dir}/manager"
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

echo "Minting a private CA and a dex serving certificate via cert-manager..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: selfsigned}
spec: {selfSigned: {}}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: dex-ca}
spec:
  isCA: true
  commonName: nificontrol-oidc-test-ca
  secretName: dex-ca
  privateKey: {algorithm: ECDSA, size: 256}
  issuerRef: {name: selfsigned, kind: Issuer}
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata: {name: dex-ca}
spec:
  ca: {secretName: dex-ca}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata: {name: dex-tls}
spec:
  secretName: dex-tls
  dnsNames: ["${dex_host}"]
  issuerRef: {name: dex-ca, kind: Issuer}
YAML
kubectl --context "${ctx}" -n "${namespace}" wait --for=condition=Ready certificate/dex-tls --timeout=120s

echo "Deploying dex over HTTPS with the issued certificate..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: dex-config}
data:
  config.yaml: |
    issuer: ${dex_issuer}
    storage: {type: memory}
    web:
      https: 0.0.0.0:5556
      tlsCert: /tls/tls.crt
      tlsKey: /tls/tls.key
    oauth2: {skipApprovalScreen: true}
    staticClients:
      - id: nifi
        secret: nifi-oidc-secret
        name: NiFi
        redirectURIs: ["https://${svc_host}:8443/nifi-api/access/oidc/callback"]
    enablePasswordDB: true
    staticPasswords:
      - email: "alice@example.com"
        hash: "\$2a\$10\$2b2cU8CPhOTaGrs1HRQuAueS7JTT5ZHsHSzYiFPm1leZck7Mc8T4u"
        username: "alice"
        userID: "08a8684b-db88-4b73-90a9-3cd1661f5466"
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: dex}
spec:
  replicas: 1
  selector: {matchLabels: {app: dex}}
  template:
    metadata: {labels: {app: dex}}
    spec:
      containers:
        - name: dex
          image: ${dex_image}
          args: ["dex", "serve", "/etc/dex/config.yaml"]
          ports: [{containerPort: 5556}]
          volumeMounts:
            - {name: config, mountPath: /etc/dex}
            - {name: tls, mountPath: /tls, readOnly: true}
      volumes:
        - name: config
          configMap: {name: dex-config}
        - name: tls
          secret: {secretName: dex-tls}
---
apiVersion: v1
kind: Service
metadata: {name: dex}
spec:
  selector: {app: dex}
  ports: [{port: 5556, targetPort: 5556}]
YAML
kubectl --context "${ctx}" -n "${namespace}" rollout status deploy/dex --timeout=120s
echo "  waiting for dex HTTPS discovery (trusting the CA)..."
dexok=0
for _ in $(seq 1 30); do
  if kubectl --context "${ctx}" -n "${namespace}" run dexprobe-$$ --rm -i --restart=Never --image=curlimages/curl:8.7.1 \
       --overrides='{"spec":{"volumes":[{"name":"ca","secret":{"secretName":"dex-ca"}}],"containers":[{"name":"dexprobe-'$$'","image":"curlimages/curl:8.7.1","stdin":true,"volumeMounts":[{"name":"ca","mountPath":"/ca","readOnly":true}],"command":["curl","-fsS","--cacert","/ca/ca.crt","'"${dex_issuer}"'/.well-known/openid-configuration"]}]}}' >/dev/null 2>&1; then dexok=1; break; fi
  sleep 5
done
[ "${dexok}" = "1" ] || { echo "dex HTTPS discovery never became available" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }

echo "Creating the OIDC client-secret Secret..."
kubectl --context "${ctx}" -n "${namespace}" create secret generic oidc-client \
  --from-literal=secret=nifi-oidc-secret --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: secured NiFiCluster with OIDC over HTTPS and a private CA..."
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: secure}
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  storage: {enabled: false}
  jvm: {heapInitial: 512m, heapMax: 1g}
  resources: {requests: {cpu: 250m, memory: 1Gi}}
  internalTLS: {enabled: true, selfSigned: {}}
  authentication:
    mode: OIDC
    oidc:
      discoveryURL: ${dex_issuer}/.well-known/openid-configuration
      clientID: nifi
      clientSecretRef: {name: oidc-client, key: secret}
      claim: email
      caSecretRef: {name: dex-ca, key: ca.crt}
    adminIdentities: ["alice@example.com"]
  deletionPolicy: Delete
YAML
ready=0
for _ in $(seq 1 120); do
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "secured OIDC-over-HTTPS NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
echo "  cluster Ready."

echo "Phase 2: operator built NiFi's truststore for the private CA (strategy NIFI)..."
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  grep -q "^nifi.security.user.oidc.truststore.strategy=NIFI$" /opt/nifi/nifi-current/conf/nifi.properties \
  || { echo "OIDC truststore strategy was not switched to NIFI" >&2; dump_diagnostics; exit 1; }
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  test -f /opt/nifi/nifi-current/nificontrol-truststores/oidc-truststore.p12 \
  || { echo "operator did not build the augmented OIDC truststore" >&2; exit 1; }
echo "  strategy=NIFI and the augmented truststore are present."

echo "Phase 3 (PASS gate): NiFi built the OIDC client from dex's HTTPS discovery document..."
# This only succeeds if NiFi trusts the private CA over HTTPS — the whole point of the feature.
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  sh -c 'grep -q "oidcAuthorizationCodeAuthenticationProvider" /opt/nifi/nifi-current/logs/nifi-app.log' \
  || { echo "NiFi did not register the OIDC provider (HTTPS discovery over the private CA failed)" >&2; dump_diagnostics; exit 1; }
echo "  NiFi fetched dex's HTTPS discovery document over the private CA and built the OIDC provider."

echo "Phase 4 (PASS gate): NiFi initiates a real OIDC handshake to dex over HTTPS..."
redirect=""
for path in "nifi-api/oauth2/authorization/consumer" "oauth2/authorization/consumer"; do
  candidate="$(kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
    curl -sk -o /dev/null -w '%{redirect_url}' "https://${svc_host}:8443/${path}" 2>/dev/null || true)"
  echo "  /${path} -> ${candidate:-<none>}"
  case "${candidate}" in "${dex_issuer}/auth"*) redirect="${candidate}"; break;; esac
done
case "${redirect}" in
  "${dex_issuer}/auth"*"client_id=nifi"*) : ;;
  "${dex_issuer}/auth"*) echo "authorize redirect missing client_id=nifi: ${redirect}" >&2; exit 1;;
  *) echo "no initiation path produced a 302 to dex's HTTPS authorize endpoint" >&2; dump_diagnostics; exit 1;;
esac
echo "  NiFi redirected to dex's HTTPS authorize endpoint with client_id=nifi."

echo "PASS: OIDC over HTTPS with a private CA works end to end — the operator added the CA to NiFi's truststore, NiFi fetched HTTPS discovery from dex, and initiates a real OIDC handshake."
