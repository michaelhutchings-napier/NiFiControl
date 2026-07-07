#!/usr/bin/env bash
# End-to-end test of OIDC authentication (spec.authentication mode OIDC) against a real dex
# identity provider and a real secured Apache NiFi 2.10 in kind. OIDC is a browser
# redirect flow, so the reliable, non-interactive proof is the authorization *initiation*:
#
#   1. build + load the operator image, install cert-manager, helm install the operator,
#   2. deploy dex (a real OIDC provider) with a static "nifi" client and user alice,
#   3. provision a secured NiFiCluster (internalTLS selfSigned) with authentication mode OIDC
#      pointing at dex, client secret from a Secret, and adminIdentities [alice@example.com],
#   4. assert the operator wired OIDC into the running nifi.properties (discovery URL, client
#      id, identifying claim) and delivered the client secret from the Secret,
#   5. assert NiFi initiates a real OIDC handshake: GET /nifi-api/oauth2/authorization/consumer
#      returns HTTP 302 to dex's authorization endpoint carrying our client_id and the NiFi
#      callback redirect_uri. NiFi can only build this redirect by fetching dex's discovery
#      document at startup, so this proves the operator's OIDC config drives a live exchange
#      with the provider. This is the PASS gate.
#   6. best effort: drive dex's password login through the authorization-code flow and report
#      whether NiFi issues a session (not gated — dex login-form specifics vary by version).
#
# The registration id "consumer" and the paths above are NiFi 2.x internals
# (OidcRegistrationProperty.REGISTRATION_ID = "consumer"; Spring's default
# /oauth2/authorization base under the /nifi-api context).
#
# Requires kind, kubectl, docker, go, helm, internet. Run: make integration-oidc-kind
# The kind cluster is kept between runs; tear it down with KIND_DELETE=1.
set -euo pipefail

cluster="${KIND_CLUSTER:-nificontrol-oidc}"
ctx="kind-${cluster}"
opns="nificontrol-system"
namespace="nifi-oidc"
repo_root="$(cd "$(dirname "$0")/.." && pwd)"
image="nificontrol:e2e"
certmanager="https://github.com/cert-manager/cert-manager/releases/download/v1.16.2/cert-manager.yaml"
dex_image="dexidp/dex:v2.41.1"
svc_host="secure-nifi.${namespace}.svc"
dex_issuer="http://dex.${namespace}.svc:5556/dex"

cleanup() {
  # KEEP=1 leaves the NiFiCluster (and its pod logs) in place for debugging a failure.
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
  echo "==== operator logs (tail) ====" >&2
  kubectl --context "${ctx}" -n "${opns}" logs deploy/nificontrol --tail=30 2>&1 >&2 || true
  echo "==== NiFi app log OIDC lines ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- sh -c 'grep -i oidc /opt/nifi/nifi-current/logs/nifi-app.log | tail -15' 2>&1 >&2 || true
  echo "==== pods ====" >&2
  kubectl --context "${ctx}" -n "${namespace}" get pods 2>&1 >&2 || true
}

if ! kind get clusters 2>/dev/null | grep -qx "${cluster}"; then
  kind create cluster --name "${cluster}"
fi

# Side-load the host NiFi and dex images when present (kind pulls otherwise).
for img in apache/nifi:2.10.0 "${dex_image}"; do
  if docker image inspect "${img}" >/dev/null 2>&1; then
    kind load docker-image "${img}" --name "${cluster}" >/dev/null 2>&1 || true
  fi
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

echo "Deploying dex (OIDC provider) with client 'nifi' and user alice..."
# alice's password hash is dex's documented bcrypt for the literal "password".
kubectl --context "${ctx}" -n "${namespace}" apply -f - >/dev/null <<YAML
apiVersion: v1
kind: ConfigMap
metadata: {name: dex-config}
data:
  config.yaml: |
    issuer: ${dex_issuer}
    storage: {type: memory}
    web: {http: 0.0.0.0:5556}
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
          volumeMounts: [{name: config, mountPath: /etc/dex}]
      volumes:
        - name: config
          configMap: {name: dex-config}
---
apiVersion: v1
kind: Service
metadata: {name: dex}
spec:
  selector: {app: dex}
  ports: [{port: 5556, targetPort: 5556}]
YAML
kubectl --context "${ctx}" -n "${namespace}" rollout status deploy/dex --timeout=120s
echo "  waiting for dex discovery to serve..."
dexok=0
for _ in $(seq 1 30); do
  if kubectl --context "${ctx}" -n "${namespace}" run dexprobe-$$ --rm -i --restart=Never --image=curlimages/curl:8.7.1 --command -- \
       curl -fsS "${dex_issuer}/.well-known/openid-configuration" >/dev/null 2>&1; then dexok=1; break; fi
  sleep 5
done
[ "${dexok}" = "1" ] || { echo "dex discovery never became available" >&2; kubectl --context "${ctx}" -n "${namespace}" get pods >&2; exit 1; }

echo "Creating the OIDC client-secret Secret..."
kubectl --context "${ctx}" -n "${namespace}" create secret generic oidc-client \
  --from-literal=secret=nifi-oidc-secret \
  --dry-run=client -o yaml | kubectl --context "${ctx}" apply -f - >/dev/null

echo "Phase 1: provisioning a secured NiFiCluster with OIDC authentication..."
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
  resources:
    requests: {cpu: 250m, memory: 1Gi}
  internalTLS:
    enabled: true
    selfSigned: {}
  authentication:
    mode: OIDC
    oidc:
      discoveryURL: ${dex_issuer}/.well-known/openid-configuration
      clientID: nifi
      clientSecretRef: {name: oidc-client, key: secret}
      claim: email
    adminIdentities: ["alice@example.com"]
  deletionPolicy: Delete
YAML
ready=0
for _ in $(seq 1 120); do
  [ "$(kubectl --context "${ctx}" -n "${namespace}" get nificluster secure -o jsonpath='{.status.ready}' 2>/dev/null)" = "true" ] && { ready=1; break; }
  sleep 10
done
[ "${ready}" = "1" ] || { echo "secured OIDC NiFiCluster did not become Ready" >&2; dump_diagnostics; exit 1; }
echo "  cluster Ready."

echo "Phase 2: operator wired OIDC into the running nifi.properties..."
props() { kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- grep "^$1=" /opt/nifi/nifi-current/conf/nifi.properties 2>/dev/null; }
props "nifi.security.user.oidc.discovery.url" | grep -q "${dex_issuer}" || { echo "OIDC discovery URL not wired" >&2; dump_diagnostics; exit 1; }
props "nifi.security.user.oidc.client.id" | grep -q "=nifi$" || { echo "OIDC client id not wired" >&2; exit 1; }
props "nifi.security.user.oidc.claim.identifying.user" | grep -q "=email$" || { echo "OIDC identifying claim not wired" >&2; exit 1; }
# The client secret must be delivered from the Secret; assert it is present without printing it.
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  sh -c 'grep -q "^nifi.security.user.oidc.client.secret=.\+" /opt/nifi/nifi-current/conf/nifi.properties' || { echo "OIDC client secret not delivered from the Secret" >&2; exit 1; }
echo "  discovery URL, client id, identifying claim wired; client secret delivered."

echo "Phase 3a (PASS gate): NiFi built the OIDC client from dex's discovery document..."
# The oidcAuthorizationCodeAuthenticationProvider bean only exists once NiFi has successfully
# constructed a ClientRegistration, which it does by fetching dex's discovery document at
# startup. Its presence proves the operator's OIDC config reached and was accepted by dex.
kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
  sh -c 'grep -q "oidcAuthorizationCodeAuthenticationProvider" /opt/nifi/nifi-current/logs/nifi-app.log' \
  || { echo "NiFi did not register the OIDC authentication provider (discovery likely failed)" >&2; dump_diagnostics; exit 1; }
echo "  NiFi registered the OIDC authentication provider from dex's discovery."

echo "Phase 3b (PASS gate): NiFi initiates a real OIDC handshake with dex..."
# A 302 to dex's authorize endpoint carrying our client_id proves the operator's OIDC config
# drives a live exchange. The authorize URL itself comes from dex's discovery document. Try
# the known candidate initiation paths and accept only a redirect that actually points at dex
# (a bare /oauth2/authorization without the /nifi-api context 302s to the UI, not to dex).
redirect=""
for path in "nifi-api/oauth2/authorization/consumer" "oauth2/authorization/consumer"; do
  candidate="$(kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- \
    curl -sk -o /dev/null -w '%{redirect_url}' "https://${svc_host}:8443/${path}" 2>/dev/null || true)"
  echo "  /${path} -> ${candidate:-<none>}"
  case "${candidate}" in "${dex_issuer}/auth"*) redirect="${candidate}"; break;; esac
done
case "${redirect}" in
  "${dex_issuer}/auth"*) : ;;
  *) echo "no initiation path produced a 302 to dex's authorize endpoint" >&2; dump_diagnostics; exit 1;;
esac
case "${redirect}" in
  *"client_id=nifi"*) : ;;
  *) echo "authorize redirect is missing client_id=nifi: ${redirect}" >&2; exit 1;;
esac
case "${redirect}" in
  *"access%2Foidc%2Fcallback"*|*"access/oidc/callback"*) : ;;
  *) echo "authorize redirect is missing the NiFi callback redirect_uri: ${redirect}" >&2; exit 1;;
esac
echo "  NiFi redirected to dex's authorize endpoint with client_id=nifi and the NiFi callback."

echo "Phase 4 (best effort): full authorization-code login as alice via dex..."
# Driven entirely from inside the NiFi pod, which reaches both dex (Service DNS) and NiFi
# (localhost) and has curl. Not gated on: dex's login-form flow is version-specific, and
# Phase 3 already proves the operator's OIDC wiring drives a real handshake.
full_flow() {
  kubectl --context "${ctx}" -n "${namespace}" exec secure-nifi-0 -c nifi -- sh -s <<SH 2>/dev/null
set -e
J=/tmp/oidc-jar; DJ=/tmp/dex-jar; rm -f "\$J" "\$DJ"
base="https://${svc_host}:8443"
# 1. NiFi initiates -> dex authorize URL (store NiFi's state cookie in \$J)
auth="\$(curl -sk -c "\$J" -o /dev/null -w '%{redirect_url}' "\$base/nifi-api/oauth2/authorization/consumer")"
[ -n "\$auth" ] || { echo "no authorize redirect"; exit 1; }
# 2. GET dex authorize; single connector auto-redirects to the login form (follow, keep dex cookies)
login="\$(curl -s -c "\$DJ" -b "\$DJ" -L -o /tmp/dex-login.html -w '%{url_effective}' "\$auth")"
# form action is like /dex/auth/<connector>/login?back=&state=<reqid>
action="\$(grep -o 'action="[^"]*"' /tmp/dex-login.html | head -1 | sed 's/action="//;s/"//')"
[ -n "\$action" ] || { echo "no dex login form action"; exit 1; }
case "\$action" in http*) post="\$action";; /*) post="${dex_issuer%/dex}\$action";; *) post="\$login/\$action";; esac
# 3. POST alice's credentials -> dex approves (skipApprovalScreen) -> 302 to NiFi callback with code
cb="\$(curl -s -c "\$DJ" -b "\$DJ" -o /dev/null -w '%{redirect_url}' --data-urlencode 'login=alice' --data-urlencode 'password=password' "\$post")"
# follow dex approval redirects until we bounce back to the NiFi callback
hops=0
while [ -n "\$cb" ] && [ \$hops -lt 5 ]; do
  case "\$cb" in
    *"/nifi-api/access/oidc/callback"*)
      # 4. hand the code to NiFi's callback with NiFi's state cookie
      final="\$(curl -sk -c "\$J" -b "\$J" -o /dev/null -w '%{http_code}' "\$cb")"
      echo "callback status: \$final"
      break;;
    http*)
      cb="\$(curl -s -c "\$DJ" -b "\$DJ" -o /dev/null -w '%{redirect_url}' "\$cb")";;
    /*)
      cb="\$(curl -s -c "\$DJ" -b "\$DJ" -o /dev/null -w '%{redirect_url}' "${dex_issuer%/dex}\$cb")";;
    *) break;;
  esac
  hops=\$((hops+1))
done
# 5. did NiFi establish a session for alice? current-user should not be anonymous.
who="\$(curl -sk -b "\$J" "\$base/nifi-api/flow/current-user" 2>/dev/null)"
echo "current-user: \$who"
echo "\$who" | grep -q 'alice@example.com' || exit 1
SH
}
if full_flow | tee /tmp/oidc-fullflow.log && grep -q 'alice@example.com' /tmp/oidc-fullflow.log; then
  echo "  BONUS: full OIDC authorization-code login as alice succeeded."
else
  echo "  full login flow not asserted (dex login-form flow is version-specific); Phase 3 is the gate."
fi

echo "PASS: the operator configured OIDC and NiFi performs a real OIDC handshake with dex (client_id, callback, and discovery-driven authorize endpoint all verified)."
