#!/usr/bin/env bash
# Provision a real Apache NiFi 2.10.0 instance secured with HTTPS and client-certificate
# (mutual TLS) authentication, then run the secured integration test against it. This
# mirrors the operator's managed internal-TLS runtime: a self-signed CA signs the server
# and operator-client certificates, NiFi consumes PKCS12 keystores, and the operator
# client certificate's subject DN is configured as the NiFi initial admin identity.
#
# Requires Docker and openssl. Run with: make integration-nifi-tls
set -euo pipefail

container="nificontrol-integration-tls-${$}"
port="${NIFI_TLS_INTEGRATION_PORT:-18443}"
workdir="$(mktemp -d)"
password="nificontrol-$(date +%s)"
admin_identity="CN=nificontrol-operator"
node_identity="CN=nifi-server"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
  rm -rf "${workdir}"
}
trap cleanup EXIT

# --- Certificate authority and leaf certificates ----------------------------------
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -keyout "${workdir}/ca.key" -out "${workdir}/ca.crt" \
  -subj "/CN=NiFiControl Integration CA" >/dev/null 2>&1

# Server / node certificate: serverAuth + clientAuth, SANs for localhost.
openssl req -newkey rsa:2048 -nodes \
  -keyout "${workdir}/server.key" -out "${workdir}/server.csr" \
  -subj "/CN=nifi-server" >/dev/null 2>&1
cat >"${workdir}/server.ext" <<EOF
subjectAltName = DNS:localhost,DNS:${container},IP:127.0.0.1
extendedKeyUsage = serverAuth,clientAuth
keyUsage = digitalSignature,keyEncipherment
EOF
openssl x509 -req -in "${workdir}/server.csr" -CA "${workdir}/ca.crt" -CAkey "${workdir}/ca.key" \
  -CAcreateserial -days 2 -extfile "${workdir}/server.ext" -out "${workdir}/server.crt" >/dev/null 2>&1

# Operator client certificate: clientAuth. Its subject DN is the NiFi admin identity.
openssl req -newkey rsa:2048 -nodes \
  -keyout "${workdir}/client.key" -out "${workdir}/client.csr" \
  -subj "/CN=nificontrol-operator" >/dev/null 2>&1
cat >"${workdir}/client.ext" <<EOF
extendedKeyUsage = clientAuth
keyUsage = digitalSignature,keyEncipherment
EOF
openssl x509 -req -in "${workdir}/client.csr" -CA "${workdir}/ca.crt" -CAkey "${workdir}/ca.key" \
  -CAcreateserial -days 2 -extfile "${workdir}/client.ext" -out "${workdir}/client.crt" >/dev/null 2>&1

# PKCS12 keystore (server identity) and truststore (CA), consumed by NiFi.
openssl pkcs12 -export -in "${workdir}/server.crt" -inkey "${workdir}/server.key" \
  -certfile "${workdir}/ca.crt" -name nifi-server \
  -out "${workdir}/keystore.p12" -passout "pass:${password}" >/dev/null 2>&1
openssl pkcs12 -export -nokeys -in "${workdir}/ca.crt" -caname nificontrol-ca \
  -out "${workdir}/truststore.p12" -passout "pass:${password}" >/dev/null 2>&1

# Operator-rendered authorizers.xml: the client certificate DN is the initial admin.
cat >"${workdir}/authorizers.xml" <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<authorizers>
    <userGroupProvider>
        <identifier>file-user-group-provider</identifier>
        <class>org.apache.nifi.authorization.FileUserGroupProvider</class>
        <property name="Users File">./conf/users.xml</property>
        <property name="Initial User Identity admin">${admin_identity}</property>
        <property name="Initial User Identity node">${node_identity}</property>
    </userGroupProvider>
    <accessPolicyProvider>
        <identifier>file-access-policy-provider</identifier>
        <class>org.apache.nifi.authorization.FileAccessPolicyProvider</class>
        <property name="User Group Provider">file-user-group-provider</property>
        <property name="Authorizations File">./conf/authorizations.xml</property>
        <property name="Initial Admin Identity">${admin_identity}</property>
        <property name="Node Identity node">${node_identity}</property>
    </accessPolicyProvider>
    <authorizer>
        <identifier>managed-authorizer</identifier>
        <class>org.apache.nifi.authorization.StandardManagedAuthorizer</class>
        <property name="Access Policy Provider">file-access-policy-provider</property>
    </authorizer>
</authorizers>
EOF

chmod -R a+r "${workdir}"

docker run --rm -d \
  --name "${container}" \
  -p "127.0.0.1:${port}:8443" \
  -v "${workdir}:/opt/tls:ro" \
  -e "NIFI_KEYSTORE_PASSWORD=${password}" \
  --entrypoint /bin/bash \
  apache/nifi:2.10.0 -ec '
    . /opt/nifi/scripts/common.sh
    cp /opt/tls/keystore.p12 /opt/nifi/nifi-current/conf/keystore.p12
    cp /opt/tls/truststore.p12 /opt/nifi/nifi-current/conf/truststore.p12
    cp /opt/tls/authorizers.xml /opt/nifi/nifi-current/conf/authorizers.xml
    prop_replace nifi.web.http.host ""
    prop_replace nifi.web.http.port ""
    prop_replace nifi.web.https.host 0.0.0.0
    prop_replace nifi.web.https.port 8443
    prop_replace nifi.web.proxy.host "localhost:8443,127.0.0.1:8443"
    prop_replace nifi.security.keystore ./conf/keystore.p12
    prop_replace nifi.security.keystoreType PKCS12
    prop_replace nifi.security.keystorePasswd "${NIFI_KEYSTORE_PASSWORD}"
    prop_replace nifi.security.keyPasswd "${NIFI_KEYSTORE_PASSWORD}"
    prop_replace nifi.security.truststore ./conf/truststore.p12
    prop_replace nifi.security.truststoreType PKCS12
    prop_replace nifi.security.truststorePasswd "${NIFI_KEYSTORE_PASSWORD}"
    prop_replace nifi.security.needClientAuth true
    prop_replace nifi.security.allow.anonymous.authentication false
    prop_replace nifi.security.user.authorizer managed-authorizer
    prop_replace nifi.security.user.login.identity.provider ""
    prop_replace nifi.remote.input.secure true
    exec /opt/nifi/nifi-current/bin/nifi.sh run
  ' >/dev/null

base_uri="https://127.0.0.1:${port}"
for _ in $(seq 1 120); do
  if curl -fsS \
      --cert "${workdir}/client.crt" --key "${workdir}/client.key" --cacert "${workdir}/ca.crt" \
      "${base_uri}/nifi-api/flow/current-user" >/dev/null 2>&1; then
    NIFI_TLS_API_URI="${base_uri}" \
    NIFI_TLS_CA="${workdir}/ca.crt" \
    NIFI_TLS_CLIENT_CERT="${workdir}/client.crt" \
    NIFI_TLS_CLIENT_KEY="${workdir}/client.key" \
      go test -count=1 -tags=integration ./integration -run MutualTLS -v
    exit 0
  fi
  sleep 2
done

echo "NiFi did not become ready over mTLS" >&2
docker logs "${container}" || true
exit 1
