#!/usr/bin/env bash
set -euo pipefail

container="nificontrol-integration-${$}"
port="${NIFI_INTEGRATION_PORT:-18080}"

cleanup() {
  docker rm -f "${container}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run --rm -d \
  --name "${container}" \
  -p "127.0.0.1:${port}:8080" \
  --entrypoint /bin/bash \
  apache/nifi:2.10.0 -ec '
    . /opt/nifi/scripts/common.sh
    prop_replace nifi.web.http.host 0.0.0.0
    prop_replace nifi.web.http.port 8080
    prop_replace nifi.web.https.host ""
    prop_replace nifi.web.https.port ""
    prop_replace nifi.security.keystore ""
    prop_replace nifi.security.keystoreType ""
    prop_replace nifi.security.keystorePasswd ""
    prop_replace nifi.security.keyPasswd ""
    prop_replace nifi.security.truststore ""
    prop_replace nifi.security.truststoreType ""
    prop_replace nifi.security.truststorePasswd ""
    prop_replace nifi.remote.input.secure false
    exec /opt/nifi/nifi-current/bin/nifi.sh run
  ' >/dev/null

for _ in $(seq 1 90); do
  if curl -fsS "http://127.0.0.1:${port}/nifi-api/flow/about" >/dev/null; then
    NIFI_API_URI="http://127.0.0.1:${port}" go test -count=1 -tags=integration ./internal/controller ./integration -v
    exit 0
  fi
  sleep 2
done

docker logs "${container}"
exit 1
