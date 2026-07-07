# Internal HTTPS and Mutual TLS

NiFiControl can run an operator-managed (`mode: Internal`) Apache NiFi 2.10 cluster
with HTTPS and certificate authentication, using [cert-manager](https://cert-manager.io)
to issue and rotate the certificates. This document explains the prerequisites, the
trust model, certificate ownership and rotation, and how to migrate an existing
development-mode (HTTP) cluster to managed HTTPS.

## Prerequisites

- **cert-manager must already be installed** in the cluster (CRDs and controller).
  NiFiControl never installs cert-manager. If the cert-manager CRDs are absent, the
  `NiFiCluster` reports `TLSReady=False` with reason `CertManagerMissing` and does not
  create the StatefulSet — it does not crash or silently fall back to HTTP.
- The operator needs RBAC for `cert-manager.io` `certificates` and `issuers` (and read
  access to `clusterissuers`). The `nificontrol` Helm chart includes these rules.
- NiFi 2.10 requires HTTPS for any authenticated access; there is no authenticated HTTP.

## Four distinct security concepts

These are independent and easily confused:

1. **TLS server verification** — the operator (a client) verifies the NiFi server
   certificate against a CA. Configured for external clusters with `api.tls.caSecretKeyRef`
   and for managed clusters automatically from the issued CA.
2. **mTLS client authentication** — the operator presents its *own* client certificate so
   NiFi can authenticate and authorize it. This is how the operator authenticates to a
   managed internal-TLS cluster, and optionally to an external cluster via
   `api.auth.clientCertificate`.
3. **Bearer / JWT authentication** — a static bearer token (`api.auth.bearerTokenSecretKeyRef`)
   or a username/password pair exchanged for a JWT at `/nifi-api/access/token`
   (`api.auth.usernameSecretKeyRef` + `api.auth.passwordSecretKeyRef`). Used for external
   clusters only.
4. **Source credentials** — Git, OCI, and NiFi Registry flow sources carry their own
   `credentials` (username/password, token, CA). These authenticate to the *flow source*,
   not to NiFi, and are unrelated to the cluster API auth above.

**The three NiFi API authentication modes — mTLS client certificate, bearer token, and
username/password — are mutually exclusive.** Combined behaviour (for example presenting a
client certificate for transport while exchanging a token for identity) is rejected by
both CEL validation and the client builder until it is proven against live NiFi 2.10.

## Certificate providers

Enable internal TLS with `spec.internalTLS.enabled: true` and select exactly one provider.

### Self-signed (operator-managed CA)

The default when no other provider is set. The operator reconciles a **two-stage
self-signed chain**, never signing leaf certificates directly with a SelfSigned issuer:

```
SelfSigned Issuer  ──signs──▶  CA Certificate (isCA)  ──backs──▶  CA Issuer  ──signs──▶  leaf certs
```

```yaml
spec:
  internalTLS:
    enabled: true
    selfSigned:
      caDuration: 8760h
    certificate:
      duration: 2160h
      renewBefore: 360h
```

### Existing cert-manager Issuer or ClusterIssuer

```yaml
spec:
  internalTLS:
    enabled: true
    issuerRef:
      name: corporate-ca
      kind: ClusterIssuer      # or Issuer (namespaced)
      group: cert-manager.io
```

### Externally supplied PKCS12 Secrets

The operator generates no certificates; you supply them. Each referenced Secret must
contain `keystore.p12`, `truststore.p12`, PEM `tls.crt`, and PEM `tls.key`. `ca.crt` is
optional. When present NiFiControl uses it to pin trust; when absent the operator and
readiness probe use the system trust store. The identities must match the certificate
subject DNs exactly.

```yaml
spec:
  internalTLS:
    enabled: true
    external:
      serverSecretName: nifi-server-tls
      clientSecretName: nifi-operator-tls
      keystorePasswordSecretRef:
        name: nifi-keystore-password
        key: password
      initialAdminIdentity: "CN=nifi-operator,O=Example"
      nodeIdentity: "CN=nifi-node,O=Example"
```

## What the operator creates and owns

In self-signed / issuerRef modes, in the cluster namespace, all owned by the `NiFiCluster`
(so they are garbage-collected with it):

| Resource | Purpose |
| --- | --- |
| `Secret …-nifi-keystore-pw` | Randomly generated PKCS12 password (never a literal). |
| `Issuer …-nifi-selfsigned` | Self-signed root (self-signed mode only). |
| `Certificate …-nifi-ca` + `Issuer …-nifi-ca-issuer` | The CA chain (self-signed mode only). |
| `Certificate …-nifi-server` → Secret `…-nifi-server-tls` | Server/node identity, `serverAuth`+`clientAuth`, PKCS12 keystore/truststore. |
| `Certificate …-nifi-operator` → Secret `…-nifi-operator-tls` | Operator client identity, `clientAuth`, PKCS12 + PEM. |
| `ConfigMap …-nifi-tls-config` | Rendered `authorizers.xml` and the mTLS readiness probe script. |

cert-manager owns the issued Secrets and writes `keystore.p12`, `truststore.p12`, `ca.crt`,
`tls.crt`, and `tls.key` into them via its native PKCS12 keystore feature. The operator
does **not** convert PEM to PKCS12 in an init container — NiFi consumes the cert-manager
PKCS12 files directly.

The server pod mounts only the **server** Secret read-only; the operator loads the
**client** Secret for its REST client. The truststore trusts the shared CA, so NiFi
accepts the operator client certificate and inter-node connections.

### Generated identities

Identities are derived from predictable certificate common names:

- Operator client (NiFi **initial admin**): `CN=<cluster>-operator`
- Server/node (NiFi **node identity**): `CN=<cluster>-node`

Override the common names with `internalTLS.certificate.operatorCommonName` /
`nodeCommonName`. The `authorizers.xml` initial-admin and node-identity entries are kept
in sync with these. Without `spec.authentication`, `login-identity-providers.xml` is left
untouched — certificate authentication needs only the managed authorizer.

### User login on secured clusters

Internal TLS gives the operator (and any certificate holder) mutual-TLS access. To let
people log in with credentials — single-user, LDAP, or OIDC — configure
`spec.authentication` on top of internalTLS; see
[user-authentication.md](user-authentication.md).

### Canvas authorization bootstrap

NiFi's file access policy provider seeds the initial admin (the operator) with the
*global* policies — `/flow`, `/tenants`, `/policies`, `/controller` — but it does **not**
seed access to the **root process group** when no flow, and therefore no root group id,
exists at first-boot authorizer initialization. That is always the case for a freshly
provisioned managed cluster, so without intervention the operator authenticates but every
canvas reconcile fails with `HTTP 403: No applicable policies could be found`.

Because the initial admin does hold `/policies` and `/tenants` read/write, the operator
repairs this itself: once a secured managed cluster is reachable, and before it is marked
`Ready`, the operator resolves the concrete root group id (via `/flow/process-groups/root`,
authorized by `/flow` read) and grants its own identity read/write on
`/process-groups/{rootId}` and `/data/process-groups/{rootId}` — exactly the root-group
policies NiFi would have seeded had the flow existed. The step is idempotent, retried until
it succeeds (reported as `AuthorizationBootstrapPending`), and is a no-op on insecure
clusters (no managed authorizer) and external clusters (not the operator's to bootstrap).

### Shared node identity limitation

The cluster uses a **single shared server/node certificate** whose DNS SANs cover the
Service names and a wildcard for the per-pod headless addresses
(`*.<cluster>-nifi-headless.<ns>.svc`). All nodes therefore present the same identity,
and a single `Node Identity` authorizer entry (`CN=<cluster>-node`) covers the cluster.
This keeps the certificate stable as replicas scale, but it means individual nodes are not
distinguished from one another in NiFi's authorizations. Per-node identities are a future
enhancement.

## TLS readiness gating

The operator marks the cluster `TLSReady` only once the consumed Secrets actually contain
`keystore.p12`, `truststore.p12`, `tls.crt`, and `tls.key`. `ca.crt` is optional trust
material. While certificates are still being issued the condition is `TLSReady=False`
reason `TLSPending`, and the StatefulSet is not created. This prevents NiFi from starting
before the material it consumes exists.

## Services, probes, and endpoint

When TLS is enabled the managed Service and container expose the HTTPS port (default
`8443`), and the reported endpoint becomes `https://<cluster>-nifi.<ns>.svc:8443`. Because
NiFi requires a client certificate (`needClientAuth=true`), an ordinary `httpGet` probe
cannot pass — it cannot present a certificate. Startup and readiness therefore use an
**exec probe** that runs an mTLS request using the node's own certificate; liveness uses a
TCP check.

## Certificate rotation

cert-manager renews leaf certificates before expiry (`renewBefore`) and rewrites the issued
Secret in place. NiFiControl watches those Secrets and folds the keystore/truststore/CA
content into a checksum annotation on the pod template. When cert-manager rotates a
certificate, the checksum changes and the StatefulSet performs a normal rolling update so
each NiFi pod restarts with the new keystore. The operator also rebuilds its registered
mTLS REST client from the rotated client Secret.

### Failure and recovery behaviour

- **cert-manager not installed / issuer broken**: `TLSReady=False` (`CertManagerMissing`
  or `TLSPending`); no StatefulSet change. Existing running pods keep their current
  certificates. Install or fix cert-manager and reconciliation resumes.
- **Renewal in flight**: the old keystore stays mounted and valid until the new Secret is
  written; pods roll only after the new material is present, so there is no window with a
  missing keystore.
- **Rollout pauses mid-rotation**: because the rollout is a standard StatefulSet rolling
  update, a stuck pod halts the roll with older pods still serving on the previous (still
  valid) certificate. Resolve the pod issue and the roll continues.
- **Operator client lags a server rotation**: the new server certificate is signed by the
  same CA the operator already trusts, so the operator's existing client keeps verifying
  the server; the client is rebuilt from the rotated client Secret on the next reconcile.

## Migrating from development HTTP to managed HTTPS

Development mode (`internalTLS` absent or `enabled: false`) runs plaintext HTTP on `8080`
with no authentication — suitable only for controlled development. To migrate:

1. Install cert-manager and, if using `issuerRef`, create the Issuer/ClusterIssuer.
2. Add `internalTLS.enabled: true` with a provider to the `NiFiCluster`.
3. Apply. The operator provisions certificates, waits for `TLSReady`, then rolls the
   StatefulSet onto HTTPS `8443`. The endpoint changes from `http://…:8080` to
   `https://…:8443` and dependent resources reconcile against it automatically.

Because the web port, scheme, and authentication all change, this is a disruptive
transition for the managed cluster (pods restart and the API endpoint moves). Plan it like
any other NiFi restart. Persistent repositories on the data volume are preserved.

## Operator mTLS client for external clusters

External clusters can also authenticate the operator with a client certificate:

```yaml
spec:
  mode: External
  api:
    uri: https://nifi.example.com:8443
    tls:
      caSecretKeyRef: { name: nifi-api-tls, key: ca.crt }
      serverName: nifi.example.com
    auth:
      clientCertificate:
        secretName: nifi-operator-client
        certKey: tls.crt
        keyKey: tls.key
```

`insecureSkipVerify` remains available for external development clusters but is **never**
used for managed clusters.
