# User Authentication

`spec.authentication` configures how people log in to a secured, operator-managed
cluster. It requires `internalTLS` (NiFi only allows user authentication over HTTPS) and
supports three modes: `SingleUser`, `LDAP`, and `OIDC`. In every mode the operator keeps
talking to NiFi over mutual TLS — NiFi authenticates client certificates before any login
provider — so the operator, site-to-site peers, and NiFiUser certificate identities are
unaffected by the mode you pick. With a login mode configured, the operator relaxes
NiFi's TLS listener from *needing* client certificates to *wanting* them
(`nifi.security.needClientAuth=false`), because mandatory client certificates would
reject browser logins at the TLS handshake.

Authorization stays with the operator-managed file-based authorizer in all modes, so
identities that log in through the provider are managed the same way as certificate
identities: seed administrators with `adminIdentities`, or manage fine-grained access
declaratively with `NiFiUser`, `NiFiUserGroup`, and `NiFiPolicy` resources (see
[authorization.md](authorization.md)).

## Admin identities

```yaml
spec:
  authentication:
    mode: LDAP
    adminIdentities: [alice, bob]
```

Each listed identity is created as a NiFi user (if missing) and granted the
administrative policy set once the cluster is reachable: read on `/flow`, read/write on
`/controller`, `/tenants`, and `/policies`, read on `/system`, `/counters`, and
`/provenance`, write on `/restricted-components`, and read/write on the root process
group and its data. Identities must match what the provider yields — the single-user
username, the LDAP identity (per `identityStrategy`), or the OIDC claim value. Removing
an identity from the list does not revoke previously granted policies; use NiFiPolicy
resources when you need managed revocation.

## Single user

```yaml
spec:
  authentication:
    mode: SingleUser
    singleUser:
      credentialsSecretRef: {name: admin-creds}   # keys: username, password (12+ chars)
    adminIdentities: [admin]
```

The node applies the credentials with `nifi.sh set-single-user-credentials` at startup,
so NiFi hashes the password itself and the plaintext never lands in configuration files.
Unlike stock NiFi's single-user mode, authorization still goes through the managed
authorizer — list the username in `adminIdentities` (or grant it policies) for it to see
anything. Rotating the Secret's content rolls the nodes automatically.

## LDAP

```yaml
spec:
  authentication:
    mode: LDAP
    ldap:
      url: ldap://openldap.auth.svc:389
      authenticationStrategy: SIMPLE      # SIMPLE | LDAPS | START_TLS
      managerDN: cn=admin,dc=example,dc=org
      managerPasswordSecretRef: {name: ldap-manager, key: password}
      userSearchBase: ou=users,dc=example,dc=org
      userSearchFilter: (uid={0})
      identityStrategy: USE_USERNAME      # or USE_DN
      caSecretRef: {name: ldap-ca, key: ca.crt}   # optional; for LDAPS/START_TLS with a private CA
    adminIdentities: [alice]
```

The operator renders `login-identity-providers.xml` (manager password included) into a
per-cluster Secret that the nodes mount and copy into place at startup. `LDAPS` and
`START_TLS` trust the JDK trust store by default. To trust a directory whose certificate
is signed by a **private CA**, set `caSecretRef` to a Secret holding the PEM CA bundle
(default key `ca.crt`): the operator validates the PEM, ships it to the nodes, and builds
it into a PKCS12 truststore that the LDAP provider references — no custom image needed.
Group synchronization (`ldap-user-group-provider`) is not yet wired — manage groups with
`NiFiUserGroup` resources instead.

## Client certificates (mTLS)

On any secured (`internalTLS`) cluster, clients can authenticate with an X.509 **client
certificate** — NiFi validates the certificate at the TLS handshake, before any login provider, so
this works in every `spec.authentication` mode and needs no login provider at all. It is the right
fit for service accounts and automation (the operator itself authenticates this way), and can run
alongside `LDAP`/`OIDC` logins for humans on the same cluster.

Model each certificate identity as a `NiFiUser` whose `identity` is the certificate's subject DN.
The operator can issue the certificate for you via cert-manager:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata:
  name: ingest-bot
spec:
  clusterRef: {name: secure}
  identity: "CN=ingest-bot, O=dataflows"   # the cert subject DN becomes the NiFi identity
  certificate:
    create: true                           # operator issues a client cert via cert-manager…
    secretName: ingest-bot-tls             # …into this Secret (tls.crt / tls.key / ca.crt)
```

Mount that Secret into the client and present the cert on each request, for example:

```bash
curl --cert tls.crt --key tls.key --cacert ca.crt https://<host>/nifi-api/flow/about
```

Authorize the identity like any other — list it in `adminIdentities`, or grant scoped access with
`NiFiPolicy` (see [authorization.md](authorization.md)). To use a certificate signed **outside** the
operator, omit `certificate.create` and set `identity` to that certificate's DN. A runnable example
that pairs LDAP logins with an mTLS service account is
[examples/ldap-auth.yaml](../examples/ldap-auth.yaml).

Because a login provider relaxes NiFi's TLS listener from *needing* to *wanting* client certificates,
certificate and login-based authentication coexist without extra configuration.

## OIDC

```yaml
spec:
  authentication:
    mode: OIDC
    oidc:
      discoveryURL: https://idp.example.com/.well-known/openid-configuration
      clientID: nifi
      clientSecretRef: {name: oidc-client, key: secret}
      claim: email                        # the claim that becomes the NiFi identity
      additionalScopes: [groups]
      caSecretRef: {name: oidc-ca, key: ca.crt}   # optional; provider served by a private CA
    adminIdentities: [alice@example.com]
```

OIDC is configured purely through `nifi.properties`; the client secret reaches the node
as an environment variable from the referenced Secret. Register the cluster's callback
URL (`https://<host>/nifi-api/access/oidc/callback`) with the identity provider, and make
sure the host people use (Ingress host or load balancer) is in NiFi's proxy allow-list —
the operator adds the Service DNS names and the Ingress host automatically.

If the provider's HTTPS endpoint is served by a **private CA** (so NiFi can't fetch the
discovery document against the JDK trust store), set `caSecretRef` to a Secret holding the
PEM CA bundle (default key `ca.crt`). NiFi's OIDC discovery has no custom-truststore path,
so the operator adds the CA to a writable copy of the node's own server truststore and
switches `nifi.security.user.oidc.truststore.strategy` to `NIFI` — NiFi then trusts both
the internal mTLS CA and your provider's CA.

OIDC is exercised end to end in the kind E2E suite against a real
[dex](https://dexidp.io) identity provider. `integration-oidc-kind` asserts the operator
wires OIDC into `nifi.properties`, that NiFi builds its OIDC client from dex's discovery
document, and that NiFi issues a spec-correct authorization-code request to dex (correct
`client_id`, callback `redirect_uri`, `scope`, and PKCE). `integration-oidc-tls-kind`
additionally runs dex over HTTPS behind a cert-manager private CA and proves the
`caSecretRef` path: NiFi fetches the HTTPS discovery document trusting only the supplied
CA and completes the same handshake. The interactive browser login itself (entering
credentials at the provider) is not scripted — validate that against your own provider in
staging.

## Credential rotation and rollouts

The pod template carries a checksum over the mode and every referenced Secret's content
(`nifi.controlnifi.io/auth-checksum`), so changing the mode, its settings, or rotating any
referenced Secret rolls the nodes one at a time. NiFiNodeGroup pools inherit the
cluster's authentication and roll with it.
