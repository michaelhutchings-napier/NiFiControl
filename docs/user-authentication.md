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
    adminIdentities: [alice]
```

The operator renders `login-identity-providers.xml` (manager password included) into a
per-cluster Secret that the nodes mount and copy into place at startup. `LDAPS` and
`START_TLS` trust the JDK trust store; directories signed by a private CA are not yet
supported. Group synchronization (`ldap-user-group-provider`) is not yet wired — manage
groups with `NiFiUserGroup` resources instead.

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
    adminIdentities: [alice@example.com]
```

OIDC is configured purely through `nifi.properties`; the client secret reaches the node
as an environment variable from the referenced Secret. Register the cluster's callback
URL (`https://<host>/nifi-api/access/oidc/callback`) with the identity provider, and make
sure the host people use (Ingress host or load balancer) is in NiFi's proxy allow-list —
the operator adds the Service DNS names and the Ingress host automatically. OIDC is
covered by rendering/unit tests but is not exercised in the kind E2E suite (it needs a
running identity provider); validate your provider integration in a staging environment.

## Credential rotation and rollouts

The pod template carries a checksum over the mode and every referenced Secret's content
(`nifi.controlnifi.io/auth-checksum`), so changing the mode, its settings, or rotating any
referenced Secret rolls the nodes one at a time. NiFiNodeGroup pools inherit the
cluster's authentication and roll with it.
