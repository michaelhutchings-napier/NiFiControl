# User Authentication

NiFiControl configures user login for secured managed clusters. The operator
itself always talks to NiFi with mTLS.

## Admins

Seed admin access with `adminIdentities`:

```yaml
spec:
  authentication:
    adminIdentities:
      - "CN=admin, O=platform"
```

Use `NiFiUser`, `NiFiUserGroup`, and `NiFiPolicy` for ongoing authorization.

## Single User

```yaml
spec:
  authentication:
    singleUser:
      usernameSecretKeyRef:
        name: nifi-single-user
        key: username
      passwordSecretKeyRef:
        name: nifi-single-user
        key: password
```

## LDAP

```yaml
spec:
  authentication:
    ldap:
      url: ldaps://ldap.example.com:636
      userSearchBase: ou=people,dc=example,dc=com
      managerDn: cn=nifi,ou=svc,dc=example,dc=com
      managerPasswordSecretKeyRef:
        name: ldap-bind
        key: password
```

## Client Certificates

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata:
  name: alice
spec:
  clusterRef:
    name: secure
  identity: "CN=alice, O=platform"
```

Grant access with `NiFiPolicy`.

## OIDC

Configure OIDC under `spec.authentication.oidc` with issuer URL, client id, and
client secret refs. Secret changes roll the NiFi pods.
