# Authorization

Use these only on secured NiFi clusters with a managed authorizer.

| Kind | Purpose |
| --- | --- |
| `NiFiUser` | Creates or adopts a NiFi user tenant by identity. |
| `NiFiUserGroup` | Creates a group tenant from `NiFiUser` members. |
| `NiFiPolicy` | Grants `read` or `write` on a NiFi resource path. |

`NiFiPolicy` owns only the grants it declares. It preserves other tenants already
present on the same NiFi policy, including NiFi's seeded initial-admin grant.

## Example

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata:
  name: prometheus
spec:
  clusterRef:
    name: secure
  identity: "CN=prometheus, O=monitoring"
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata:
  name: prometheus-read-flow
spec:
  clusterRef:
    name: secure
  resource: /flow
  action: read
  userRefs:
    - name: prometheus
```

Common policy resources:

| Resource | Use |
| --- | --- |
| `/flow` | Metrics, flow overview, root access. |
| `/controller` | Controller-level access. |
| `/process-groups/{id}` | Process group access. |
| `/data/process-groups/{id}` | Site-to-site data access. |

## Delete

With `deletionPolicy: Delete`, deleting a `NiFiPolicy` removes only that CR's
recorded grants. If the NiFi policy becomes empty, the operator deletes it.
