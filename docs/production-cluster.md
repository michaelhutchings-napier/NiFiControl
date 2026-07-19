# Production Cluster

Use this page as the short checklist for hardening a managed `NiFiCluster`.

## Baseline

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata:
  name: production
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 3
  internalTLS:
    enabled: true
    issuerRef:
      name: nifi-ca
      kind: ClusterIssuer
  storage:
    enabled: true
  resources:
    requests:
      cpu: "1"
      memory: 4Gi
```

## Key Fields

| Area | Fields |
| --- | --- |
| Scheduling | `pod.affinity`, `pod.tolerations`, `pod.topologySpreadConstraints`, `pod.priorityClassName` |
| Availability | `podDisruptionBudget`, `rolloutStrategy`, `nodeLifecycle` |
| Network | `service`, `externalServices`, `ingress`, `proxyHosts` |
| Storage | `storage.repositories`, `storage.content`, `storage.database`, `storage.flowfile`, `storage.provenance` |
| Config | `configOverrides`, `nifiPropertiesFrom`, `bootstrapPropertiesFrom` |
| Logs | `logging.level`, `logging.loggers`, `logging.retention` |
| Pods | `pod.metadata`, `pod.serviceAccountName`, `pod.sidecars`, `pod.initContainers`, `pod.volumes` |

## Coordination

- Single-node clusters need no external coordination.
- Multi-node clusters can use ZooKeeper or Kubernetes coordination, depending on
  your `spec.coordination` settings.

## Overrides

Use typed fields first. Use `configOverrides` for NiFi settings the API does not
model yet. Operator-owned keys are rejected at admission.

Use Secret-backed refs for sensitive values:

```yaml
spec:
  nifiPropertiesFrom:
    - secretKeyRef:
        name: ldap-manager-password
        key: value
      property: nifi.security.user.login.identity.provider
```

## OpenShift

Set pod security context and service account fields through `spec.pod` to match
your SCC. Keep random UIDs and writable mounted volumes in mind.
