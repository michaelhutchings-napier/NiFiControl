# NiFiControl Helm Chart

Installs the NiFiControl operator and CRDs. It does not create a NiFi cluster.
Use the `nifi-cluster` chart or apply `NiFiCluster` resources separately.

## Install

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

## Common Values

| Value | Default | Purpose |
| --- | --- | --- |
| `image.repository` | `ghcr.io/michaelhutchings-napier/nificontrol` | Operator image. |
| `image.tag` | `latest` | Operator tag. |
| `replicaCount` | `1` | Controller replicas. |
| `leaderElection.enabled` | `true` | Required for multiple replicas. |
| `rbac.create` | `true` | Create ClusterRole/Binding. |
| `serviceAccount.create` | `true` | Create ServiceAccount. |
| `metrics.service.enabled` | `true` | Expose operator metrics. |
| `metrics.serviceMonitor.enabled` | `false` | Create ServiceMonitor. |

## CRDs

Helm installs CRDs on first install but does not upgrade them. After changing or
upgrading CRDs, apply them explicitly:

```bash
kubectl apply --server-side --force-conflicts -f charts/nificontrol/crds/
```

For development:

```bash
make manifests
make helm-sync-crds
make helm-verify
```
