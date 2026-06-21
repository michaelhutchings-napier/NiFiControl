# NiFiControl Helm Chart

This chart installs the NiFiControl Kubernetes operator and its CRDs. It does
not install an Apache NiFi cluster; `NiFiCluster` resources point the operator
at NiFi API endpoints that already exist.

## Install

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

Check the controller:

```bash
kubectl rollout status deployment/nificontrol -n nificontrol-system
kubectl get crd | grep nifi.controlnifi.io
```

## Configuration

| Value | Default | Description |
| --- | --- | --- |
| `replicaCount` | `1` | Number of controller-manager replicas. |
| `image.repository` | `ghcr.io/michaelhutchings-napier/nificontrol` | Operator image repository. |
| `image.tag` | `latest` | Operator image tag. |
| `image.pullPolicy` | `IfNotPresent` | Kubernetes image pull policy. |
| `serviceAccount.create` | `true` | Create the operator ServiceAccount. |
| `serviceAccount.name` | `""` | Existing ServiceAccount name when creation is disabled. |
| `rbac.create` | `true` | Create the ClusterRole and ClusterRoleBinding. |
| `leaderElection.enabled` | `true` | Enable controller-runtime leader election. |
| `metrics.bindAddress` | `:8080` | Controller metrics bind address. |
| `metrics.port` | `8080` | Metrics container port. Keep it aligned with the bind address. |
| `metrics.service.enabled` | `true` | Create a metrics Service. |
| `healthProbe.bindAddress` | `:8081` | Health probe bind address. |
| `healthProbe.port` | `8081` | Health probe container port. |
| `resources` | See `values.yaml` | Controller resource requests and limits. |
| `podDisruptionBudget.enabled` | `false` | Create a PodDisruptionBudget. |
| `extraArgs` | `[]` | Additional manager command-line arguments. |
| `extraEnv` | `[]` | Additional manager environment variables. |

The chart also supports standard scheduling and pod customization values:
`imagePullSecrets`, `podAnnotations`, `podLabels`, `nodeSelector`, `tolerations`,
`affinity`, `topologySpreadConstraints`, `priorityClassName`,
`podSecurityContext`, and `securityContext`.

## CRD Lifecycle

The generated CRDs are packaged in the chart's `crds/` directory. Helm installs
them before the controller, but Helm does not upgrade or delete CRDs during a
normal chart upgrade or uninstall. After changing an API type, regenerate and
sync them with:

```bash
make manifests
make helm-sync-crds
```

For an existing installation, apply updated CRDs explicitly before upgrading:

```bash
kubectl apply -f charts/nificontrol/crds/
helm upgrade nificontrol ./charts/nificontrol -n nificontrol-system
```

## Existing RBAC

To use pre-created RBAC and a ServiceAccount:

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --set rbac.create=false \
  --set serviceAccount.create=false \
  --set serviceAccount.name=nificontrol-controller-manager
```
