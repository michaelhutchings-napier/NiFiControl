# Node Groups

`NiFiNodeGroup` adds an independently sized node pool to an existing managed
`NiFiCluster`. Use it for different CPU/memory/storage profiles inside one NiFi
cluster.

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiNodeGroup
metadata:
  name: workers
spec:
  clusterRef:
    name: production
  replicas: 3
  resources:
    requests:
      cpu: "1"
      memory: 4Gi
```

## Notes

- Each group has its own StatefulSet and scale subresource.
- HPA, KEDA, or `NiFiAutoscaler` can target a group directly.
- Scale-down uses graceful offload.
- Cluster-level pod settings are inherited unless the group overrides them.
- `deletionPolicy: Delete` removes the group's Kubernetes resources; `Orphan`
  leaves them.
