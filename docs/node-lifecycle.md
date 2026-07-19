# Node Lifecycle

NiFiControl offloads nodes before scale-down so queued FlowFiles move to
remaining nodes instead of being stranded on a removed pod.

## Scale-Down Flow

1. Detect lower desired replica count.
2. Pick the highest ordinal node.
3. Disconnect and offload it through the NiFi cluster API.
4. Wait for offload completion or timeout.
5. Delete the pod/PVC only after the node is safe to remove.

## Options

```yaml
spec:
  nodeLifecycle:
    offloadOnScaleDown: true
    offloadTimeoutSeconds: 600
```

Disable offload only for disposable development clusters.

## Test

```bash
make integration-offload-kind
```
