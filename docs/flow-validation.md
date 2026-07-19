# Flow Validation

Set `spec.validateOnly: true` on a `NiFiFlowDeployment` to dry-run a snapshot
against NiFi. The operator imports the flow into a temporary validation group,
checks it, and removes the group without changing the live target.

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata:
  name: payments-check
spec:
  validateOnly: true
  clusterRef:
    name: production
  target:
    processGroupName: payments-check
  source:
    bundleRef:
      name: payments
```

## What It Checks

- Snapshot can be fetched and parsed.
- NiFi accepts the import.
- Components reach the requested validation state.
- Referenced parameter contexts and controller services can be resolved.

Use it in CI by applying the resource, waiting for `Ready=True`, then deleting it.
