# BlueGreen Rollouts

`NiFiFlowDeployment` supports `rollout.strategy: BlueGreen` for safer flow
updates. The operator imports the new snapshot into a candidate process group,
waits for readiness, switches boundary connections, then retires the old group.

## Example

```yaml
spec:
  rollout:
    strategy: BlueGreen
    readiness:
      waitForValid: true
      timeoutSeconds: 600
    queuePolicy:
      action: Drain
      timeoutSeconds: 300
```

## Requirements

- The target flow must expose stable input/output ports.
- Candidate ports must match the live ports by name.
- Boundary connections are recreated because NiFi does not support changing a
  connection endpoint in place.
- Failed switching rolls back to the previous live group when possible.

Use `StopAllThenApply` for simpler flows that do not need dual process groups.
