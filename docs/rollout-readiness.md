# Rollout Controls

`NiFiFlowDeployment` rollout settings work with every strategy.

## Readiness

```yaml
spec:
  rollout:
    readiness:
      waitForValid: true
      waitForControllerServices: true
      timeoutSeconds: 600
      maxUnavailable: 1
```

## Queue Drain

```yaml
spec:
  rollout:
    queuePolicy:
      action: Drain
      timeoutSeconds: 300
      onTimeout: Fail
```

`onTimeout` can be `Fail`, `Drop`, or `Proceed`.

## Retry and Cancel

```yaml
spec:
  rollout:
    retry:
      maxRetries: 3
    cancel:
      requested: false
```

Use cancel to abort an in-flight asynchronous NiFi request.
