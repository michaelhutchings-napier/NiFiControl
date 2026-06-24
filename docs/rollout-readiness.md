# Advanced Rollout Readiness and Controls

Beyond the rollout strategy (`ApplyOnly`, `StopAllThenApply`, `ChangedOnly`, `Rolling`,
`BlueGreen`), `NiFiFlowDeployment` supports strategy-agnostic controls that gate, drain,
cancel, and retry a rollout. They are configured under `spec.rollout` and are all
opt-in — omitting a block preserves the previous behaviour.

```yaml
spec:
  rollout:
    strategy: StopAllThenApply
    readiness:
      requireValidComponents: true
      requireEnabledControllerServices: true
      maxUnavailable: 0
      timeoutSeconds: 300
    queuePolicy:
      enabled: true
      timeoutSeconds: 60
      onTimeout: Fail      # Fail | Drop | Proceed
    retry:
      maxRetries: 2
    cancel: false
```

## Readiness gate

When `rollout.readiness` is set, a completed rollout is **not** marked in sync until the
deployed process group is healthy:

- `requireEnabledControllerServices` (default true) enables the deployed group's controller
  services before evaluating validity.
- `requireValidComponents` (default true) waits until the group reports no more than
  `maxUnavailable` invalid components.
- `timeoutSeconds` (default 300) bounds the wait; on expiry the rollout fails (honouring
  `retry`), and — if `rollback.enabled` — the previous successful flow is restored.

While waiting the deployment reports `syncState: AwaitingReadiness`. The gate applies to
in-place strategies; the `BlueGreen` strategy gates its candidate before switching traffic
and verifies validity after the switch.

## Queue drain policy

For `StopAllThenApply`, `rollout.queuePolicy.enabled` drains the deployed group's connection
queues — while components are still running — before the group is stopped for replacement,
so in-flight data is not discarded by the restart. If the queues do not empty within
`timeoutSeconds`, `onTimeout` decides:

- `Fail` (default) — abort the rollout.
- `Drop` — discard the remaining flow files and proceed.
- `Proceed` — continue regardless.

While draining the deployment reports `syncState: DrainingQueues`.

## Retry

`rollout.retry.maxRetries` re-attempts a failed rollout up to N times (with backoff)
before falling back to rollback (when enabled) or leaving the rollout failed. The attempt
count is reported as `status.activeRollout.retryCount` and resets when the target changes
or the rollout succeeds.

## Cancellation

Set `rollout.cancel: true` to abort an in-flight rollout: a `BlueGreen` rollout switches
traffic back to blue and removes the candidate, while an in-place rollout cancels the NiFi
replace request. The deployment reports `syncState: RolloutCancelled`. Clear the flag
(`cancel: false`) before starting a new rollout.
