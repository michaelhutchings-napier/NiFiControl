# Transactional BlueGreen Rollouts

`NiFiFlowDeployment` supports a transactional BlueGreen rollout strategy. Unlike the
in-place strategies (`ApplyOnly`, `StopAllThenApply`, `ChangedOnly`, `Rolling`), which
replace the live process group's contents, BlueGreen deploys the new version as a separate
**candidate (green)** process group, validates it, and then switches the **external
boundary connections** from the live **(blue)** process group's ports to green's matching
ports before retiring blue. NiFiControl never approximates BlueGreen by simply creating and
deleting process groups — the connection switch is real and recoverable.

```yaml
spec:
  rollout:
    strategy: BlueGreen
    blueGreen:
      drainTimeoutSeconds: 120
      onDrainTimeout: Fail        # or Drop
      readinessTimeoutSeconds: 300
      requireEnabledControllerServices: true
```

## Why connection switching is delete-and-recreate

A NiFi connection's source and destination are immutable; they cannot be re-pointed with
an update. Switching traffic therefore means deleting the boundary connection and creating
a new one to the candidate's matching port. A connection can only be deleted once its
source is stopped and its queue is empty. Each switch is: stop the producing side → drain
(or, on timeout, drop) the queue → delete the old connection → create the new connection to
the green port. The full definition of every boundary connection is recorded first so
traffic can be switched **back** to blue if anything fails.

## Lifecycle

1. **DeployingCandidate** — the new snapshot is imported beside blue as `<name>-candidate`.
2. **AwaitingReadiness** — the candidate's controller services are enabled (when
   `requireEnabledControllerServices` is true) and the operator waits until the candidate
   reports zero invalid components, bounded by `readinessTimeoutSeconds`. The candidate is
   then started so its ports are active.
3. **SwitchingTraffic** — the operator inventories every connection in the parent group
   that crosses blue's edge, matching each to a blue input or output port by **name**.
   Each boundary connection is then switched to the green port with the same name, one at a
   time so each step is durable.
4. **Promoting** — the candidate is renamed to the target name and adopted as the live
   deployment; external sources stopped during the switch are restarted; the rollout is
   marked complete.
5. **Blue retirement** — blue is kept (stopped) for one reconcile as a recovery margin,
   then deleted.

If a switch fails (a queue will not drain under the `Fail` policy, a matching green port is
missing, or a NiFi call errors), the rollout enters **RollingBackTraffic**: already-switched
connections are restored to blue from the recorded inventory, the candidate is deleted, and
the deployment is marked failed. As with in-place rollbacks, the failed target is not
retried until the source or generation changes.

## Requirements and matching rules

- **Port names must match** between the live flow and the new version. Boundary connections
  are re-pointed by input/output port name; a candidate missing a matching port aborts and
  rolls back.
- **Inbound boundary sources must be stoppable** — a processor, input port, or output port.
  A boundary connection feeding the deployment from a funnel (which cannot be stopped)
  causes the rollout to fail clearly rather than risk data loss. Outbound destinations may
  be any component, including funnels.
- cert-manager and other dependencies are unrelated to BlueGreen; it operates purely
  through the NiFi REST API.

## Queue handling

`drainTimeoutSeconds` bounds how long the operator waits for each boundary queue to empty
after stopping its source. `onDrainTimeout` selects what happens if it does not:

- `Fail` (default) — abort and switch traffic back to blue. No in-flight data is lost.
- `Drop` — issue a NiFi drop request to discard the remaining flow files, then proceed.

## Limitations

- **Shared boundary, not per-flowfile** — the cutover is per boundary connection. Data
  already inside blue's internal queues when it is retired is not migrated to green; this is
  inherent to a blue/green cutover. Drain boundary queues (the default) to avoid losing data
  crossing the edge.
- **Single switch granularity** — each boundary connection is switched independently and
  durably, but the set of switches is not a single atomic NiFi transaction. The recorded
  inventory makes the switch *recoverable*: a mid-switch failure restores blue.
- **First deployment** — when there is no existing (blue) process group, BlueGreen performs
  an ordinary import; there is nothing to switch.
