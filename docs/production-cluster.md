# Production Cluster Hardening

Operator-managed (`mode: Internal`) NiFi clusters support production controls for
placement, availability, external exposure, and safe version upgrades.

```yaml
spec:
  mode: Internal
  replicas: 3
  scheduling:
    priorityClassName: high-priority
    topologySpreadConstraints: [...]
    tolerations: [...]
    nodeSelector: {...}
    affinity: {...}
  podDisruptionBudget:
    enabled: true
    maxUnavailable: 1        # or minAvailable
  ingress:
    enabled: true
    ingressClassName: nginx
    host: nifi.example.com
    path: /
    pathType: Prefix
    contextPath: ""          # set when NiFi is served under a sub-path
    tls:
      secretName: nifi-ingress-tls
  upgrade:
    strategy: RollingUpdate   # or OnDelete
    partition: 0              # staged/canary upgrades
    minReadySeconds: 30
```

## Scheduling

`spec.scheduling` threads `nodeSelector`, `tolerations`, `affinity`,
`topologySpreadConstraints`, and `priorityClassName` onto the managed pods so NiFi nodes
can be spread across failure domains and isolated onto dedicated capacity.

## PodDisruptionBudget

`spec.podDisruptionBudget.enabled` creates a `policy/v1` PodDisruptionBudget selecting the
NiFi nodes. Set exactly one of `maxUnavailable` or `minAvailable`; when neither is set the
operator defaults to `maxUnavailable: 1`. The PDB keeps quorum during voluntary
disruptions — node drains and the rolling pod restarts triggered by certificate rotation.

## Ingress and proxy host

`spec.ingress` creates a `networking/v1` Ingress routing `host` (and `path`) to the NiFi
Service, and — importantly — adds `host` to NiFi's `nifi.web.proxy.host` allow-list (and
sets `nifi.web.proxy.context.path` from `contextPath`). Without this NiFi rejects proxied
requests with an "invalid host header" error. Ingress TLS termination uses a Secret you
supply via `ingress.tls.secretName`; it is independent of `internalTLS` (operator-managed
pod-to-pod mTLS).

## Safe version upgrades

Change `spec.image` to upgrade NiFi. `spec.upgrade` controls the StatefulSet roll:

- `strategy: RollingUpdate` (default) replaces nodes one at a time; `OnDelete` waits for
  you to delete pods manually, for fully controlled upgrades.
- `partition` holds back nodes with an ordinal below the value, enabling a canary node
  before completing the roll.
- `minReadySeconds` requires each upgraded node to stay ready before the next is replaced.

During a roll the cluster reports `status` reason `Upgrading` (and is not `Ready`) until
every node is updated and ready, so dependent resources wait for a healthy cluster. With a
PodDisruptionBudget and multiple replicas the upgrade preserves availability.

Because all NiFi nodes in a cluster must run the same version and the flow is migrated on
startup, take a backup before a major upgrade (flow-configuration backup and persistent
repository snapshots are covered in a separate sub-milestone).

## Graceful scale-down

Reducing `spec.replicas` on a clustered cluster drains each removed node through NiFi's
cluster offload API before its pod is deleted, so queued FlowFiles are redistributed rather
than stranded on a removed node's volume. See [node-lifecycle.md](node-lifecycle.md).
