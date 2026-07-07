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

`spec.additionalProxyHosts` adds extra `host[:port]` entries to the allow-list *on top of*
the operator-computed Service DNS names and the Ingress host — for an external load balancer
or a DNS name people reach NiFi through that the operator cannot infer:

```yaml
spec:
  additionalProxyHosts:
    - nifi.example.com
    - nifi.example.com:8443
```

Unlike a `configOverrides` override of `nifi.web.proxy.host` (which *replaces* the whole
allow-list), `additionalProxyHosts` is additive, so the operator's own Service names stay
trusted.

## Extra Services

`spec.externalServices` provisions additional Kubernetes Services in front of the node pods
beyond the operator's own ClusterIP and headless Services — for example a LoadBalancer for
the web UI, or a NodePort for site-to-site. Each Service selects the node pods with the
operator's own selector and is tracked (like the operator's Services) by annotation, so an
`Orphan` deletion leaves it behind and it is removed only under the `Delete` policy or when
dropped from the spec.

```yaml
spec:
  externalServices:
    - name: nifi-lb
      type: LoadBalancer
      annotations:
        external-dns.alpha.kubernetes.io/hostname: nifi.example.com
      ports:
        - name: https
          port: 8443
          targetPort: web        # named container port: web, cluster, s2s, load-balance
      loadBalancerSourceRanges: ["10.0.0.0/8"]
      externalTrafficPolicy: Local
```

`targetPort` accepts a named container port or a numeric port as a string. Remember to add
the load balancer's hostname to `additionalProxyHosts` so NiFi accepts requests arriving
through it.

## Custom ports

`spec.ports` overrides the network ports NiFi binds; unset fields keep NiFi's defaults. The
operator applies each to `nifi.properties`, the container ports, and the relevant Services.

```yaml
spec:
  ports:
    http: 8080             # plaintext web port (non-TLS mode; HTTPS is internalTLS.httpsPort)
    clusterProtocol: 11443 # node-to-node cluster protocol port
    remoteInput: 10000     # site-to-site raw socket port
    loadBalance: 6342      # cluster connection load-balance port
```

The HTTPS web port is configured through `internalTLS.httpsPort`, not here.

## Non-default cluster DNS domain

`spec.clusterDomain` sets the Kubernetes cluster DNS domain (default `cluster.local`) used
to build the fully-qualified Service names in the node TLS certificate SANs and the
operator-computed `nifi.web.proxy.host` allow-list. Set it on clusters configured with a
non-default domain so the generated certificate and allow-list carry the right FQDNs:

```yaml
spec:
  clusterDomain: cluster.internal
```

Intra-cluster clustering and mTLS use the short `<pod>.<svc>.<ns>.svc` names (resolved via
the pod's DNS search path), so they work regardless; `clusterDomain` matters when something
reaches a node by its full `.svc.<domain>` FQDN.

## Controller thread pool

`spec.maxTimerDrivenThreadCount` sets NiFi's controller-level maximum timer-driven thread
count — the pool that runs timer-driven processors. Unlike most settings, it is not a
`nifi.properties` value: it lives in the flow and is applied through the NiFi API
(`/nifi-api/controller/config`) once the cluster is reachable. The operator enforces it
declaratively — it reads the live value on each reconcile and resets it if it has drifted
(for example after a manual change in the UI), so the CR stays the source of truth.

```yaml
spec:
  maxTimerDrivenThreadCount: 25
```

Leave it unset to keep NiFi's default. Applying it needs the operator to reach the cluster
API; on a secured cluster that uses the operator's own mutual-TLS admin identity, and on an
external cluster it uses `spec.api.auth`. The cluster is not reported `Ready` until the
configured value has been applied.

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

## Repository storage

`spec.storage.repositories` places individual NiFi repositories on dedicated
PersistentVolumes, so the content repository can use bulk storage while the flowfile
repository stays on fast disk:

```yaml
spec:
  storage:
    enabled: true
    size: 10Gi               # conf, local state, and unlisted repositories
    storageClassName: fast
    repositories:
      content:    {size: 100Gi, storageClassName: bulk}
      provenance: {size: 50Gi, storageClassName: bulk}
      # flowfile and database stay on the main data volume
```

An unset `storageClassName` inherits the main volume's class. Because StatefulSet volume
claim templates are immutable, adding or removing a repository entry on an existing
cluster recreates the StatefulSet around the running pods and then rolls them one at a
time; the affected repository starts empty on its new volume — existing repository
contents are **not** migrated, so drain queues (or take a backup) before changing the
layout. NiFiNodeGroup pools inherit the cluster's layout unless the group overrides
`spec.storage`.

## Raw configuration overrides

For NiFi settings the API does not model directly — repository tuning, timeouts, custom
extension properties, extra JVM arguments — `spec.configOverrides` merges raw entries into
the generated node configuration:

```yaml
spec:
  configOverrides:
    nifiProperties:
      nifi.queue.swap.threshold: "40000"
      nifi.content.repository.archive.max.retention.period: "3 days"
    bootstrapProperties:
      java.arg.16: "-XX:+UseG1GC"
```

The entries are rendered into a per-cluster ConfigMap and applied on every node — the
primary pool and all NiFiNodeGroup pools — at startup, *after* the operator-managed
settings, so an override wins over the image's shipped default. Changing an override rolls
the nodes (a checksum annotation on the pod template triggers the StatefulSet update), and
removing one restores the running image's shipped default on the next roll; custom keys the
image does not ship are removed entirely.

Keys the operator itself manages are rejected at admission because a raw override would
sever the operator's own wiring: the web listener host/port, TLS keystore and truststore
settings, the sensitive properties key, cluster/ZooKeeper node settings, and the heap
arguments `java.arg.2`/`java.arg.3` (set `spec.jvm` instead). `nifi.web.proxy.host` *can*
be overridden — but the override replaces the operator-computed allow-list, so include the
in-cluster Service DNS names or the operator (and Ingress) will be rejected with an "invalid
host header" error. To *add* an external load balancer hostname without replacing the
allow-list, prefer the additive `spec.additionalProxyHosts` (see "Ingress and proxy host"
above).

`configOverrides.logbackXml` replaces `conf/logback.xml` wholesale for custom log levels,
appenders, or retention. The content is not validated — a malformed document surfaces as a
NiFi startup failure — and removing it restores the image's shipped `logback.xml` on the
next roll.

Values that must not appear in the NiFiCluster resource itself — an LDAP manager
password, a proxy credential — come from Secrets via `configOverrides.nifiPropertiesFrom`:

```yaml
spec:
  configOverrides:
    nifiPropertiesFrom:
      - name: nifi-extra-properties   # Secret data keys are property names
```

Secrets merge in list order and inline `nifiProperties` entries win. The payload the
nodes mount is itself a Secret, so Secret-sourced values never land in a ConfigMap. The
same property-name rules and operator-managed denylist apply, but because admission
cannot read Secret contents they are enforced at reconcile time: a violation puts the
cluster `Ready=False` with reason `ConfigOverridesInvalid` instead of rejecting the
update. Changing a referenced Secret's content rolls the nodes automatically.

## Pod customization

`spec.pod` customizes the generated node pods beyond the dedicated fields — most commonly
to mount custom NAR extensions or JDBC drivers, attach a log-shipping sidecar, or pull the
NiFi image from a private registry:

```yaml
spec:
  pod:
    labels: {team: data}
    annotations: {example.com/scrape: "true"}
    imagePullSecrets: [{name: regcred}]
    serviceAccountName: nifi-nodes
    securityContext:
      runAsUser: 1000
      runAsNonRoot: true
    extraVolumes:
      - name: nars
        persistentVolumeClaim: {claimName: custom-nars}
    extraVolumeMounts:
      - name: nars
        mountPath: /opt/nifi/nifi-current/nar_extensions
    extraInitContainers:
      - name: fetch-drivers
        image: curlimages/curl:8.7.1
        command: ["sh", "-c", "curl -fsSLo /drivers/postgresql.jar https://example.com/postgresql.jar"]
        volumeMounts: [{name: drivers, mountPath: /drivers}]
    extraContainers:
      - name: log-shipper
        image: fluent/fluent-bit:3.0
```

`spec.pod.securityContext` sets the pod-level security context (for example `runAsUser`,
`runAsGroup`, `runAsNonRoot`, `seccompProfile`). It replaces the operator default, except
that `fsGroup` falls back to `1000` — the `apache/nifi` image's uid/gid — when you leave it
unset, so mounted volumes stay writable even if you only override `runAsUser`.

Like `configOverrides`, `spec.pod` applies to the primary pool and all NiFiNodeGroup pools.
Operator-managed metadata wins on conflicts (selector labels and checksum annotations
cannot be overridden), extra containers and volumes are appended after the operator's own,
and the reserved volume names (`data`, `nificontrol-*`) and container names (`nifi`,
`initialize-data`) are rejected at admission.
