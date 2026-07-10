# Migrating from NiFiKop

This guide maps [NiFiKop](https://github.com/konpyutaika/nifikop) (`nifi.konpyutaika.com`)
Custom Resources to their NiFiControl (`nifi.controlnifi.io`) equivalents. It is a *manual*
guide: it tells you what each NiFiKop kind becomes, what maps cleanly, and — just as important —
what cannot be translated by any manifest tool and must be done as part of an Apache NiFi
upgrade.

## Read this first: migrating NiFiKop means migrating NiFi 1.x → 2.x

NiFiKop's `nifi-cluster` chart defaults to **Apache NiFi 1.28** (a 1.x line). **NiFiControl runs
Apache NiFi 2.x only.** So a NiFiKop → NiFiControl move is, underneath, a NiFi **major-version**
upgrade, and it has two layers that differ enormously in difficulty:

- **Control plane (translatable).** Cluster topology, TLS, tenancy (users/groups/policies),
  parameter contexts, and registry clients are configuration. This guide translates them.
- **Flows (not translatable by manifests).** Your actual dataflows are NiFi 1.x artifacts.
  NiFi 2.x is a breaking release — event-driven scheduling removed, many components dropped or
  renamed, Java 21, and the flow-registry model changed. **No CRD-conversion tool can migrate a
  1.x flow to 2.x**; that is Apache NiFi's own upgrade path, and it is the real work.

The pragmatic posture that works: **stand up the NiFiControl cluster and its tenancy / parameter
contexts / registry clients from translated manifests, then re-establish your flows on 2.x**
(re-version them in a 2.x-compatible registry) rather than expecting them to carry across
automatically. Treat everything below as producing a reviewed *draft*, not a drop-in.

## Naming and coexistence

NiFiKop kinds are `Nifi*` in group `nifi.konpyutaika.com`; NiFiControl kinds are `NiFi*` (capital
F) in group `nifi.controlnifi.io`. Because the groups differ, **both operators can run
side-by-side** (in separate namespaces) during a cutover with no CRD or controller conflict —
migrate one cluster at a time. Where a NiFiControl resource should attach to NiFi objects that
already exist, set `adoptionPolicy.mode` (`AdoptByName` / `AdoptById`) instead of `Never`.

## Kind mapping at a glance

| NiFiKop (`nifi.konpyutaika.com`) | NiFiControl (`nifi.controlnifi.io`) | Effort |
| --- | --- | --- |
| `NifiParameterContext` | `NiFiParameterContext` | **Clean** |
| `NifiRegistryClient` (v2alpha1) | `NiFiRegistryClient` | **Clean** |
| `NifiUser` | `NiFiUser` **+** `NiFiPolicy`(s) | **Mostly clean** (inline policies explode into separate resources) |
| `NifiUserGroup` | `NiFiUserGroup` **+** `NiFiPolicy`(s) | **Mostly clean** |
| `NifiCluster` | `NiFiCluster` **+** `NiFiNodeGroup`(s) | **Lossy — review required** (node-list → StatefulSet) |
| `NifiNodeGroupAutoscaler` | `NiFiAutoscaler` (+ `NiFiNodeGroup`) | **Partial** (some scale-down strategies unsupported) |
| `NifiConnection` | `NiFiConnection` | Flow layer (see flows caveat) |
| `NifiDataflow` | `NiFiFlowDeployment` / `NiFiFlowBundle` | **Not auto-migratable** (1.x flow content + registry model) |
| `NifiResource` (free-form) | the specific canvas CRD | **Manual** (no generic equivalent) |

## Parameter contexts — clean

`sensitive: true` parameters become `sensitiveValueFrom` (a Secret reference — NiFiControl never
inlines secret values). `inheritedParameterContexts` → `inheritedRefs`. `disableTakeOver: false`
→ `adoptionPolicy.mode: AdoptByName`.

```yaml
# NiFiKop
apiVersion: nifi.konpyutaika.com/v1
kind: NifiParameterContext
metadata: {name: payments}
spec:
  clusterRef: {name: prod, namespace: nifi}
  parameters:
    - {name: batchSize, value: "500"}
    - {name: apiKey, value: "", sensitive: true}
  inheritedParameterContexts: [{name: common}]
  disableTakeOver: false
---
# NiFiControl
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterContext
metadata: {name: payments}
spec:
  clusterRef: {name: prod, namespace: nifi}
  parameters:
    - {name: batchSize, value: "500"}
    - name: apiKey
      sensitiveValueFrom:                        # value lives in a Secret, never inlined
        secretKeyRef: {name: payments-params, key: apiKey}
  inheritedRefs: [{name: common}]
  adoptionPolicy: {mode: AdoptByName}      # was disableTakeOver: false
```

## Registry clients — clean

NiFiKop's v2alpha1 `type: registry|github|gitlab` maps to NiFiControl's
`type: NiFiRegistry|GitHub|GitLab`. A v1 `NifiRegistryClient` (bare `uri`) becomes
`type: NiFiRegistry` with `uri`.

```yaml
# NiFiKop (v2alpha1)
apiVersion: nifi.konpyutaika.com/v2alpha1
kind: NifiRegistryClient
metadata: {name: flows}
spec:
  clusterRef: {name: prod, namespace: nifi}
  type: github
  githubConfig: {repositoryOwner: acme, repositoryName: flows, branch: main}
---
# NiFiControl
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRegistryClient
metadata: {name: flows}
spec:
  clusterRef: {name: prod, namespace: nifi}
  type: GitHub
  github: {repositoryOwner: acme, repositoryName: flows, defaultBranch: main}
```

## Users and groups — mostly clean (policies explode)

NiFiKop inlines `accessPolicies` on the user/group. NiFiControl models authorization as
**separate `NiFiPolicy` resources** (`resource` + `action: read|write` + `userRefs`/
`userGroupRefs`), so each inline policy becomes its own resource. `createCert` + `secretName` →
`certificate: {create, secretName}`.

```yaml
# NiFiKop
apiVersion: nifi.konpyutaika.com/v1
kind: NifiUser
metadata: {name: analyst}
spec:
  clusterRef: {name: prod, namespace: nifi}
  identity: "CN=analyst"
  createCert: true
  secretName: analyst-tls
  accessPolicies:
    - {type: global, action: read, resource: /flow}
---
# NiFiControl
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata: {name: analyst}
spec:
  clusterRef: {name: prod, namespace: nifi}
  identity: "CN=analyst"
  certificate: {create: true, secretName: analyst-tls}
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy                 # each inline accessPolicy becomes a NiFiPolicy
metadata: {name: analyst-read-flow}
spec:
  clusterRef: {name: prod, namespace: nifi}
  resource: /flow
  action: read
  userRefs: [{name: analyst}]
```

`NifiUserGroup` maps the same way: `usersRef` → `users: [{userRef: {name: ...}}]`, and its
`accessPolicies` become `NiFiPolicy` resources whose `userGroupRefs` name the group.

## Cluster — lossy, review required

This is the one that is not a field rename. **NiFiKop models nodes as an explicit `nodes []`
list** (each with its own id, config group, and per-node overrides) and manages individual pods;
**NiFiControl runs a StatefulSet driven by `replicas`, with extra pools modeled as
`NiFiNodeGroup`.** A homogeneous NiFiKop cluster collapses to `replicas: <len(nodes)>`; distinct
`nodeConfigGroups` become `NiFiNodeGroup`s. Field mapping:

| NiFiKop `NifiCluster.spec` | NiFiControl | Notes |
| --- | --- | --- |
| `nodes[]` / `nodeConfigGroups` | `replicas` (+ `NiFiNodeGroup` per extra group) | Per-node identity/config does not survive; homogenize |
| `readOnlyConfig.nifiProperties` (overrideConfigs) | `configOverrides.nifiProperties` (map) | `overrideConfigMap`/`overrideSecretConfig` → `configOverrides.nifiPropertiesFrom` |
| `readOnlyConfig.logbackConfig` | `spec.logging` **or** `configOverrides.logbackXml` | The two are mutually exclusive |
| `listenersConfig.sslSecrets` / `pkiBackend` | `internalTLS` (cert-manager) | See [internal-tls.md](internal-tls.md) |
| `clusterManager` + `zkAddress` / `zkPath` | `coordination` (`mode: ZooKeeper` + `zookeeperConnectString`, or `mode: Kubernetes`) | Kubernetes coordination needs no ZooKeeper |
| `nodeConfig.storageConfigs` | `storage` | |
| `nodeConfig.resourcesRequirements` | `resources` | |
| `readOnlyConfig.bootstrapProperties` (heap) | `jvm` (`heapInitial`/`heapMax`) | |
| `ldapConfiguration` / `singleUserConfiguration` | `authentication` | See [user-authentication.md](user-authentication.md) |
| `managedAdminUsers` / `managedReaderUsers` | `NiFiUser` + `NiFiPolicy` | |
| `readOnlyConfig.maximumEventDrivenThreadCount` | — | **Dropped**: event-driven scheduling was removed in NiFi 2.x |
| `type: external` refs (`nifiURI`, `secretRef`, …) | `mode: External` + `spec.api` | For driving an existing NiFi over its REST API |

```yaml
# NiFiKop (abridged)
apiVersion: nifi.konpyutaika.com/v1
kind: NifiCluster
metadata: {name: prod}
spec:
  clusterManager: zookeeper
  zkAddress: zk.nifi.svc:2181
  nodes: [{id: 1, nodeConfigGroup: default}, {id: 2, nodeConfigGroup: default}]
  nodeConfigGroups:
    default:
      storageConfigs: [{mountPath: /data, pvcSpec: {resources: {requests: {storage: 20Gi}}}]
      resourcesRequirements: {requests: {cpu: "1", memory: 4Gi}}
  readOnlyConfig:
    nifiProperties: {overrideConfigs: "nifi.ui.banner.text=PROD"}
  listenersConfig: {sslSecrets: {tlsSecretName: prod-tls, create: true}}
---
# NiFiControl (reviewed draft)
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata: {name: prod}
spec:
  mode: Internal
  replicas: 2                                  # was len(nodes)
  coordination: {mode: ZooKeeper, zookeeperConnectString: zk.nifi.svc:2181}
  storage: {enabled: true, size: 20Gi}
  resources: {requests: {cpu: "1", memory: 4Gi}}
  internalTLS: {enabled: true, selfSigned: {}} # was sslSecrets
  configOverrides:
    nifiProperties:
      nifi.ui.banner.text: {value: "PROD"}
```

## Autoscaler — partial

`NifiNodeGroupAutoscaler` → a `NiFiAutoscaler` targeting the `NiFiCluster` or a `NiFiNodeGroup`.
`replicas`/scale subresource → `minReplicas`/`maxReplicas`. `downscaleStrategy: nonprimary` →
`behavior.scaleDownStrategy: NonPrimary`; **`lifo` and `leastbusy` are not supported** — NiFiControl
always offloads highest-ordinal-first (StatefulSet semantics; see [autoscaling.md](autoscaling.md)
and [node-lifecycle.md](node-lifecycle.md)). NiFiKop's autoscaler is replica-count driven; a
`NiFiAutoscaler` also lets you scale on Prometheus/queue-depth metrics.

## Flows — not auto-migratable

`NifiDataflow` references a flow by NiFi-Registry **`bucketId` / `flowId` / `flowVersion`** — the
1.x bucket model, holding 1.x flow content. Git-based 2.x flow registries have no buckets, and the
flow itself needs the 1.x → 2.x upgrade. There is no faithful automatic translation. The path:

1. Bring the flow onto Apache NiFi 2.x (open it in a 2.x instance, resolve removed/renamed
   components, replace any Variable Registry usage with Parameter Contexts).
2. Version it in a 2.x-compatible registry, referenced by a `NiFiRegistryClient`.
3. Deploy it with `NiFiFlowDeployment` / `NiFiFlowBundle`.

When you re-declare the deployment, map NiFiKop's `syncMode` to NiFiControl's `driftPolicy`:
`never` → `Ignore`, `once` → `Warn` (after first success), `always` → `Reconcile`.

`NifiConnection` (canvas connections between dataflows) maps to `NiFiConnection`, but as part of
this flow layer it only makes sense once the flows exist on 2.x. `NifiResource` (free-form
`configuration`) has no generic equivalent — re-model it with the specific canvas CRD
(`NiFiProcessGroup`, `NiFiProcessor`, `NiFiControllerService`, …).

## What does not carry over

- **Flow content** — a NiFi 1.x → 2.x upgrade, out of scope for manifest translation.
- **Per-node individual configuration and pod management** — NiFiControl uses a StatefulSet.
- **Event-driven scheduling** (`maximumEventDrivenThreadCount`) — removed in NiFi 2.x.
- **`lifo` / `leastbusy` scale-down** — NiFiControl offloads highest-ordinal-first.
- **Bootstrap notification services** — removed in NiFi 2.x; use Kubernetes-native alerting
  (see [observability.md](observability.md)).

## Checklist

1. Deploy the NiFiControl operator alongside NiFiKop (separate namespace).
2. Translate and apply `NiFiCluster` (+ `NiFiNodeGroup`s); confirm it reaches Ready.
3. Translate `NiFiUser` / `NiFiUserGroup` (+ exploded `NiFiPolicy`s), `NiFiParameterContext`s,
   and `NiFiRegistryClient`s.
4. Bring flows onto NiFi 2.x, re-version them, and deploy with `NiFiFlowDeployment`.
5. Cut traffic over, then decommission the NiFiKop-managed cluster.
