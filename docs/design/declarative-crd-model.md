# NiFi Declarative CRD Model

## 1. Summary

This document proposes a next-generation, NiFiKop v2-style declarative API for
running Apache NiFi on Kubernetes. The goal is a clean Kubernetes-native control
plane where clusters, security, registry clients, parameter contexts, services,
canvas resources, and whole flow releases can be managed through versioned,
auditable custom resources.

The design intentionally keeps the strongest ideas from NiFiKop:

- Kubernetes owns the desired state.
- The operator stores NiFi-side IDs and revision versions in status.
- Finalizers clean up NiFi-side resources.
- Reconciliation is idempotent and tolerant of asynchronous NiFi operations.
- Flow deployment can be higher level than hand-managing every processor.

It also fixes the areas where the current API is hard to extend:

- Use consistent `NiFi` spelling and consistent field names.
- Put readiness, dependency state, observed generation, drift, adoption, and
  last errors into common status fields.
- Make drift and adoption explicit user choices.
- Protect operator-owned labels, selectors, annotations, and identities.
- Treat dependency ordering, stop-before-update, disable-before-update, and
  deletion policy as first-class API behavior.
- Add missing canvas and runtime CRDs so NiFi can be declarative from both high
  and low levels.

The preferred user experience is layered. Most users deploy a versioned
`NiFiFlowDeployment` from Git, NiFi Registry, OCI, or an embedded snapshot.
Advanced users can opt into generic low-level CRDs such as `NiFiProcessor`,
`NiFiConnection`, and `NiFiControllerService` when they need exact control.

## 2. Current NiFiKop CRD Review

The current repository defines API types in `api/v1`, `api/v1alpha1`, and
`api/v2alpha1`, generated CRDs under `config/crd/bases`, Helm CRDs under
`helm/nifikop/crds`, examples under `config/samples`, and reconcilers under
`internal/controller`.

### `NifiCluster`

`NifiCluster` manages an internal or external NiFi cluster. Its spec covers
client authentication, internal versus external clusters, node URI templates,
root process group ID for external clusters, Kubernetes services and pods,
cluster manager type, ZooKeeper connection settings, images, labels, managed
admin and reader users, node config groups, node definitions, disruption
budgets, LDAP, listeners, sidecars, external services, topology spread, and
single-user mode.

Important status fields:

- `status.nodesState`: per-node `gracefulActionState`, `configurationState`,
  `initClusterNode`, `podIsReady`, `creationTime`, and `lastUpdatedTime`.
- `status.state`: cluster lifecycle state such as `ClusterInitializing`,
  `ClusterReconciling`, `ClusterRunning`, or `NifiClusterNoNodes`.
- `status.rollingUpgradeStatus`.
- `status.rootProcessGroupId`.
- `status.prometheusReportingTask.id` and `.version`.

Patterns worth keeping:

- Internal and external cluster support.
- Per-node readiness and graceful scaling state.
- Root process group ID recorded in status.
- Finalizers for cluster cleanup and user cleanup.
- Patch-based updates to avoid stomping concurrent changes.
- Explicit scaling strategies for graceful upscale and downscale.

Patterns to change or avoid:

- Cluster status should use standard Kubernetes `conditions` in addition to
  custom state enums.
- Status should include `observedGeneration`, dependency readiness, and last
  sync error.
- Operator identity labels such as `nifi_cr`, `nodeId`, and
  `nifi_node_group` are essential but must be reserved and protected
  consistently across all child resources.
- Cluster child identity should use a stable operator-owned label prefix, while
  legacy labels can remain as compatibility aliases.
- Readiness should be decomposed into `ClusterReachable`, `NodesReady`,
  `CanvasReady`, and `Reconciled`.

### `NifiUser`

`NifiUser` manages a NiFi tenant user, optional certificate material, and access
policies attached directly to the user.

Important status fields:

- `status.id`: NiFi user ID.
- `status.version`: NiFi revision version.

Patterns worth keeping:

- User identity can differ from Kubernetes object name.
- Optional certificate creation and JKS output are useful.
- Access policies close to identity can be convenient.

Patterns to change or avoid:

- Inline policies should remain supported for convenience, but a standalone
  `NiFiPolicy` CRD is needed for reusable, auditable authorization.
- Status should add `observedGeneration`, `conditions`, `ready`, `drift`, and
  `lastSyncError`.
- Adoption policy should define whether an existing NiFi user with the same
  identity is adopted, rejected, or only observed.

### `NifiUserGroup`

`NifiUserGroup` manages a NiFi user group, membership references to
`NifiUser`, and inline access policies.

Important status fields:

- `status.id`: NiFi user group ID.
- `status.version`: NiFi revision version.

Patterns worth keeping:

- User references are Kubernetes-native.
- Group identity can differ from the object name.
- Membership is reconciled from desired state.

Patterns to change or avoid:

- Default identity behavior differs from `NifiUser` by using
  `<namespace>-<name>`; v2 should make identity naming explicit and consistent.
- Inline policies should be supplemented by `NiFiPolicy`.
- Membership dependency readiness should be reflected in status.

### `NifiRegistryClient`

`NifiRegistryClient` configures NiFi flow registry clients. The v1 API is a
simple NiFi Registry URI and description. The v2alpha1 API already broadens this
to a typed registry client with `registry`, `github`, and `gitlab` variants,
authentication secret references, repository location, branch/path options, and
parameter context handling.

Important status fields:

- `status.id`: NiFi registry client ID.
- `status.version`: NiFi revision version.
- v2alpha1 also tracks `status.latestSecretsResourceVersion`.

Patterns worth keeping:

- Typed registry client support is a strong precedent for v2 source modeling.
- Secret resource version tracking enables secret-driven resync.
- Status stores NiFi ID and revision version.

Patterns to change or avoid:

- Registry clients should expose standard source and auth references that can
  be reused by `NiFiFlowBundle` and `NiFiFlowDeployment`.
- Status should expose `ResolvedSource`, `Ready`, and drift fields.

### `NifiParameterContext`

`NifiParameterContext` manages NiFi parameter contexts, inherited contexts,
plain parameters, and sensitive parameters through Kubernetes secrets.

Important status fields:

- `status.id`: NiFi parameter context ID.
- `status.version`: NiFi revision version.
- `status.latestUpdateRequest`: asynchronous NiFi update request state.
- `status.latestSecretsResourceVersion`: last seen referenced secrets.

Patterns worth keeping:

- Secret-backed sensitive values are required for GitOps.
- Inherited parameter contexts are modeled.
- Asynchronous update request tracking is necessary for NiFi.
- The current `disableTakeOver` option acknowledges adoption as a real concern.

Patterns to change or avoid:

- Replace negative `disableTakeOver` semantics with explicit
  `adoptionPolicy`.
- Make sensitive values use a common `SecretKeyRef` schema.
- Add dependency readiness for inherited contexts and secret references.
- Expose `driftStatus` and conditions rather than relying on a single state.

### `NifiDataflow`

`NifiDataflow` deploys a versioned flow from a registry client into a parent
process group. It supports bucket ID, flow ID, version pinning or latest,
position, parameter context binding, sync modes (`never`, `once`, `always`),
skip-invalid flags, cluster and registry references, display name, parent
process group reference, and update strategy (`drop` or `drain`).

Important status fields:

- `status.processGroupID`: deployed process group ID.
- `status.state`: `Created`, `Starting`, `Ran`, `OutOfSync`, or `InSync`.
- `status.latestUpdateRequest`: asynchronous versioned flow update request.
- `status.latestDropRequest`: asynchronous queue drop request.

Patterns worth keeping:

- High-level flow deployment exists and should be the primary user workflow.
- Version pinning and sync mode are valuable.
- Drop/drain update strategies are necessary for safe changes.
- Parent process group can be a direct NiFi ID or a Kubernetes resource
  reference.
- Asynchronous update and drop requests are tracked in status.

Patterns to change or avoid:

- `NifiDataflow` should evolve into a clearer `NiFiFlowDeployment` with
  explicit source, target, rollout, rollback, ownership, adoption, and drift
  policies.
- Avoid forcing users to understand bucket/flow IDs when Git, OCI, or embedded
  snapshots are the desired source of truth.
- Avoid implicit adoption or takeover.
- Status needs deployed source digest/version, resource inventory, readiness,
  conditions, and drift details.

### `NifiConnection`

`NifiConnection` is a low-level canvas CRD for connecting components, currently
focused on dataflow subcomponents. It supports source and destination component
references, queue and load-balance configuration, prioritizers, bend points,
and update strategy.

Important status fields:

- `status.connectionID`: NiFi connection ID.
- `status.state`: `Created`, `OutOfSync`, or `InSync`.

Patterns worth keeping:

- Generic connection configuration maps well to NiFi.
- The reconciler handles source/destination dependency checks.
- Deletion considers drain/drop behavior and queue state.
- Drift detection is already present through out-of-sync checks.

Patterns to change or avoid:

- Component references should support all canvas object CRDs, not just
  `NifiDataflow` subcomponents.
- Status should include revision version, observed generation, queue status,
  dependency readiness, and conditions.
- Drift policy should be explicit, not inferred from sync behavior.

### `NifiNodeGroupAutoscaler`

`NifiNodeGroupAutoscaler` manages a selectable node group within a
`NifiCluster` and exposes the Kubernetes scale subresource. It uses
`clusterRef`, `nodeConfigGroupId`, `nodeLabelsSelector`, optional node config,
replicas, and scale strategies.

Important status fields:

- `status.state`: `OutOfSync` or `InSync`.
- `status.replicas`: current replica count.
- `status.selector`: pod selector for the scale subresource.

Patterns worth keeping:

- A separate autoscaler CRD lets HPA/KEDA target a node group.
- Label selectors are natural for scale targets.
- Scale subresource support is useful.

Patterns to change or avoid:

- Reserved labels used by selectors must be protected.
- Status should use standard conditions and observed generation.
- The autoscaler should not depend on user-overridable identity labels.

### `NifiResource`

`NifiResource` is a generic low-level resource CRD. It currently supports a
resource `type`, a parent process group ID or reference, display name, and raw
configuration. In the current code it is mainly useful as a process group
escape hatch.

Important status fields:

- `status.id`: NiFi resource ID.
- `status.version`: NiFi revision version.

Patterns worth keeping:

- A generic resource can provide escape-hatch coverage while typed CRDs mature.
- Raw configuration is useful for advanced users and forward compatibility.

Patterns to change or avoid:

- A single raw `NifiResource` is too vague for a stable declarative API.
- Promote important NiFi concepts to explicit CRDs with typed common fields.
- Keep an escape hatch only for rare unsupported resources.

### Cross-cutting Current Patterns

Worth keeping:

- Finalizers for CRDs that own NiFi-side resources.
- Last-applied annotations and patch calculation for Kubernetes resources.
- NiFi ID and revision version in status.
- Secret resource version tracking.
- Requeue/backoff around asynchronous NiFi update, drop, and delete requests.
- Cluster readiness checks before NiFi API calls.
- Events for reference and reconciliation errors.

Worth changing:

- Add a common status contract across all CRDs.
- Add common policies for drift, adoption, deletion, and update safety.
- Move from implicit controller behavior to explicit spec fields.
- Use `NiFi` in API names unless a compatibility alias requires `Nifi`.
- Reserve operator labels and annotations through validation and merge helpers.
- Prefer owner-specific labels under a domain prefix, for example
  `nifi.controlnifi.io/cluster`, while preserving legacy aliases where needed.

## 3. Proposed API Groups and Naming

Recommended group:

```text
nifi.controlnifi.io
```

Recommended versions:

- `v1alpha1`: new API surface while field names and semantics settle.
- `v1beta1`: after conversion, validation, and migration behavior are proven.
- `v1`: after at least one production cycle with stable conversion webhooks.

Recommended naming:

- Kind names use `NiFi`, for example `NiFiCluster`, not `NifiCluster`.
- Resource names use lowercase plural Kubernetes conventions, for example
  `nificlusters`, `nififlowdeployments`.
- Field names prefer `id`, `revision`, `ref`, `policy`, and `status` terms
  consistently across CRDs.
- Direct NiFi UUID fields use `nifiId` or object-specific names in status, not
  spec, except for explicit adoption selectors.
- References use:

```yaml
clusterRef:
  name: production
  namespace: nifi-system
```

Preferred public kinds:

- `NiFiCluster`
- `NiFiParameterContext`
- `NiFiRegistryClient`
- `NiFiControllerService`
- `NiFiReportingTask`
- `NiFiProcessGroup`
- `NiFiProcessor`
- `NiFiConnection`
- `NiFiInputPort`
- `NiFiOutputPort`
- `NiFiRemoteProcessGroup`
- `NiFiFunnel`
- `NiFiLabel`
- `NiFiUser`
- `NiFiUserGroup`
- `NiFiPolicy`
- `NiFiFlowBundle`
- `NiFiFlowDeployment`

Compatibility recommendation:

- Keep conversion support for existing `Nifi*` kinds where migration is needed.
- New CRDs and documentation should use `NiFi*`.
- Do not introduce a mixed spelling API.

## Common API Contracts

### Common Spec Fields

Most NiFi-side CRDs should support:

```yaml
spec:
  clusterRef:
    name: production
  deletionPolicy: Delete
  adoptionPolicy:
    mode: Never
  driftPolicy:
    mode: Warn
  reconciliation:
    paused: false
```

Policy enums:

- `deletionPolicy`: `Delete`, `Orphan`.
- `adoptionPolicy.mode`: `Never`, `IfExists`, `AdoptById`, `AdoptByName`.
- `driftPolicy.mode`: `Ignore`, `Warn`, `Reconcile`, `Fail`.

### Common Status Fields

All CRDs that reconcile NiFi-side state should include:

```yaml
status:
  observedGeneration: 4
  ready: true
  nifiId: 4f0d4a1a-8a2f-4b7d-91e1-4f552f8df0c1
  revision:
    version: 12
    clientId: nifi-operator
  conditions:
    - type: Ready
      status: "True"
      reason: Reconciled
      message: Resource is in sync
      observedGeneration: 4
      lastTransitionTime: "2026-06-14T12:00:00Z"
  dependencies:
    ready: true
    waitingFor: []
  drift:
    status: InSync
    lastDetectedTime: null
    differences: []
  sync:
    lastAttemptTime: "2026-06-14T12:00:00Z"
    lastSuccessfulTime: "2026-06-14T12:00:02Z"
    lastError: ""
```

Recommended condition types:

- `Ready`
- `ClusterReachable`
- `DependenciesReady`
- `Reconciling`
- `InSync`
- `DriftDetected`
- `Adopted`
- `Deleting`
- `Paused`
- `Error`

## 4. Proposed CRD Catalog

### `NiFiCluster`

Purpose: manage an internal NiFi cluster or represent an external cluster as an
operator target.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata:
  name: production
spec:
  mode: Internal
  image: apache/nifi:2.0.0
  replicas: 3
  clusterManager: Kubernetes
  security:
    tls:
      enabled: true
    authentication:
      type: OIDC
  listeners:
    web:
      port: 8443
    siteToSite:
      enabled: true
  nodeGroups:
    - name: default
      replicas: 3
      storage:
        data:
          size: 100Gi
```

Key spec fields: `mode`, `external`, `image`, `replicas`, `nodeGroups`,
`clusterManager`, `security`, `listeners`, `storage`, `podTemplate`,
`serviceTemplate`, `managedAdminRefs`, `reservedMetadataPolicy`.

Key status fields: common status, `rootProcessGroupId`, `nodes`, `endpoint`,
`version`, `prometheusReportingTask`, `rollingUpgrade`.

Dependencies: Kubernetes primitives, optional cert-manager, optional external
ZooKeeper or Kubernetes cluster manager.

Deletion behavior: finalizer deletes owned Kubernetes resources and optionally
offloads/removes NiFi nodes. PVC deletion follows explicit storage reclaim
policy.

Adoption behavior: external clusters are adopted by explicit `mode: External`
and connection information. Internal cluster child resources are not adopted
unless marked with operator ownership labels and owner references.

Drift behavior: Kubernetes child drift is reconciled by default; NiFi-side
cluster drift is reported through conditions.

Phase: 1.

### `NiFiRegistryClient`

Purpose: configure a NiFi flow registry client, including NiFi Registry, GitHub,
GitLab, or future registry providers.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRegistryClient
metadata:
  name: platform-flows
spec:
  clusterRef:
    name: production
  type: GitHub
  github:
    repositoryOwner: example
    repositoryName: nifi-flows
    defaultBranch: main
    auth:
      personalAccessToken:
        secretKeyRef:
          name: github-token
          key: token
```

Key spec fields: `clusterRef`, `type`, `description`, `nifiRegistry`, `github`,
`gitlab`, `auth`, `timeouts`, `parameterContextValues`, drift/adoption/deletion
policies.

Key status fields: common status, `nifiId`, `revision`, `resolvedType`,
`latestSecretsResourceVersion`.

Dependencies: `NiFiCluster`, referenced secrets.

Deletion behavior: delete or orphan NiFi registry client based on
`deletionPolicy`.

Adoption behavior: adopt by configured name or explicit NiFi ID when policy
allows.

Drift behavior: `Warn` by default; `Reconcile` updates description/provider
configuration when safe.

Phase: 1.

### `NiFiParameterContext`

Purpose: declaratively manage parameters, sensitive parameters, and inherited
parameter contexts.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterContext
metadata:
  name: payments-prod
spec:
  clusterRef:
    name: production
  description: Payments production parameters
  inheritedRefs:
    - name: common-prod
  parameters:
    - name: kafka.bootstrap.servers
      value: kafka:9092
    - name: db.password
      sensitiveValueFrom:
        secretKeyRef:
          name: payments-db
          key: password
  driftPolicy:
    mode: Reconcile
```

Key spec fields: `clusterRef`, `description`, `parameters`,
`sensitiveValueFrom`, `inheritedRefs`, drift/adoption/deletion policies.

Key status fields: common status, `nifiId`, `revision`,
`latestUpdateRequest`, `latestSecretsResourceVersion`.

Dependencies: `NiFiCluster`, referenced secrets, inherited
`NiFiParameterContext` resources.

Deletion behavior: finalizer deletes context only when it is not bound to
managed resources or deletion policy permits cascading disable/update.

Adoption behavior: explicit `AdoptByName` or `AdoptById`; default is `Never`
for new resources and migration tooling can set adoption policy.

Drift behavior: `Reconcile` should submit NiFi parameter context update
requests; `Fail` blocks dependent resources if UI edits are detected.

Phase: 1.

### `NiFiUser`

Purpose: manage NiFi users and optional Kubernetes certificate artifacts.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata:
  name: alice
spec:
  clusterRef:
    name: production
  identity: alice@example.com
  certificate:
    create: true
    secretName: alice-nifi-client
```

Key spec fields: `clusterRef`, `identity`, `certificate`, `authProvider`,
deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, certificate secret
metadata.

Dependencies: `NiFiCluster`, optional PKI provider.

Deletion behavior: delete or orphan NiFi tenant; certificate secret follows
owner-reference or explicit retention policy.

Adoption behavior: explicit by identity or NiFi ID.

Drift behavior: warn on NiFi-side identity mismatch; reconcile policies through
`NiFiPolicy`, not inline mutation unless configured.

Phase: 1.

### `NiFiUserGroup`

Purpose: manage NiFi user groups and membership.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUserGroup
metadata:
  name: operators
spec:
  clusterRef:
    name: production
  identity: operators
  users:
    - userRef:
        name: alice
```

Key spec fields: `clusterRef`, `identity`, `users`, deletion/adoption/drift
policies.

Key status fields: common status, `nifiId`, `revision`, `memberIds`.

Dependencies: `NiFiCluster`, `NiFiUser`.

Deletion behavior: remove group from NiFi unless orphaned; policies referencing
the group should block or cascade only by explicit policy.

Adoption behavior: explicit by identity or NiFi ID.

Drift behavior: membership drift is reconciled or warned based on policy.

Phase: 1.

### `NiFiPolicy`

Purpose: manage authorization policies independently of users and groups.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata:
  name: operators-view-flow
spec:
  clusterRef:
    name: production
  action: Read
  resource:
    type: Global
    path: /flow
  subjects:
    groups:
      - name: operators
```

Key spec fields: `clusterRef`, `action`, `resource`, `subjects`,
deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `resolvedSubjects`.

Dependencies: `NiFiCluster`, `NiFiUser`, `NiFiUserGroup`, optional component
resource references.

Deletion behavior: remove policy unless orphaned.

Adoption behavior: adopt by action/resource/subject tuple or explicit NiFi ID.

Drift behavior: `Fail` is a good default for security-sensitive environments;
`Reconcile` is acceptable for fully managed clusters.

Phase: 3, with inline policy compatibility in phase 1.

### `NiFiControllerService`

Purpose: manage generic NiFi controller services without creating one CRD per
service type.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiControllerService
metadata:
  name: dbcp-payments
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  type: org.apache.nifi.dbcp.DBCPConnectionPool
  properties:
    Database Connection URL: jdbc:postgresql://postgres/payments
  sensitiveProperties:
    Password:
      secretKeyRef:
        name: payments-db
        key: password
  state: Enabled
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `type`,
`bundle`, `properties`, `sensitiveProperties`, `parameterContextRef`, `state`,
deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `validationStatus`,
`referencingComponents`, `activeThreadCount`.

Dependencies: `NiFiCluster`, optional `NiFiProcessGroup`,
`NiFiParameterContext`, referenced secrets, controller services referenced by
properties.

Deletion behavior: disable before delete; block if unmanaged references exist
unless `force` policy is explicitly set.

Adoption behavior: adopt by name/type/parent process group or by NiFi ID.

Drift behavior: `Warn` by default; `Reconcile` disables service when required,
updates mutable fields, validates, then re-enables.

Phase: 1.

### `NiFiReportingTask`

Purpose: manage generic NiFi reporting tasks.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiReportingTask
metadata:
  name: prometheus
spec:
  clusterRef:
    name: production
  type: org.apache.nifi.reporting.prometheus.PrometheusReportingTask
  properties:
    Prometheus Metrics Endpoint Port: "9092"
  scheduling:
    period: 30 sec
  state: Running
```

Key spec fields: `clusterRef`, `type`, `bundle`, `properties`,
`sensitiveProperties`, `scheduling`, `state`, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `validationStatus`,
`runStatus`.

Dependencies: `NiFiCluster`, referenced secrets, referenced controller
services.

Deletion behavior: stop before delete.

Adoption behavior: adopt by name/type or NiFi ID.

Drift behavior: reconcile by stopping when required, mutating, validating, and
starting last.

Phase: 2.

### `NiFiProcessGroup`

Purpose: manage process groups as addressable canvas boundaries.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessGroup
metadata:
  name: payments
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    root: true
  parameterContextRef:
    name: payments-prod
  position:
    x: 100
    y: 200
```

Key spec fields: `clusterRef`, `displayName`, `parentProcessGroupRef`,
`parameterContextRef`, `position`, `comments`, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `componentCounts`,
`runningCount`, `stoppedCount`, `invalidCount`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`,
`NiFiParameterContext`.

Deletion behavior: delete only if empty by default; cascade requires explicit
policy.

Adoption behavior: adopt by parent/name or NiFi ID.

Drift behavior: reconcile metadata, position, and parameter context binding;
child inventory drift is handled by child CRDs or `NiFiFlowDeployment`.

Phase: 2.

### `NiFiProcessor`

Purpose: manage a generic NiFi processor without one CRD per processor type.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiProcessor
metadata:
  name: consume-payments
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  type: org.apache.nifi.processors.kafka.pubsub.ConsumeKafkaRecord_2_6
  properties:
    topic: payments
    group.id: payments-flow
  sensitiveProperties:
    sasl.password:
      secretKeyRef:
        name: kafka-auth
        key: password
  scheduling:
    strategy: TIMER_DRIVEN
    period: 1 sec
    concurrentlySchedulableTaskCount: 1
  state: Running
  position:
    x: 250
    y: 120
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `type`, `bundle`,
`properties`, `sensitiveProperties`, `scheduling`, `state`, `style`,
`position`, `autoTerminatedRelationships`, `parameterContextRef`.

Key status fields: common status, `nifiId`, `revision`, `runStatus`,
`validationStatus`, `relationships`, `activeThreadCount`, `inputBytes`,
`outputBytes`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`,
`NiFiControllerService`, `NiFiParameterContext`, referenced secrets.

Deletion behavior: stop before delete; terminate threads when policy allows;
block if queues must be drained first.

Adoption behavior: adopt by parent/name/type or NiFi ID.

Drift behavior: `Reconcile` stops the processor when necessary, mutates config,
then restores desired run state after dependencies are ready.

Phase: 2.

### `NiFiConnection`

Purpose: manage connections between processors, ports, funnels, remote process
groups, or flow-deployed components.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiConnection
metadata:
  name: consume-to-transform
spec:
  clusterRef:
    name: production
  source:
    processorRef:
      name: consume-payments
    relationship: success
  destination:
    processorRef:
      name: transform-payments
  configuration:
    backPressureObjectThreshold: 10000
    backPressureDataSizeThreshold: 1 GB
    loadBalanceStrategy: DO_NOT_LOAD_BALANCE
  updateStrategy:
    queue: Drain
```

Key spec fields: `clusterRef`, `source`, `destination`, `configuration`,
`bends`, `labelIndex`, `updateStrategy`, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `queue`,
`sourceId`, `destinationId`.

Dependencies: `NiFiCluster`, source and destination component CRDs.

Deletion behavior: drain or drop queue according to policy before deletion.

Adoption behavior: adopt by source/destination/relationships or NiFi ID.

Drift behavior: detect queue and endpoint drift; `Fail` should block if a UI
edit changes endpoints and adoption is not enabled.

Phase: 2.

### `NiFiInputPort`

Purpose: manage input ports inside a process group.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiInputPort
metadata:
  name: payments-in
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  state: Running
  position:
    x: 50
    y: 100
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `displayName`,
`comments`, `position`, `state`, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `runStatus`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`.

Deletion behavior: stop before delete; respect connection drain policy.

Adoption behavior: adopt by parent/name or NiFi ID.

Drift behavior: reconcile metadata and run state last.

Phase: 3.

### `NiFiOutputPort`

Purpose: manage output ports inside a process group.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiOutputPort
metadata:
  name: payments-out
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  state: Running
  position:
    x: 900
    y: 100
```

Key spec fields: same shape as `NiFiInputPort`.

Key status fields: common status, `nifiId`, `revision`, `runStatus`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`.

Deletion behavior: stop before delete; respect connection drain policy.

Adoption behavior: adopt by parent/name or NiFi ID.

Drift behavior: reconcile metadata and run state last.

Phase: 3.

### `NiFiRemoteProcessGroup`

Purpose: manage remote process groups for site-to-site flows.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRemoteProcessGroup
metadata:
  name: central-nifi
spec:
  clusterRef:
    name: edge
  parentProcessGroupRef:
    name: payments
  targetUris:
    - https://central-nifi.example.com:8443/nifi
  transportProtocol: HTTP
  communicationsTimeout: 30 sec
  yieldDuration: 10 sec
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `targetUris`,
`transportProtocol`, `proxy`, `localNetworkInterface`, `timeout`,
`yieldDuration`, `position`, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`, `transmissionStatus`,
`targetSecure`, `inputPortCount`, `outputPortCount`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`, network reachability.

Deletion behavior: stop transmission before delete.

Adoption behavior: adopt by parent/name/target URI or NiFi ID.

Drift behavior: warn by default because remote NiFi endpoints may change
outside this cluster.

Phase: 3.

### `NiFiFunnel`

Purpose: manage funnels as connection fan-in/fan-out points.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFunnel
metadata:
  name: payments-errors
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  position:
    x: 500
    y: 320
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `position`,
deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`.

Deletion behavior: block if connected unless connection deletion policy is
explicit.

Adoption behavior: adopt by parent/position/name annotation or NiFi ID.

Drift behavior: reconcile position only.

Phase: 3.

### `NiFiLabel`

Purpose: manage visual labels on the canvas.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiLabel
metadata:
  name: payments-note
spec:
  clusterRef:
    name: production
  parentProcessGroupRef:
    name: payments
  text: Payments ingestion
  style:
    fontSize: 18px
    backgroundColor: "#ffffff"
  position:
    x: 100
    y: 40
```

Key spec fields: `clusterRef`, `parentProcessGroupRef`, `text`, `style`,
`position`, dimensions, deletion/adoption/drift policies.

Key status fields: common status, `nifiId`, `revision`.

Dependencies: `NiFiCluster`, parent `NiFiProcessGroup`.

Deletion behavior: delete or orphan.

Adoption behavior: adopt by parent/text/position or NiFi ID.

Drift behavior: reconcile visual fields.

Phase: 3.

### `NiFiFlowBundle`

Purpose: define a reusable flow artifact independent of a target cluster.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowBundle
metadata:
  name: payments
spec:
  source:
    git:
      url: https://github.com/example/nifi-flows.git
      ref: main
      path: flows/payments
  version:
    semver: 1.4.2
  parameters:
    schema:
      - name: kafka.bootstrap.servers
        required: true
      - name: db.password
        sensitive: true
```

Key spec fields: `source`, `version`, `parameters.schema`, `validation`,
`dependencies`, `signature`.

Key status fields: common status without NiFi ID, `artifactDigest`,
`resolvedRevision`, `validated`, `inventory`.

Dependencies: Git, OCI, NiFi Registry, or embedded artifact source.

Deletion behavior: Kubernetes-only; no NiFi resources owned directly.

Adoption behavior: not applicable.

Drift behavior: source drift means resolved artifact changed; deployments decide
whether to roll out.

Phase: 1 for minimal source abstraction, phase 3 for signatures and advanced
validation.

### `NiFiFlowDeployment`

Purpose: deploy and reconcile a complete flow into a target process group from
a `NiFiFlowBundle` or inline source.

Example:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiFlowDeployment
metadata:
  name: payments-prod
spec:
  clusterRef:
    name: production
  source:
    bundleRef:
      name: payments
    version: 1.4.2
  target:
    parentProcessGroupRef:
      root: true
    processGroupName: payments
  parameterContextRef:
    name: payments-prod
  rollout:
    strategy: Rolling
    updateStrategy:
      queue: Drain
      stopComponents: ChangedOnly
  rollback:
    enabled: true
    onFailure: PreviousSuccessful
  driftPolicy:
    mode: Warn
  ownership:
    mode: Managed
```

Key spec fields: `clusterRef`, `source`, `target`, `parameterContextRef`,
`version`, `promotion`, `rollout`, `rollback`, `driftPolicy`,
`ownership`, `adoptionPolicy`, `deletionPolicy`.

Key status fields: common status, `deployedVersion`, `artifactDigest`,
`processGroupId`, `componentInventory`, `lastSuccessfulDeployment`,
`rolloutHistory`, `syncState`.

Dependencies: `NiFiCluster`, source provider or `NiFiFlowBundle`,
`NiFiRegistryClient`, target `NiFiProcessGroup`, `NiFiParameterContext`,
controller services and secrets referenced by the flow.

Deletion behavior: delete managed process group when `deletionPolicy: Delete`;
orphan when requested.

Adoption behavior: explicit adoption of existing target process group by name
or ID; record adopted component inventory before first mutation.

Drift behavior: first-class. `Ignore` records nothing, `Warn` records drift,
`Reconcile` restores desired artifact, and `Fail` blocks rollout until drift is
resolved or adopted.

Phase: 1.

## 5. Layered Model

### Layer 1: Platform CRDs

Platform CRDs establish the cluster, registry, parameters, identity, and
authorization substrate:

- `NiFiCluster`
- `NiFiRegistryClient`
- `NiFiParameterContext`
- `NiFiUser`
- `NiFiUserGroup`
- `NiFiPolicy`

These reconcile before runtime and canvas CRDs. They should have strong
validation, common status, and conservative deletion behavior.

### Layer 2: Runtime and Service CRDs

Runtime/service CRDs configure reusable NiFi services:

- `NiFiControllerService`
- `NiFiReportingTask`

These depend on cluster reachability and parameter contexts. Desired enabled or
running state is applied after configuration and validation.

### Layer 3: Canvas CRDs

Canvas CRDs model exact flow objects:

- `NiFiProcessGroup`
- `NiFiProcessor`
- `NiFiConnection`
- `NiFiInputPort`
- `NiFiOutputPort`
- `NiFiRemoteProcessGroup`
- `NiFiFunnel`
- `NiFiLabel`

These are optional for users who prefer high-level flow deployment. They are
useful for small flows, shared infrastructure process groups, and cases where
GitOps wants object-level ownership.

### Layer 4: High-level GitOps CRDs

High-level CRDs model artifacts and releases:

- `NiFiFlowBundle`
- `NiFiFlowDeployment`

These should be the default workflow for real applications. They let teams
promote versioned flow artifacts across environments without maintaining a CRD
for every processor and connection.

## 6. High-level Flow Deployment Design

`NiFiFlowDeployment` replaces the current `NifiDataflow` shape with a broader
release abstraction.

### Source Reference

Supported sources:

```yaml
source:
  registry:
    registryClientRef:
      name: platform-registry
    bucketId: 6b4d
    flowId: 51ab
    version: 7
```

```yaml
source:
  git:
    url: https://github.com/example/nifi-flows.git
    ref: refs/tags/payments-1.4.2
    path: payments/flow.json
    auth:
      secretKeyRef:
        name: git-auth
        key: token
```

```yaml
source:
  oci:
    image: ghcr.io/example/nifi-flows/payments:1.4.2
    digest: sha256:...
```

```yaml
source:
  embedded:
    snapshotRef:
      configMapKeyRef:
        name: payments-flow
        key: flow.json
```

`source.bundleRef` should be preferred when a reusable `NiFiFlowBundle` exists.

### Target Process Group

Targets should support root, existing process group reference, existing NiFi ID
for adoption, or creation by name:

```yaml
target:
  parentProcessGroupRef:
    name: platform
  processGroupName: payments
```

### Parameter Context Binding

```yaml
parameterContextRef:
  name: payments-prod
```

The operator must bind the deployment process group to the resolved parameter
context before starting components.

### Version Pinning and Promotion

`NiFiFlowDeployment` should require an immutable source pin for production:

```yaml
source:
  bundleRef:
    name: payments
  version: 1.4.2
promotion:
  channel: production
  provenance:
    promotedFrom: staging
    approvedBy: platform-team
```

Mutable refs such as Git branches can be allowed in dev, but status must record
the resolved commit, artifact digest, or registry version.

### Rollout Strategy

```yaml
rollout:
  strategy: Rolling
  maxUnavailableComponents: 1
  updateStrategy:
    queue: Drain
    stopComponents: ChangedOnly
    disableControllerServices: ChangedOnly
  readinessGates:
    requireValidComponents: true
    requireEnabledControllerServices: true
```

Recommended strategies:

- `ApplyOnly`: create/update but do not start components.
- `StopAllThenApply`: safest for disruptive changes.
- `ChangedOnly`: stop only changed processors/services.
- `Rolling`: apply subgraphs where dependencies allow.
- `BlueGreen`: create a new process group and switch upstream/downstream
  connections when possible.

### Rollback Strategy

```yaml
rollback:
  enabled: true
  onFailure: PreviousSuccessful
  historyLimit: 5
```

Status should retain the last successful artifact digest and enough NiFi
resource inventory to roll back or report why rollback is unsafe.

### Ownership and Adoption

```yaml
ownership:
  mode: Managed
  prune: true
adoptionPolicy:
  mode: AdoptByName
  requireAnnotation: true
```

Ownership modes:

- `Managed`: operator owns the whole target process group inventory.
- `InventoryOnly`: operator tracks and reports drift but does not mutate.
- `Hybrid`: operator owns only resources marked with operator ownership
  annotations.

Adoption must be explicit. First adoption should snapshot existing inventory
into status and optionally a ConfigMap for audit before mutation.

### Drift Policy

```yaml
driftPolicy:
  mode: Fail
  ignoreFields:
    - component.position
    - component.style
```

Modes:

- `Ignore`: do not compare live NiFi state to desired source.
- `Warn`: detect and report drift but do not mutate.
- `Reconcile`: restore desired source.
- `Fail`: mark deployment not ready and block rollout until resolved.

### Status

```yaml
status:
  observedGeneration: 12
  ready: true
  deployedVersion: 1.4.2
  artifactDigest: sha256:...
  processGroupId: 4f0d
  syncState: InSync
  componentInventory:
    processors: 12
    controllerServices: 3
    connections: 18
  rolloutHistory:
    - version: 1.4.2
      digest: sha256:...
      deployedAt: "2026-06-14T12:00:00Z"
      result: Succeeded
  conditions:
    - type: Ready
      status: "True"
      reason: RolloutComplete
```

## 7. Low-level CRD Design

Low-level CRDs are generic by NiFi object category. The operator must not create
one CRD per processor, controller service, or reporting task type.

### Generic Component Fields

Processors, controller services, and reporting tasks share the same shape:

```yaml
spec:
  clusterRef:
    name: production
  type: org.apache.nifi.example.Component
  bundle:
    group: org.apache.nifi
    artifact: nifi-standard-nar
    version: 2.0.0
  properties:
    property.name: value
  sensitiveProperties:
    password:
      secretKeyRef:
        name: component-secret
        key: password
  parameterContextRef:
    name: production-params
  state: Running
```

Processors additionally use:

```yaml
parentProcessGroupRef:
  name: payments
scheduling:
  strategy: TIMER_DRIVEN
  period: 1 sec
  concurrentlySchedulableTaskCount: 1
position:
  x: 100
  y: 100
autoTerminatedRelationships:
  - failure
```

Controller services use `state: Enabled` or `Disabled`. Reporting tasks and
processors use `state: Running`, `Stopped`, or `Disabled` where supported.

### References

References should support Kubernetes refs first and explicit NiFi IDs only for
adoption and migration:

```yaml
parentProcessGroupRef:
  name: payments
```

```yaml
adoptionPolicy:
  mode: AdoptById
  nifiId: 4f0d4a1a-8a2f-4b7d-91e1-4f552f8df0c1
```

### Sensitive Properties

Sensitive properties must never be stored in status or events. Status may record
secret names, keys, and resource versions, but not values.

```yaml
sensitiveProperties:
  Password:
    secretKeyRef:
      name: db-auth
      key: password
```

## 8. Reconciliation Strategy

### Dependency Graph

The operator should build a graph per cluster reconciliation target. Nodes are
CRDs and NiFi-side components. Edges are references, parent-child relationships,
and runtime dependencies.

Base ordering:

1. Cluster reachable.
2. Registry clients.
3. Parameter contexts.
4. Controller services.
5. Process groups.
6. Processors, ports, funnels, labels, remote process groups.
7. Connections.
8. Desired running/enabled/transmitting state.

`NiFiFlowDeployment` should expand its artifact into an internal inventory graph
and reconcile that graph as one deployment transaction.

### Topological Ordering

Each reconcile pass should:

1. Resolve references.
2. Populate dependency readiness in status.
3. Topologically sort ready resources.
4. Apply create/update/delete operations.
5. Apply runtime state last.

Cycles are possible through controller service references. The operator should
detect cycles and handle them with a staged plan: create disabled, configure,
validate, enable.

### Finalizers

Every CRD that owns NiFi-side resources should use a finalizer:

```text
nifi.controlnifi.io/<plural>-finalizer
```

Finalizer logic must:

- Respect `deletionPolicy`.
- Stop or disable resources when required.
- Drain or drop queues only by explicit policy.
- Remove NiFi-side resources idempotently.
- Remove the finalizer when the cluster is gone and no safe cleanup is possible,
  while recording an event.

### Status Conditions

Conditions should be updated every reconcile pass with the current observed
generation. Do not rely on a single enum for all states. Use conditions for
machine consumers and events for human breadcrumbs.

### Retries and Backoff

Use exponential backoff for transient NiFi API errors, but keep bounded polling
for NiFi asynchronous requests such as parameter context updates, versioned flow
updates, queue drops, and deletes.

### Idempotent Create, Update, Delete

Create:

- Search by adoption policy if status has no NiFi ID.
- Create if no adopted object is found.
- Store NiFi ID and revision in status immediately.

Update:

- Fetch latest NiFi entity and revision.
- Compare desired versus live using normalized component models.
- Apply only needed changes.
- Refresh status revision after each successful mutation.

Delete:

- Fetch by status NiFi ID.
- Treat missing NiFi object as success.
- Run pre-delete safety operations first.

### NiFi Revision Handling

NiFi uses optimistic revisions. The operator should:

- Store last seen revision in status.
- Always fetch latest before mutation when possible.
- Retry on revision conflict by refreshing live state and recalculating diff.
- Preserve user-facing drift semantics. A revision conflict caused by manual UI
  edits should become drift, not an infinite retry loop.

### Stop, Disable, and Update Behavior

Mutation plan examples:

- Processor property change: stop processor, update config, validate, restore
  desired run state.
- Controller service property change: disable service and referencing services
  if required, update, validate, re-enable in dependency order.
- Connection endpoint change: stop or drain according to policy, delete/recreate
  if NiFi cannot update in place.
- Parameter context update: submit update request, poll until complete, then
  reconcile dependents.

### Readiness Gates

Resources are ready only when:

- The cluster is reachable.
- Dependencies are ready.
- NiFi-side entity exists and status has current ID/revision.
- Validation state is acceptable.
- Desired runtime state has been reached.
- Drift policy is satisfied.

### Drift Detection

Drift comparison should normalize NiFi objects by removing volatile fields:

- revision client IDs
- timestamps
- runtime counters
- bulletins
- current thread counts
- status snapshots

Drift status should report the affected fields without storing sensitive values.

### Adoption

Adoption is a transition from unmanaged NiFi object to managed Kubernetes CRD.
It should:

- Require an explicit policy.
- Prefer `AdoptById` for critical resources.
- Support `AdoptByName` only within a scoped parent and type.
- Record `status.adopted: true`.
- Record the original NiFi ID, revision, and inventory.
- Avoid mutation on the first pass when `adoptionPolicy.reviewBeforeMutate` is
  true.

## 9. Safety Rules

### Reserved Metadata

Operator-owned labels and annotations must not be user-overridable. Reserved
keys include existing NiFiKop labels:

- `app`
- `nifi_cr`
- `nodeId`
- `nifi_node_group`

Recommended new labels:

- `app.kubernetes.io/name`
- `app.kubernetes.io/instance`
- `app.kubernetes.io/managed-by`
- `nifi.controlnifi.io/cluster`
- `nifi.controlnifi.io/node-id`
- `nifi.controlnifi.io/node-group`
- `nifi.controlnifi.io/component-id`
- `nifi.controlnifi.io/owner-kind`
- `nifi.controlnifi.io/owner-name`

Recommended annotations:

- `nifi.controlnifi.io/last-applied-cluster`
- `nifi.controlnifi.io/last-applied-source-digest`
- `nifi.controlnifi.io/adopted-from-id`
- `nifi.controlnifi.io/deletion-policy`

### Conflict Handling

For user-provided metadata on Kubernetes child resources:

- Reject conflicting reserved labels at admission when they are in CRD specs.
- Override conflicts in generated child objects if admission is not available.
- Emit a warning event when a conflict is ignored or overwritten.
- Never let a user-provided label affect selectors used by the operator to find
  owned resources.

For selectors:

- Selector labels must be derived only from operator-owned identity.
- User labels may be copied to metadata, but never to required match labels.

For NiFi objects:

- The operator should add a non-sensitive marker when NiFi supports comments or
  metadata fields, for example an owner annotation in component comments only if
  it does not pollute user-facing text.
- Ownership must primarily be tracked in Kubernetes status and inventory, not by
  trusting mutable display names alone.

## 10. Phased Roadmap

### Phase 1

Foundation and high-value workflows:

- API conventions and conversion strategy.
- Common status, conditions, references, and policies.
- Reserved label and annotation protection.
- `NiFiCluster`.
- `NiFiParameterContext`.
- `NiFiRegistryClient`.
- `NiFiControllerService`.
- Minimal `NiFiFlowBundle`.
- `NiFiFlowDeployment` for NiFi Registry and at least one Git source.
- SecretKeyRef support for sensitive properties.
- Dependency graph engine.
- Basic drift detection with `Ignore`, `Warn`, `Reconcile`, and `Fail`.
- Explicit adoption policy model.

### Phase 2

Object-level canvas and runtime expansion:

- `NiFiReportingTask`.
- `NiFiProcessGroup`.
- `NiFiProcessor`.
- `NiFiConnection`.
- Adoption workflows and inventory snapshots.
- Full drift reporting with normalized diffs.
- Stop-before-update and disable-before-update planners.
- Rollback to previous successful flow deployment.
- Migration tooling from existing `NifiDataflow`, `NifiParameterContext`, and
  `NifiRegistryClient`.

### Phase 3

Full canvas coverage and advanced GitOps:

- `NiFiInputPort`.
- `NiFiOutputPort`.
- `NiFiRemoteProcessGroup`.
- `NiFiFunnel`.
- `NiFiLabel`.
- Standalone `NiFiPolicy`.
- Advanced promotion and rollback.
- OCI flow bundles.
- Artifact signatures and verification.
- Blue/green flow deployments where graph topology allows.
- Advanced policy controls for pruning, orphaning, and managed/unmanaged
  boundaries.

## Recommended Defaults

- `driftPolicy.mode: Warn` for development and `Fail` for production
  deployments where manual UI edits must be reviewed.
- `adoptionPolicy.mode: Never` unless migration tooling sets it explicitly.
- `deletionPolicy: Orphan` for high-level flow deployments in early releases,
  then `Delete` only after deletion safety has matured.
- `ownership.mode: Managed` for `NiFiFlowDeployment` targets created by the
  operator.
- Runtime state changes are always applied last.

## Migration Notes

Existing users should be able to migrate incrementally:

- Keep existing `Nifi*` CRDs served while introducing `NiFi*` CRDs.
- Provide conversion or migration tooling from `NifiDataflow` to
  `NiFiFlowDeployment`.
- Map `NifiParameterContext.spec.disableTakeOver: false` to
  `adoptionPolicy.mode: AdoptByName`.
- Map `NifiDataflow.spec.syncMode` to `driftPolicy` and rollout behavior:
  `never` becomes `Ignore`, `once` becomes `Warn` after first success, and
  `always` becomes `Reconcile`.
- Preserve existing NiFi IDs and revisions in status during migration whenever
  possible.
- Preserve legacy labels such as `nifi_cr` and `nodeId` as compatibility
  labels, but source selectors from new protected labels.
