# NiFiControl

[![CI](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/ci.yaml/badge.svg)](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/ci.yaml)

NiFiControl is a declarative Kubernetes control plane for Apache NiFi.

The project provides a Kubernetes-native API under `nifi.controlnifi.io` for
clusters, registry clients, parameter contexts, canvas components, controller
services, and high-level flow deployments.

## Current Scope

- Managed clusters support production controls: pod scheduling (affinity,
  tolerations, topology spread, priority class), a PodDisruptionBudget, an Ingress
  with NiFi proxy-host wiring, and safe rolling/`OnDelete` version upgrades. See
  [docs/production-cluster.md](docs/production-cluster.md). Scaling a clustered
  cluster down gracefully offloads each removed node's data through the NiFi cluster
  API before its pod is deleted; see [docs/node-lifecycle.md](docs/node-lifecycle.md).
- Managed clusters expose a Kubernetes scale subresource, so they can be resized with
  `kubectl scale`, a HorizontalPodAutoscaler, or KEDA (driven by queue depth). `NiFiAutoscaler`
  is the native option: it renders a KEDA `ScaledObject` (Prometheus metrics) or a native HPA
  (CPU/memory) for a `NiFiCluster` or `NiFiNodeGroup`, with NiFi-safe defaults. Scale-downs run
  the graceful offload above. See [docs/autoscaling.md](docs/autoscaling.md).
- `NiFiNodeGroup` adds heterogeneous, independently-scalable node pools to a cluster —
  differently-sized tiers that join the same NiFi cluster, each with its own scale
  subresource. See [docs/node-groups.md](docs/node-groups.md).
- CRDs, RBAC, samples, and a controller-runtime manager are available.
- Registry clients, parameter contexts, process groups, controller services,
  processors, ports, connections, funnels, and labels reconcile against NiFi.
  `NiFiRegistryClient` supports the NiFi Registry, GitHub, and GitLab flow registry
  types, with repository settings as typed fields and access tokens sourced from Secrets.
- `NiFiReportingTask` manages a controller-level reporting task: it reconciles the task's type,
  bundle, properties (including sensitive properties from Secrets), and scheduling, and
  starts/stops it via NiFi's run-status endpoint (`state: Enabled`/`Disabled`).
- Authorization is declarative on secured clusters: `NiFiUser`/`NiFiUserGroup` manage NiFi
  tenants and `NiFiPolicy` grants a `(resource, action)` to them — e.g. authorizing a metrics
  scrape identity. See [docs/authorization.md](docs/authorization.md).
- Flow deployments import complete embedded NiFi `RegisteredFlowSnapshot`
  contents and use NiFi's asynchronous replace requests for updates.
- Flow configuration can be backed up to a ConfigMap/Secret and restored
  (imported or replaced) declaratively. See
  [docs/backup-restore.md](docs/backup-restore.md).
- Prometheus metrics and Kubernetes Events: `spec.metrics` renders a Prometheus Operator
  ServiceMonitor for NiFi 2.x's built-in `/nifi-api/flow/metrics/prometheus` endpoint (and the
  operator chart can scrape the operator itself), while controllers emit Events on provision,
  scale/offload, and backup/restore transitions. See
  [docs/observability.md](docs/observability.md).
- Public Git repositories, OCI images, and NiFi Registry sources fetch and
  materialize full snapshots. Authenticated source credentials remain under
  development.

## Full Flow Snapshots

Set `spec.source.snapshot` on a `NiFiFlowBundle` or inline on a
`NiFiFlowDeployment`. The value is the native snapshot JSON/YAML containing
`flowContents`, so nested process groups, processors, controller services,
ports, funnels, labels, connections, and other NiFi snapshot fields are passed
through without reducing them to the individual canvas CRDs.

The first reconciliation imports the snapshot below the target parent process
group. Later digest or version changes create and poll a NiFi process-group
replace request, then clean up the request and publish `SnapshotInSync` status.
See `config/samples/nifi_v1alpha1_nififlowbundle.yaml` for a complete example.

Git sources read `path` as JSON or YAML (`flow.json` by default) and record the
resolved commit SHA. NiFi Registry sources use the referenced
`NiFiRegistryClient.spec.uri` and fetch either the pinned integer version or the
latest version when `source.registry.version` is omitted.

OCI sources read `path` from the image filesystem (`flow.json` by default) and
record the resolved manifest digest. Set `source.oci.digest` to pin retrieval;
otherwise `source.oci.image` may contain a tag that is periodically refreshed.

## TLS And Credentials

Secure external NiFi 2 APIs can use a custom CA and automatic token exchange:

```yaml
spec:
  mode: External
  api:
    uri: https://nifi.example.com:8443
    tls:
      caSecretKeyRef:
        name: nifi-api-tls
        key: ca.crt
      serverName: nifi.example.com
    auth:
      usernameSecretKeyRef:
        name: nifi-api-auth
        key: username
      passwordSecretKeyRef:
        name: nifi-api-auth
        key: password
```

The three external API authentication modes — `auth.clientCertificate` (mTLS),
`auth.bearerTokenSecretKeyRef`, and username/password — are mutually exclusive.
Username/password authentication exchanges credentials at NiFi's
`/nifi-api/access/token` endpoint and caches the returned JWT until refresh.

Git, OCI, and NiFi Registry sources accept `credentials` with
`usernameSecretKeyRef`, `passwordSecretKeyRef`, `tokenSecretKeyRef`, and
`caSecretKeyRef`. Configure either a token or a username/password pair.
`insecureSkipVerify` is available for controlled development environments.
Referenced Secret changes automatically trigger reconciliation.

### Managed internal HTTPS and mTLS

Operator-managed (`mode: Internal`) clusters can run HTTPS with certificate
authentication, backed by [cert-manager](https://cert-manager.io). Select one
certificate provider — an operator-managed two-stage self-signed CA, an existing
Issuer/ClusterIssuer, or externally supplied PKCS12 Secrets:

```yaml
spec:
  mode: Internal
  internalTLS:
    enabled: true
    httpsPort: 8443
    selfSigned:
      caDuration: 8760h
```

cert-manager must be installed separately; if its CRDs are absent the cluster reports
`TLSReady=False` (`CertManagerMissing`) rather than falling back to HTTP. The operator
issues a server/node certificate (`serverAuth`+`clientAuth`, wildcard headless SANs) and
an operator client certificate (`clientAuth`), consumes the cert-manager PKCS12 keystores
directly, derives the NiFi initial admin and node identities from predictable common
names, and rolls pods on certificate rotation. See
[docs/internal-tls.md](docs/internal-tls.md) for the trust model, ownership, rotation
behaviour, the shared node-identity limitation, and migration from development HTTP.
`insecureSkipVerify` is never used for managed clusters.

## Flow Safety

`NiFiFlowDeployment` keeps bounded rollout history and stores each checkpointed
snapshot in an owner-controlled ConfigMap. Automatic rollback restores the
previous successful checkpoint after an asynchronous NiFi replacement failure:

```yaml
spec:
  rollout:
    strategy: StopAllThenApply
  rollback:
    enabled: true
    onFailure: PreviousSuccessful
    historyLimit: 5
  driftPolicy:
    mode: Reconcile
    ignoreFields:
      - component.position
```

Drift is checked every minute using NiFi 2.10's process-group download API.
Generated identifiers, snapshot metadata, and NiFi defaults are normalized
before comparison. `Ignore` skips live comparison, `Warn` reports differences,
`Reconcile` replaces drifted contents, and `Fail` blocks further rollout.

`ApplyOnly` does not explicitly change component run state.
`StopAllThenApply` stops the whole group and starts it after replacement.
`ChangedOnly` and `Rolling` use NiFi's native differential asynchronous replace
workflow, which stops and restarts affected components in dependency order.
`BlueGreen` deploys the new version as a separate candidate process group, gates it
on readiness, and transactionally switches the external boundary connections from the
live group's ports to the candidate's matching ports before retiring the old group;
a failed switch is rolled back to the original group. See
[docs/bluegreen-rollout.md](docs/bluegreen-rollout.md) for the lifecycle, port-matching
rules, queue-drain policy, and limitations.

Every strategy can be gated and controlled with `rollout.readiness` (wait for valid
components and enabled controller services, with `maxUnavailable` and `timeoutSeconds`),
`rollout.queuePolicy` (drain queues before `StopAllThenApply` stops the group, with a
`Fail`/`Drop`/`Proceed` timeout policy), `rollout.retry.maxRetries` (bounded automatic
re-attempts), and `rollout.cancel` (abort an in-flight rollout). See
[docs/rollout-readiness.md](docs/rollout-readiness.md).

## Module

```text
github.com/michaelhutchings-napier/NiFiControl
```

## Development

```bash
make generate
make manifests
go test ./...
make helm-verify
```

Run the native import/replace lifecycle against a disposable Apache NiFi
2.10.0 container with:

```bash
make integration-nifi
```

Install the CRDs and controller manifests with:

```bash
kubectl apply -k config/default
```

Install with Helm:

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

The Helm chart installs the operator and its CRDs. Note that Helm installs CRDs only on the
first install and never upgrades them, so after upgrading to a chart with new CRD fields, apply
the current CRDs explicitly:

```bash
kubectl apply --server-side --force-conflicts -f charts/nificontrol/crds/
```

It does not install an
Apache NiFi cluster by itself. Install a managed NiFi cluster as a separate
Helm release so the operator remains available for upgrades and finalization:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --create-namespace
```

The `nifi-cluster` chart creates a `NiFiCluster` resource; NiFiControl then
provisions its Services, StatefulSet, and persistent storage.

Render the sample resources with:

```bash
kubectl kustomize config/samples
```

## Continuous Integration

The [CI workflow](.github/workflows/ci.yaml) runs on every push and pull request: it builds,
`go vet`s (including the `integration` build tag), and tests the module, regenerates the CRDs,
RBAC, and deepcopy code to confirm the committed manifests are up to date, and runs
`make helm-verify`. This is the same gate expected before each change is committed.

## License

NiFiControl is licensed under the [Apache License 2.0](LICENSE).
