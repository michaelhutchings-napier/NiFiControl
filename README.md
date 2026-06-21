# NiFiControl

NiFiControl is a declarative Kubernetes control plane for Apache NiFi.

The project provides a Kubernetes-native API under `nifi.controlnifi.io` for
clusters, registry clients, parameter contexts, canvas components, controller
services, and high-level flow deployments.

## Current Scope

- CRDs, RBAC, samples, and a controller-runtime manager are available.
- Registry clients, parameter contexts, process groups, controller services,
  processors, ports, connections, funnels, and labels reconcile against NiFi.
- Flow deployments import complete embedded NiFi `RegisteredFlowSnapshot`
  contents and use NiFi's asynchronous replace requests for updates.
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

Use `auth.bearerTokenSecretKeyRef` instead when a token is managed externally.
Username/password authentication exchanges credentials at NiFi's
`/nifi-api/access/token` endpoint and caches the returned JWT until refresh.

Git, OCI, and NiFi Registry sources accept `credentials` with
`usernameSecretKeyRef`, `passwordSecretKeyRef`, `tokenSecretKeyRef`, and
`caSecretKeyRef`. Configure either a token or a username/password pair.
`insecureSkipVerify` is available for controlled development environments.
Referenced Secret changes automatically trigger reconciliation.

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

The Helm chart installs the operator and its CRDs. It does not install an
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
