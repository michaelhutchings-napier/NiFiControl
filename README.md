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
- Git, OCI, and Registry sources currently resolve source metadata; fetching
  and materializing their remote flow artifacts remain under development.

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
