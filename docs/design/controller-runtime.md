# Controller Runtime Notes

NiFiControl controllers follow a small set of conventions while the NiFi-side
reconcilers are being built out.

## Finalizers

Every resource that will eventually own NiFi-side state receives:

```text
nifi.controlnifi.io/finalizer
```

The current implementation removes this finalizer without NiFi cleanup because
NiFi-side reconciliation has not been implemented yet. Cleanup behavior will
become policy-aware before any controller creates remote NiFi resources.

## Conditions

Controllers update common status conditions:

- `Ready`
- `Reconciling`
- `DependenciesReady`

Until NiFi-side reconciliation exists, resources are accepted but not marked
ready.

## Cluster Dependencies

Cluster-scoped resources require `spec.clusterRef`. Controllers index
`spec.clusterRef` as `namespace/name` and watch `NiFiCluster` changes. When a
cluster changes, dependent registry clients, parameter contexts, controller
services, and flow deployments are requeued.

This keeps dependency status fresh without requiring users to touch dependent
objects after a cluster becomes ready.

## NiFi API Reachability

`NiFiCluster` supports an initial `spec.api.uri` field. When set, the controller
checks `GET /nifi-api/flow/about` and marks the cluster ready if the endpoint
responds with a 2xx or 3xx status code.

This is intentionally narrow. Authentication, TLS trust configuration, and rich
cluster discovery should be added before using the checker as the full NiFi
client implementation.
