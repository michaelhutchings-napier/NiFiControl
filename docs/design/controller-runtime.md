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

