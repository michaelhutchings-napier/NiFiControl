# Controller Runtime Notes

NiFiControl controllers follow a small set of conventions while the NiFi-side
reconcilers are being built out.

## Finalizers

Every resource that will eventually own NiFi-side state receives:

```text
nifi.controlnifi.io/finalizer
```

Cleanup is policy-aware for implemented resources. `DeletionPolicy: Delete`
removes managed NiFi or Kubernetes state, while `Orphan` leaves that state in
place before removing the finalizer.

## Conditions

Controllers update common status conditions:

- `Ready`
- `Reconciling`
- `DependenciesReady`

Implemented reconcilers mark resources ready only after their NiFi-side or
Kubernetes-side state is synchronized. Placeholder reconcilers remain accepted
but not ready.

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

## Managed NiFi Workloads

`NiFiCluster` supports two modes. `External` checks a user-supplied API URI.
`Internal` reconciles a client Service, a headless Service, and a StatefulSet.
NiFi configuration and repositories share a per-node persistent volume, with an
init container copying the image's initial configuration before NiFi starts.

The first managed runtime targets Apache NiFi 1.28 in internal HTTP mode so all
existing API reconcilers can operate end to end. Multiple replicas require a
user-supplied ZooKeeper connect string. HTTPS trust, NiFi 2 token
authentication, ingress, and operator-managed ZooKeeper remain follow-up work.
