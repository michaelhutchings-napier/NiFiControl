# Node Groups (Heterogeneous Node Pools)

A `NiFiNodeGroup` adds an extra pool of NiFi nodes to an existing operator-managed cluster.
All node groups join the **same** NiFi cluster as the cluster's primary pool — they share its
headless Service, ZooKeeper coordination, sensitive-properties key, and TLS materials, so they
are peers in one flow — but each group runs with its own replica count, resources, JVM heap,
storage, and scheduling, and can be **autoscaled independently** through its own scale
subresource.

This is the NiFi equivalent of Kubernetes node pools: one cluster, several differently-sized
tiers (for example a small always-on coordinator pool and a large, autoscaled worker pool).

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiNodeGroup
metadata:
  name: workers
spec:
  clusterRef:
    name: production          # an Internal, clustered NiFiCluster in the same namespace
  replicas: 4
  jvm:
    heapInitial: 4g
    heapMax: 6g
  resources:
    requests: {cpu: "2", memory: 8Gi}
  storage:
    size: 100Gi
  scheduling:
    nodeSelector: {workload: nifi-workers}
  scaleDown:
    offloadData: true
```

## Model

- The cluster's own `spec.replicas` defines the **primary pool** (it keeps the cluster's scale
  subresource). Each `NiFiNodeGroup` is an **additional pool** with its own StatefulSet named
  `<cluster>-nifi-<group>`.
- Any field a group omits is inherited from the cluster (`image`, `resources`, `jvm`,
  `storage`, `scheduling`, `upgrade`, `scaleDown`). All nodes in a cluster must run the same
  NiFi version, so override `image` only to pin the same version from a different repository.
- Groups require an `Internal`, clustered cluster (`spec.coordination` set) in the **same
  namespace** — pods join the cluster's headless Service for cluster DNS, which is
  namespace-scoped.
- TLS is consumed read-only from the cluster; the cluster controller owns certificate
  provisioning, and group pods roll in lockstep with the primary pool on rotation.

## Independent autoscaling

Each `NiFiNodeGroup` exposes a scale subresource, so a pool can be scaled on its own:

```bash
kubectl scale nifinodegroup/workers --replicas=8
```

Point an HPA or KEDA `ScaledObject` at the group (`scaleTargetRef.kind: NiFiNodeGroup`) to
autoscale that tier by its own metric, leaving the primary pool fixed. See
[autoscaling.md](autoscaling.md) for trigger and guardrail guidance — it applies per group.

## Graceful scale-down

Reducing a group's `replicas` drains each removed node through the NiFi cluster offload API
(disconnect → offload → remove, highest-ordinal first) before its pod is deleted, exactly like
the primary pool ([node-lifecycle.md](node-lifecycle.md)). Because the cluster's primary pool
always remains to receive the data, even a group's last node drains gracefully when scaling to
zero. The group's `spec.scaleDown` (or the cluster's, inherited) controls the timeout and the
`Fail`/`Force` timeout policy.

## Deletion

`deletionPolicy: Delete` removes the group's StatefulSet and its PersistentVolumeClaims when
the `NiFiNodeGroup` is deleted; `Orphan` (the default) leaves them. As with the cluster,
deletion is abrupt — to drain a group first, scale it to `0` (which offloads gracefully) and
then delete it.

## Selectors and existing clusters

Pools are distinguished by a `nifi.controlnifi.io/node-pool` label; the cluster's headless
Service and PodDisruptionBudget select all pools, while each StatefulSet's selector is
scoped to its pool. Group pods are owned by their `NiFiNodeGroup`, so the cluster's primary
StatefulSet never adopts them. Node groups can be added to any clustered, operator-managed
cluster without recreating it.
