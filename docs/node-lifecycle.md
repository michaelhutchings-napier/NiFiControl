# Graceful Node Offload on Scale-Down

When a clustered, operator-managed (`mode: Internal`) NiFi cluster is scaled down, removing
a node's pod without first draining it would strand the FlowFiles queued on that node's
persistent volume until the pod (and its PVC) is recreated. NiFiControl instead drives each
removed node through NiFi's cluster offload lifecycle before deleting its pod, so the queued
data is redistributed to the nodes that remain.

```yaml
spec:
  mode: Internal
  replicas: 3
  coordination:
    zookeeperConnectString: zookeeper.dataflows.svc:2181
  scaleDown:
    offloadData: true       # drain each node before its pod is removed (default)
    timeoutSeconds: 600     # per-node disconnect + offload budget
    onTimeout: Fail         # Fail | Force
```

## How it works

NiFi requires a node to be disconnected and offloaded before it can be safely removed from a
cluster. When `spec.replicas` is reduced, NiFiControl removes nodes from the highest pod
ordinal down — the same order in which a StatefulSet deletes pods — one at a time:

1. **Disconnect** — the node is asked to leave active processing
   (`DISCONNECTING` → `DISCONNECTED`).
2. **Offload** — the node redistributes its queued FlowFiles to the remaining connected
   nodes (`OFFLOADING` → `OFFLOADED`).
3. **Remove** — the offloaded node is deleted from the cluster.
4. **Shrink** — only now is the StatefulSet's replica count decremented by one, which
   deletes that node's pod.

The cluster reports `status` reason `ScalingDown` (and `status.scaleDown` with the node and
phase) while this is in progress, and the StatefulSet is held at its current size until the
top node has fully left the cluster. The operator matches NiFi nodes to pods by their
cluster address (`<statefulset>-<ordinal>.<headless>.<namespace>.svc`), which is exactly the
`nifi.cluster.node.address` each managed node advertises.

Each node is processed across several reconciliations; the operator polls the NiFi cluster
API and advances one step at a time, so a multi-node scale-down (for example 5 → 2) drains
and removes nodes 4, then 3, then 2 in turn.

## Timeout behaviour

`timeoutSeconds` bounds how long a single node may take to disconnect and offload (a node
with a large or stuck queue can take a long time). When the budget is exceeded:

- `onTimeout: Fail` (default) halts the scale-down and reports an error, leaving the node in
  the cluster for an operator to investigate. No data is lost.
- `onTimeout: Force` removes the node from the cluster and deletes its pod anyway. This
  unblocks the scale-down but may strand any FlowFiles still queued on the node.

## Disabling offload

Set `scaleDown.offloadData: false` to skip offload and let the StatefulSet shrink
immediately. This is faster but leaves any queued FlowFiles on the removed node's persistent
volume rather than redistributing them. Offload is also skipped automatically when there is
no cluster to receive the data — a single-node cluster, or a scale-down to zero.

## Scope

Offload uses the NiFi cluster controller REST API and therefore applies only to clustered,
operator-managed clusters (`replicas > 1` with `coordination`). It is independent of, and
composes with, the rolling version upgrades and PodDisruptionBudget described in
[production-cluster.md](production-cluster.md): a PDB protects against involuntary
disruptions, while scale-down offload makes a deliberate replica reduction lossless.

## Testing

`make integration-offload-kind` stands up a two-node clustered NiFi 2.10 (plus ZooKeeper)
inside a local kind cluster and runs the live node-offload lifecycle through the cluster API,
exactly as the operator does. The cluster-node REST client and the controller's scale-down
state machine are also covered by unit tests in `pkg/nifi` and `internal/controller`.
