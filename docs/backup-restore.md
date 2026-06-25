# Flow Backup and Restore

NiFiControl captures and restores **flow configuration** declaratively through two CRDs:

- `NiFiBackup` takes a point-in-time copy of a process group's flow and stores it in a
  ConfigMap or Secret.
- `NiFiRestore` applies a captured snapshot back into a cluster, either importing it as a new
  process group or replacing an existing group's contents.

The captured artifact is the same NiFi `RegisteredFlowSnapshot` that a `NiFiFlowDeployment`
imports, so backups are portable across clusters and namespaces and can be committed to Git.

## What is and is not backed up

`NiFiBackup` captures the **flow definition** — process groups, processors, controller
services, connections, ports, funnels, labels, and their configuration — exactly as NiFi's
process-group download API returns it. It does **not** copy the data sitting in the
persistent repositories (queued FlowFiles, content, provenance), and sensitive property
values are exported in the encoded form NiFi uses for versioned flows.

Point-in-time recovery of the persistent **repositories** is a storage-layer concern handled
with CSI `VolumeSnapshot` objects against the cluster's data PersistentVolumeClaims (which the
operator labels `nifi.controlnifi.io/cluster=<name>-nifi`). This requires a CSI driver with a
`VolumeSnapshotClass`; it is intentionally left to the storage layer rather than reimplemented
in the operator, because snapshot capability, consistency, and retention are storage-specific.
See "Repository snapshots" below.

## Backing up a flow

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiBackup
metadata:
  name: nightly-root-flow
spec:
  clusterRef:
    name: production
  processGroupId: ""        # empty = the whole flow (root process group)
  storage:
    type: ConfigMap         # ConfigMap (default) | Secret
    name: ""                # defaults to "<backup-name>-flow"
```

The operator downloads the process group, writes the snapshot under the `flow.json` key of an
owned ConfigMap (or Secret), and records the result in `status`:

```yaml
status:
  phase: Succeeded
  processGroupId: 1b9f...
  storageRef: nightly-root-flow-flow
  storageType: ConfigMap
  digest: 4c2f...           # sha256 of the snapshot
  sizeBytes: 18324
  completedTime: "..."
```

A backup is a one-shot, immutable artifact: once `phase: Succeeded` it is never recaptured.
Take a new backup by creating another `NiFiBackup`. The stored ConfigMap/Secret is owned by
the `NiFiBackup`, so deleting the backup deletes its artifact. Use a Secret when the flow
includes encoded sensitive values you want held as Secret data.

## Restoring a flow

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRestore
metadata:
  name: restore-from-nightly
spec:
  clusterRef:
    name: production
  source:
    backupRef: nightly-root-flow      # or source.storageRef: {type, name}
  mode: Import                        # Import (default) | Replace
  targetProcessGroupId: ""            # empty = root
```

- **Import** (default, non-destructive) creates the snapshot as a new child process group
  under the target. `status.restoredProcessGroupId` is the new group.
- **Replace** swaps the target group's contents with the snapshot using NiFi's asynchronous
  replace workflow, polling until it completes.

`source` is either a `backupRef` (a `NiFiBackup` in the same namespace — the restore waits for
it to reach `Succeeded`) or a `storageRef` pointing directly at a ConfigMap/Secret with a
`flow.json` key (for example a snapshot committed to Git and applied as a ConfigMap). Exactly
one of the two must be set. A restore is also one-shot: once `phase: Succeeded` it is not
re-applied.

## Disaster-recovery and migration

To migrate a flow to another cluster, apply the backup's ConfigMap (or a `NiFiBackup`-produced
Secret) in the target namespace and create a `NiFiRestore` with `source.storageRef`
referencing it and `clusterRef` pointing at the new cluster. Because the artifact is a plain
ConfigMap/Secret, it can be exported, version-controlled, and re-applied anywhere.

## Repository snapshots

When you also need the queued data, snapshot the cluster's data volumes with CSI. The managed
StatefulSet's PVCs are labelled `nifi.controlnifi.io/cluster=<name>-nifi`:

```bash
kubectl get pvc -l nifi.controlnifi.io/cluster=production-nifi
# For each PVC, create a VolumeSnapshot referencing your VolumeSnapshotClass.
```

Pair a `NiFiBackup` (flow definition) with VolumeSnapshots (repository data) for a complete
recovery point. Restoring repository data means provisioning new PVCs from the snapshots
before the StatefulSet starts; the flow definition is restored with `NiFiRestore`.
