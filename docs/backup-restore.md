# Backup and Restore

`NiFiBackup` captures a process group's flow definition as a NiFi
`RegisteredFlowSnapshot`. `NiFiRestore` imports or replaces a process group from
that snapshot.

This backs up flow configuration, not repository contents. Pair it with PVC
snapshots if you need queued FlowFiles or repository data.

## Backup

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiBackup
metadata:
  name: root-backup
spec:
  clusterRef:
    name: production
  target:
    processGroupId: root
  storage:
    configMap:
      name: root-backup
```

Use a Secret instead of a ConfigMap when the snapshot includes sensitive
properties.

## Restore

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiRestore
metadata:
  name: root-restore
spec:
  clusterRef:
    name: production
  source:
    backupRef:
      name: root-backup
  target:
    parentProcessGroupId: root
  mode: Import
```

Use `mode: Replace` when restoring over an existing process group.
