# Controller Runtime Notes

NiFiControl controllers follow the same small contract.

## Finalizers

Resources that create remote NiFi state add a finalizer. `deletionPolicy:
Delete` removes remote state; `Orphan` only removes the finalizer.

## Conditions

Every resource reports `Ready` and, when useful, `Reconciling` plus specific
conditions such as `TLSReady`, `ClusterReachable`, or `SnapshotInSync`.

## Dependencies

Child resources reference clusters with `spec.clusterRef`. Controllers wait for
the target `NiFiCluster` to be `Ready` before using the NiFi API.

## Idempotency

Controllers adopt by stored NiFi id first, then by stable names where supported.
Updates use NiFi revisions and retry on transient API failures.
