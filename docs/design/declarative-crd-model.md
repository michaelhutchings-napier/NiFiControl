# Declarative CRD Model

NiFiControl models NiFi 2.x in layers. Users can manage a whole flow from a
snapshot or manage individual canvas components when they need fine control.

## Layers

| Layer | Kinds |
| --- | --- |
| Platform | `NiFiCluster`, `NiFiNodeGroup` |
| Security/config | `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy`, `NiFiParameterContext`, `NiFiParameterProvider`, `NiFiRegistryClient` |
| Canvas | `NiFiProcessGroup`, `NiFiProcessor`, `NiFiConnection`, `NiFiInputPort`, `NiFiOutputPort`, `NiFiFunnel`, `NiFiLabel`, `NiFiControllerService`, `NiFiRemoteProcessGroup`, `NiFiReportingTask` |
| GitOps flow | `NiFiFlowBundle`, `NiFiFlowDeployment` |
| Operations | `NiFiBackup`, `NiFiRestore`, `NiFiAutoscaler` |

## Common API Shape

Most resources use:

- `spec.clusterRef`
- `spec.deletionPolicy`
- `spec.adoptionPolicy` where adoption is meaningful
- `status.ready`
- `status.nifiId`
- `status.revision`
- `status.conditions`

## Design Rules

- Prefer NiFi 2.x APIs and snapshot formats.
- Keep credentials in Secrets.
- Preserve existing NiFi state unless a CR explicitly owns it.
- Use finalizers for remote deletes.
- Make flow deployment safe by default: status, history, rollback, and drift
  reporting.

## Flow Deployment

`NiFiFlowDeployment` applies a full `RegisteredFlowSnapshot` from:

- inline YAML/JSON
- `NiFiFlowBundle`
- Git
- OCI
- NiFi Registry

Rollout strategies include `ApplyOnly`, `StopAllThenApply`, `ChangedOnly`,
`Rolling`, and `BlueGreen`.

## Current Direction

Keep the low-level canvas CRDs for exact control, but make `NiFiFlowDeployment`
the preferred GitOps interface for full flows.
