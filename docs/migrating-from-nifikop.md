# Migrating from NiFiKop

NiFiControl is a new API for NiFi 2.x. Treat migration from NiFiKop as both a
Kubernetes resource migration and a NiFi 1.x to 2.x flow migration.

## Kind Mapping

| NiFiKop | NiFiControl | Notes |
| --- | --- | --- |
| `NifiCluster` | `NiFiCluster`, `NiFiNodeGroup` | Review manually. NiFiControl uses StatefulSets and replicas. |
| `NifiUser` | `NiFiUser`, `NiFiPolicy` | Inline policies become separate policy CRs. |
| `NifiUserGroup` | `NiFiUserGroup`, `NiFiPolicy` | Same policy split. |
| `NifiParameterContext` | `NiFiParameterContext` | Usually clean. |
| `NifiRegistryClient` | `NiFiRegistryClient` | Usually clean. |
| `NifiNodeGroupAutoscaler` | `NiFiAutoscaler` | Scale-down strategies differ. |
| `NifiDataflow` | `NiFiFlowDeployment`, `NiFiFlowBundle` | Re-export for NiFi 2.x. |
| `NifiConnection` | `NiFiConnection` | Use as part of the new canvas model. |

## Checklist

1. Install NiFiControl beside NiFiKop in a separate namespace.
2. Create a NiFi 2.10 cluster with `NiFiCluster`.
3. Convert users, groups, policies, parameter contexts, and registry clients.
4. Export or rebuild flows for NiFi 2.x.
5. Deploy flows with `NiFiFlowDeployment`.
6. Move traffic.
7. Decommission the NiFiKop cluster.

## Watch Outs

- NiFiKop defaults to NiFi 1.x; NiFiControl targets NiFi 2.x.
- Per-node NiFiKop config does not map 1:1 to StatefulSet replicas.
- `lifo` and `leastbusy` scale-down are not implemented; NiFiControl offloads
  highest ordinals first.
- Use `adoptionPolicy` where NiFiControl should attach to existing NiFi objects.
