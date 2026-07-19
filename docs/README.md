# NiFiControl Docs

NiFiControl manages Apache NiFi 2.x with Kubernetes CRDs. The docs are short
operator references: install it, pick the CRDs you need, apply examples, and check
status conditions.

## Start

- [Production cluster](production-cluster.md)
- [Internal TLS](internal-tls.md)
- [User authentication](user-authentication.md)
- [Authorization](authorization.md)
- [Examples](../examples/)

## Flow Management

- [Flow validation](flow-validation.md)
- [BlueGreen rollout](bluegreen-rollout.md)
- [Rollout readiness](rollout-readiness.md)
- [Backup and restore](backup-restore.md)
- [Parameter providers](parameter-providers.md)

## Operations

- [Autoscaling](autoscaling.md)
- [Node groups](node-groups.md)
- [Node lifecycle](node-lifecycle.md)
- [Observability](observability.md)
- [Argo CD](argocd.md)
- [Istio](istio.md)
- [Migrating from NiFiKop](migrating-from-nifikop.md)

## API Groups

| Area | Kinds |
| --- | --- |
| Cluster | `NiFiCluster`, `NiFiNodeGroup` |
| Security | `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy` |
| Config | `NiFiParameterContext`, `NiFiParameterProvider`, `NiFiRegistryClient` |
| Canvas | `NiFiProcessGroup`, `NiFiProcessor`, `NiFiConnection`, `NiFiInputPort`, `NiFiOutputPort`, `NiFiFunnel`, `NiFiLabel`, `NiFiControllerService`, `NiFiRemoteProcessGroup`, `NiFiReportingTask` |
| Flow | `NiFiFlowBundle`, `NiFiFlowDeployment` |
| Ops | `NiFiBackup`, `NiFiRestore`, `NiFiAutoscaler` |
