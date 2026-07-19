# NiFiControl

[![CI](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/ci.yaml/badge.svg)](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/ci.yaml)
[![E2E (kind)](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/e2e.yaml/badge.svg)](https://github.com/michaelhutchings-napier/NiFiControl/actions/workflows/e2e.yaml)

NiFiControl is a Kubernetes operator for Apache NiFi 2.x. It manages NiFi clusters,
security, canvas resources, flow deployments, backups, autoscaling, and day-2 operations
through CRDs in the `nifi.controlnifi.io` API group.

Start here:

- [Documentation index](docs/README.md)
- [Examples](examples/)
- [Operator Helm chart](charts/nificontrol/)
- [NiFi cluster Helm chart](charts/nifi-cluster/)

## Install

Install the operator:

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

Install a NiFi cluster as a separate release:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --create-namespace
```

The operator chart installs CRDs and the controller. The cluster chart creates a
`NiFiCluster` resource; the operator then creates the Services, StatefulSet, TLS
materials, storage, and related resources.

## Main CRDs

| Area | Kinds |
| --- | --- |
| Cluster | `NiFiCluster`, `NiFiNodeGroup` |
| Security | `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy` |
| Configuration | `NiFiParameterContext`, `NiFiParameterProvider`, `NiFiRegistryClient` |
| Canvas | `NiFiProcessGroup`, `NiFiProcessor`, `NiFiConnection`, `NiFiInputPort`, `NiFiOutputPort`, `NiFiFunnel`, `NiFiLabel`, `NiFiControllerService`, `NiFiRemoteProcessGroup`, `NiFiReportingTask` |
| Flows | `NiFiFlowBundle`, `NiFiFlowDeployment` |
| Operations | `NiFiBackup`, `NiFiRestore`, `NiFiAutoscaler` |

## Common Tasks

- Production cluster settings: [docs/production-cluster.md](docs/production-cluster.md)
- Internal HTTPS and mTLS: [docs/internal-tls.md](docs/internal-tls.md)
- User login: [docs/user-authentication.md](docs/user-authentication.md)
- Authorization: [docs/authorization.md](docs/authorization.md)
- Flow deployments: [docs/flow-validation.md](docs/flow-validation.md), [docs/bluegreen-rollout.md](docs/bluegreen-rollout.md)
- Autoscaling: [docs/autoscaling.md](docs/autoscaling.md)
- Backup and restore: [docs/backup-restore.md](docs/backup-restore.md)
- Migration from NiFiKop: [docs/migrating-from-nifikop.md](docs/migrating-from-nifikop.md)

## Development

```bash
make generate
make manifests
go test ./...
make helm-verify
```

Run the main kind E2E checks with:

```bash
make integration-authz-kind
make integration-flowdeploy-kind
make integration-autoscaler-kind
```

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
