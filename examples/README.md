# NiFiControl examples

Complete, `kubectl apply`-able scenarios. Every manifest here uses the exact spec shapes that
NiFiControl's end-to-end kind harnesses (`hack/test-*-kind.sh`) apply against real Apache NiFi 2.10,
so they are working configurations, not sketches.

Apply an example with, e.g.:

```bash
kubectl create namespace dataflows
kubectl -n dataflows apply -f examples/quickstart.yaml
```

They assume the NiFiControl operator and its CRDs are already installed (see the
[chart](../charts/nificontrol) and [docs](../docs/README.md)). TLS examples additionally require
[cert-manager](https://cert-manager.io); the autoscaling example requires
[KEDA](https://keda.sh) and/or the Prometheus Operator.

| Example | What it shows | Kinds |
| --- | --- | --- |
| [quickstart.yaml](quickstart.yaml) | Smallest possible single-node cluster (insecure, HTTP) | `NiFiCluster` |
| [secured-cluster.yaml](secured-cluster.yaml) | TLS + single-user login + an admin tenant | `NiFiCluster`, `NiFiUser`, `NiFiPolicy` |
| [tenancy.yaml](tenancy.yaml) | Parameter contexts, users, groups, and access policies | `NiFiParameterContext`, `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy` |
| [parameter-provider.yaml](parameter-provider.yaml) | Source parameters from env vars or an external secret manager | `NiFiParameterProvider` |
| [canvas-flow.yaml](canvas-flow.yaml) | A small flow built declaratively on the canvas | `NiFiProcessGroup`, `NiFiControllerService`, `NiFiProcessor`, `NiFiConnection`, `NiFiFunnel`, `NiFiOutputPort`, `NiFiLabel` |
| [flow-deployment.yaml](flow-deployment.yaml) | GitOps: version a flow bundle and deploy it | `NiFiFlowBundle`, `NiFiFlowDeployment`, `NiFiRegistryClient` |
| [backup-restore.yaml](backup-restore.yaml) | Capture a flow into a ConfigMap and restore it | `NiFiBackup`, `NiFiRestore` |
| [autoscaling.yaml](autoscaling.yaml) | Scale a cluster (or node pool) on queue depth or CPU | `NiFiNodeGroup`, `NiFiAutoscaler` |

Every NiFi-resident kind supports `deletionPolicy` (`Delete`/`Orphan`), `driftPolicy`
(`Ignore`/`Warn`/`Reconcile`/`Fail`), and `adoptionPolicy` (`Never`/`IfExists`/`AdoptById`/`AdoptByName`).
The examples set these where they matter and omit them where the defaults are fine — see
[docs/README.md](../docs/README.md) for the model.
