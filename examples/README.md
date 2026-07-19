# NiFiControl examples

Complete, `kubectl apply`-able scenarios for Apache NiFi 2.10.

Apply an example with, e.g.:

```bash
kubectl create namespace dataflows
kubectl -n dataflows apply -f examples/quickstart.yaml
```

Install the operator first. TLS examples require cert-manager. Autoscaling
examples require KEDA and/or the Prometheus Operator.

| Example | What it shows | Kinds |
| --- | --- | --- |
| [quickstart.yaml](quickstart.yaml) | Smallest possible single-node cluster (insecure, HTTP) | `NiFiCluster` |
| [secured-cluster.yaml](secured-cluster.yaml) | TLS + single-user login + an admin tenant | `NiFiCluster`, `NiFiUser`, `NiFiPolicy` |
| [ldap-auth.yaml](ldap-auth.yaml) | LDAP/LDAPS login for humans + a client-cert (mTLS) service account | `NiFiCluster`, `NiFiUser`, `NiFiPolicy` |
| [tenancy.yaml](tenancy.yaml) | Parameter contexts, users, groups, and access policies | `NiFiParameterContext`, `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy` |
| [parameter-provider.yaml](parameter-provider.yaml) | Source parameters from env vars or an external secret manager | `NiFiParameterProvider` |
| [canvas-flow.yaml](canvas-flow.yaml) | A small flow built declaratively on the canvas | `NiFiProcessGroup`, `NiFiControllerService`, `NiFiProcessor`, `NiFiConnection`, `NiFiFunnel`, `NiFiOutputPort`, `NiFiLabel` |
| [flow-deployment.yaml](flow-deployment.yaml) | GitOps: version a flow bundle and deploy it | `NiFiFlowBundle`, `NiFiFlowDeployment`, `NiFiRegistryClient` |
| [flow-validation.yaml](flow-validation.yaml) | Pre-flight dry run: validate a flow without deploying it | `NiFiFlowDeployment` |
| [backup-restore.yaml](backup-restore.yaml) | Capture a flow into a ConfigMap and restore it | `NiFiBackup`, `NiFiRestore` |
| [autoscaling.yaml](autoscaling.yaml) | Scale a cluster (or node pool) on queue depth or CPU | `NiFiNodeGroup`, `NiFiAutoscaler` |

Most NiFi-resident kinds support `deletionPolicy`, `driftPolicy`, and
`adoptionPolicy`. The examples set them only where they matter.
