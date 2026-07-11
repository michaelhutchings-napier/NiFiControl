# NiFiControl Documentation

NiFiControl is a declarative Kubernetes control plane for Apache NiFi 2.x. It exposes the whole
NiFi lifecycle — clusters, tenancy, canvas components, and GitOps flow deployments — as Custom
Resources under `nifi.controlnifi.io`, and reconciles them against a running NiFi over its REST API.

New here? Start with the [runnable examples](../examples/) — each is a complete, `kubectl apply`-able
scenario, verified end to end against real NiFi in kind.

## Getting started

- **[Examples](../examples/)** — copy-paste scenarios from a one-node quickstart to a full secured
  cluster with tenants, a declarative flow, GitOps deployments, autoscaling, and backup/restore.
- **[Production cluster hardening](production-cluster.md)** — pod scheduling, PodDisruptionBudget,
  Ingress + proxy hosts, rolling / `OnDelete` upgrades, termination grace, and OpenShift SCCs.

## Concepts

NiFiControl manages two layers. The **control plane** (`NiFiCluster`, `NiFiNodeGroup`) owns the
StatefulSet, services, TLS, and coordination. The **canvas/tenant/flow layer** (users, groups,
policies, parameter contexts, process groups, processors, connections, ports, controller services,
reporting tasks, flow bundles, and flow deployments) reconciles by driving the NiFi REST API. Every
NiFi-resident kind shares `deletionPolicy` (Delete/Orphan), `driftPolicy` (Ignore/Warn/Reconcile/Fail),
and `adoptionPolicy` (Never/IfExists/AdoptById/AdoptByName), so you choose per resource whether the
operator prunes, reports, reconciles, or adopts what it finds in NiFi.

## Security

- **[Internal HTTPS and mutual TLS](internal-tls.md)** — cert-manager-backed TLS, self-signed CA /
  issuerRef / external BYO PKCS12, zero-restart certificate auto-reload, and per-node identity
  certificates via the cert-manager CSI driver.
- **[User authentication](user-authentication.md)** — single-user, LDAP, and OIDC login for a
  secured cluster.
- **[Authorization (tenants and access policies)](authorization.md)** — `NiFiUser`, `NiFiUserGroup`,
  and `NiFiPolicy`, and how the operator authorizes itself and other clients.

## Flow management

- **[Backup and restore](backup-restore.md)** — capture a process group's flow into a ConfigMap or
  Secret and restore it back.
- **[BlueGreen rollouts](bluegreen-rollout.md)** — transactional flow rollouts where the graph
  topology allows.
- **[Rollout readiness and controls](rollout-readiness.md)** — gate rollouts on valid components and
  enabled services, drain queues, bound retries, and cancel in-flight rollouts.

## Scaling and lifecycle

- **[Autoscaling](autoscaling.md)** — a NiFi-aware `NiFiAutoscaler` that renders a KEDA `ScaledObject`
  (Prometheus/queue-depth) or a native HPA (CPU/memory), with safe scale-down defaults.
- **[Node groups](node-groups.md)** — heterogeneous node pools sharing one NiFi cluster.
- **[Graceful node offload](node-lifecycle.md)** — how scale-down offloads a removed node's data
  through the NiFi cluster API before the pod is deleted.

## Observability

- **[Observability](observability.md)** — Prometheus metrics, the rendered `ServiceMonitor` (incl.
  TLS clusters), operator metrics, and Kubernetes Events.

## Migration and reference

- **[Migrating from NiFiKop](migrating-from-nifikop.md)** — a kind-by-kind mapping of NiFiKop's
  `Nifi*` resources to NiFiControl's `NiFi*` equivalents.
- **[Design: the declarative CRD model](design/declarative-crd-model.md)** — the API design,
  ownership/drift/adoption model, and roadmap.

## API kinds at a glance

| Area | Kinds |
| --- | --- |
| Cluster | `NiFiCluster`, `NiFiNodeGroup` |
| Tenancy | `NiFiUser`, `NiFiUserGroup`, `NiFiPolicy`, `NiFiParameterContext` |
| Canvas | `NiFiProcessGroup`, `NiFiProcessor`, `NiFiConnection`, `NiFiInputPort`, `NiFiOutputPort`, `NiFiFunnel`, `NiFiLabel`, `NiFiControllerService`, `NiFiRemoteProcessGroup`, `NiFiReportingTask` |
| Flow / GitOps | `NiFiRegistryClient`, `NiFiFlowBundle`, `NiFiFlowDeployment` |
| Operations | `NiFiBackup`, `NiFiRestore`, `NiFiAutoscaler` |
