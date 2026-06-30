# Observability

NiFiControl surfaces two observability signals: **Prometheus metrics** (for NiFi clusters and
for the operator itself) and **Kubernetes Events** for notable lifecycle transitions. Metrics
are what make queue-depth autoscaling possible — see [docs/autoscaling.md](autoscaling.md).

## NiFi cluster metrics

NiFi 2.x **always** serves metrics in Prometheus text format from its REST API at
`/nifi-api/flow/metrics/prometheus`, on the same web port NiFi already exposes. The standalone
`PrometheusReportingTask` from NiFi 1.x was **removed in NiFi 2.0**
([NIFI-13507](https://issues.apache.org/jira/browse/NIFI-13507)), so there is no reporting
task to provision and no extra port to open. The endpoint exposes the root process group
status recursively (per-connection queue depth and byte counts, processor stats) plus
connection analytics — e.g. `nifi_amount_flowfiles_queued`, `nifi_amount_bytes_queued`.

Because the endpoint already exists, `spec.metrics` only controls whether the operator renders
a Prometheus Operator **ServiceMonitor** pointing at it:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata:
  name: production
spec:
  mode: Internal
  metrics:
    enabled: true
    # path defaults to /nifi-api/flow/metrics/prometheus
    serviceMonitor:
      enabled: true
      interval: 30s
      scrapeTimeout: 10s
      labels:
        release: kube-prometheus-stack   # so your Prometheus selects it
```

The operator renders a ServiceMonitor that selects the cluster's **client-facing** Service
(the headless Service is skipped so each pod is scraped once and not-ready pods are excluded),
on the named `web` port, with the metrics path. Status is reported on the `MetricsReady`
condition and `status.metrics`:

| condition reason | meaning |
| --- | --- |
| `ServiceMonitorReady` | the ServiceMonitor was applied |
| `MetricsEnabled` | metrics on, no ServiceMonitor requested |
| `CRDsNotInstalled` | the Prometheus Operator CRDs are absent |
| `Disabled` | metrics turned off (a previously rendered ServiceMonitor is removed) |

Metrics are **best-effort and never block cluster readiness**: a missing Prometheus Operator
or a transient apply error is reported on `MetricsReady` (and as a `MetricsDegraded` Event) but
the cluster still becomes `Ready`. Install the Prometheus Operator separately — this project
never installs it.

### TLS clusters

On a cluster with `internalTLS.enabled`, NiFi serves metrics over HTTPS and the endpoint
requires authentication (NiFi 2.x accepts a client certificate or a bearer token). The
operator renders the ServiceMonitor with `scheme: https`, `serverName` set to the Service FQDN,
and a `tlsConfig` referencing the operator-managed client certificate Secret
(`ca.crt`/`tls.crt`/`tls.key`). Two prerequisites apply:

1. The Prometheus instance must be able to read that Secret (Prometheus Operator loads the
   referenced keys), and
2. the client-certificate identity must be **authorized to read** in NiFi. Until policy
   automation lands (a future `NiFiPolicy`), grant the operator client identity read access, or
   set `serviceMonitor.insecureSkipVerify: true` only for development.

## Operator metrics

The operator exposes controller-runtime metrics (reconcile counts, work-queue latency, Go
runtime) on its metrics endpoint. The `nificontrol` chart can render a ServiceMonitor for it:

```yaml
# values.yaml for the nificontrol chart
metrics:
  service:
    enabled: true
  serviceMonitor:
    enabled: true
    interval: 30s
```

## Kubernetes Events

The controllers emit Events on notable transitions, visible with `kubectl describe` or
`kubectl get events`. Events are transition-gated and aggregated by Kubernetes, so they are not
noisy:

| Object | Type | Reason | When |
| --- | --- | --- | --- |
| NiFiCluster | Normal | `Ready` | the cluster became ready and its API is reachable |
| NiFiCluster | Normal | `ScalingDown` | a graceful scale-down started |
| NiFiCluster | Normal | `OffloadingNode` / `NodeOffloaded` | a node began / finished offloading during scale-down |
| NiFiCluster | Warning | `<reason>` | a reconcile step failed (e.g. `StatefulSetReconcileFailed`) |
| NiFiCluster | Warning | `MetricsDegraded` | the metrics ServiceMonitor could not be reconciled |
| NiFiNodeGroup | Normal | `OffloadingNode` / `NodeOffloaded` | a node-group node offloaded during scale-down |
| NiFiBackup | Normal / Warning | `BackupComplete` / `<reason>` | a flow backup succeeded or failed |
| NiFiRestore | Normal / Warning | `RestoreComplete` / `<reason>` | a flow restore succeeded or failed |

## Feeding the autoscaler

With NiFi metrics in Prometheus, a KEDA `ScaledObject` (or any HPA backed by a metrics adapter)
can scale a `NiFiCluster` or `NiFiNodeGroup` on dataflow signals such as
`sum(nifi_amount_flowfiles_queued)`. KEDA targets the scale subresource, so scale-downs still
run the operator's graceful offload. See [docs/autoscaling.md](autoscaling.md) and the samples
under `config/samples/autoscaling/`.
