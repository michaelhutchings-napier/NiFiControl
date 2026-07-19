# Observability

NiFiControl emits Kubernetes Events and can expose Prometheus metrics for NiFi
and the operator.

## NiFi Metrics

```yaml
spec:
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
```

NiFi 2.x metrics are served from:

```text
/nifi-api/flow/metrics/prometheus
```

For secured clusters, authorize the scrape identity with:

- `NiFiUser` for the client certificate subject
- `NiFiPolicy` granting `read` on `/flow`

## Operator Metrics

```yaml
metrics:
  service:
    enabled: true
  serviceMonitor:
    enabled: true
```

## Events

Controllers emit Events for create/update/delete, readiness, scale/offload,
backup/restore, rollout, and failure transitions.

## Autoscaling

Queue metrics can feed `NiFiAutoscaler` or KEDA. See [autoscaling.md](autoscaling.md).
