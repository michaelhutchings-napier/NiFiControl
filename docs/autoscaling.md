# Autoscaling

`NiFiCluster` exposes a Kubernetes **scale subresource**, so a clustered cluster can be
resized by anything that speaks the standard scale API — `kubectl scale`, a
HorizontalPodAutoscaler, or (recommended) KEDA. NiFiControl deliberately does **not** ship a
bespoke autoscaler: the scaling *decision* belongs to KEDA/HPA, and the operator owns *safe
execution* of the resulting replica change.

```bash
kubectl scale nificluster/production --replicas=5
```

The scale subresource maps:

| field | path |
| --- | --- |
| desired replicas | `.spec.replicas` |
| current replicas | `.status.replicas` |
| pod selector | `.status.selector` |

An autoscaler writes `.spec.replicas`; the operator reconciles the StatefulSet to match and
reports current size and the NiFi-node label selector back in `.status`.

## Why this composes with graceful offload

Scaling a stateful NiFi cluster down is not free: each removed node must hand its queued
FlowFiles to the nodes that remain. Because the autoscaler targets the `NiFiCluster` scale
subresource (not the StatefulSet directly), a scale-down runs the operator's
[graceful node offload](node-lifecycle.md) — disconnect → offload → remove, highest-ordinal
first, one node at a time — instead of abruptly deleting pods. Autoscaling is therefore
lossless by construction, and the `spec.scaleDown` policy (timeout, `onTimeout`) still applies.

Each offload is comparatively expensive, so autoscaling NiFi should be **deliberate, not
twitchy**. The guidance below reflects that.

## NiFiAutoscaler (recommended)

`NiFiAutoscaler` is the NiFi-native way to autoscale. It does **not** implement its own metrics
loop or scaling algorithm — it *renders* the right backend for you and adds NiFi-safe defaults,
validation, and one status surface:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiAutoscaler
metadata:
  name: production
spec:
  target:
    kind: NiFiCluster        # or NiFiNodeGroup, to autoscale a single tier
    name: production
  minReplicas: 3
  maxReplicas: 9
  metrics:
    - type: Prometheus
      prometheus:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(nifi_amount_items_queued)   # NiFi 2.x FlowFile queue depth
        threshold: "10000"
  behavior:
    scaleDownStrategy: HighestOrdinal
    stabilizationSeconds: 600
    maxNodesPerStep: 1
```

What it does:

- **Renders a KEDA `ScaledObject`** when a `Prometheus` metric is used (KEDA must be installed;
  otherwise the autoscaler reports `MetricsReady=False`/`KEDANotInstalled` and waits), or a
  **native `HorizontalPodAutoscaler`** for a `Resource` (cpu/memory) metric — no KEDA needed.
- **NiFi-safe defaults**: `minReplicas` floored at 1 (never scale a cluster to zero),
  one-node-at-a-time scale-down, a long stabilization window — because each node offload is
  expensive.
- **Targets `NiFiCluster` or `NiFiNodeGroup`** via their scale subresource, so scale-downs run
  the operator's [graceful node offload](node-lifecycle.md).
- **`scaleDownStrategy`** is forward-looking: `HighestOrdinal` (the default) and `NonPrimary`
  describe the operator's current highest-ordinal-first offload, which keeps the
  coordinator-eligible ordinal 0 until last. `LeastBusy` is reserved and rejected for now —
  removing an arbitrary node requires pod-level management rather than a StatefulSet.

It composes with the lower-level objects below; reach for those directly only if you need a
KEDA/HPA feature the CRD does not yet expose.

## KEDA directly

NiFi load is event/queue-driven, so scale on dataflow signals (queue depth, backpressure,
connection counts) rather than CPU. KEDA scales on those external metrics and drives the same
scale subresource (it manages an HPA underneath); a `NiFiAutoscaler` renders exactly this, or
you can write the `ScaledObject` yourself:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: production-nifi
spec:
  scaleTargetRef:
    apiVersion: nifi.controlnifi.io/v1alpha1
    kind: NiFiCluster
    name: production
  minReplicaCount: 3          # keep enough nodes for steady-state throughput
  maxReplicaCount: 9
  cooldownPeriod: 600         # wait before scaling down — offload is expensive
  advanced:
    horizontalPodAutoscalerConfig:
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 600
          policies:
            - type: Pods
              value: 1        # remove at most one node at a time
              periodSeconds: 300
  triggers:
    - type: prometheus
      metadata:
        serverAddress: http://prometheus.monitoring.svc:9090
        # Total FlowFiles queued across the flow, scraped from NiFi's metrics.
        query: sum(nifi_amount_items_queued)
        threshold: "10000"          # target queued FlowFiles per node
```

A complete example is in `config/samples/autoscaling/keda_scaledobject_nificluster.yaml`.

## HorizontalPodAutoscaler (without KEDA)

A plain HPA also works against the scale subresource. CPU/memory are weak proxies for
dataflow load, so prefer an external/Prometheus metric where possible:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: production-nifi
spec:
  scaleTargetRef:
    apiVersion: nifi.controlnifi.io/v1alpha1
    kind: NiFiCluster
    name: production
  minReplicas: 3
  maxReplicas: 9
  behavior:
    scaleDown:
      stabilizationWindowSeconds: 600
      policies:
        - type: Pods
          value: 1
          periodSeconds: 300
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
```

See `config/samples/autoscaling/hpa_nificluster.yaml`.

> The HPA/KEDA controller needs RBAC to read and update the `nificlusters/scale` subresource.
> KEDA grants this for its managed HPAs; for a hand-written HPA, grant the
> horizontal-pod-autoscaler service account `get`/`update`/`patch` on
> `nifi.controlnifi.io/nificlusters/scale`.

## Getting the metric signal

KEDA's Prometheus trigger needs NiFi metrics in Prometheus. NiFi 2.x **always** serves
Prometheus-format metrics from its REST API at `/nifi-api/flow/metrics/prometheus` on the web
port (the standalone `PrometheusReportingTask` was removed in NiFi 2.0). Turn on
`spec.metrics` and the operator renders a `ServiceMonitor` for that endpoint:

```yaml
spec:
  metrics:
    enabled: true
    serviceMonitor:
      enabled: true
      interval: 30s
```

This requires the Prometheus Operator CRDs (`monitoring.coreos.com`); if they are absent the
cluster reports `MetricsReady=False` (`CRDsNotInstalled`) and otherwise reconciles normally.
On a TLS cluster the scrape uses HTTPS with the operator-managed client certificate, and the
scrape identity needs NiFi read authorization. See [docs/observability.md](observability.md)
for the full picture, and `config/samples/autoscaling/servicemonitor_nificluster.yaml` for a
hand-written ServiceMonitor if you prefer to manage it yourself.

## Guardrails

- Keep `minReplicaCount`/`minReplicas` at or above your steady-state throughput need; a NiFi
  cluster needs at least one node and benefits from headroom during offload.
- Use a long `scaleDown` stabilization window and a one-node-at-a-time policy — this matches
  the operator's sequential offload and avoids thrashing.
- Node ordinal 0 (the typical primary/coordinator) is removed last, because the operator
  always scales down from the highest ordinal.
- `spec.replicas` has a minimum of 1; the operator never scales a managed cluster to zero.
