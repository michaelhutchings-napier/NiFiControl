# Autoscaling

`NiFiCluster` and `NiFiNodeGroup` expose the Kubernetes scale subresource. Scale
them with `kubectl scale`, HPA, KEDA, or the native `NiFiAutoscaler`.

`NiFiAutoscaler` is the recommended path. It creates:

- a KEDA `ScaledObject` for Prometheus queue metrics, or
- a native HPA for CPU/memory.

Scale-down uses NiFiControl's graceful offload path before deleting pods.

## Queue-Based Example

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiAutoscaler
metadata:
  name: ingest
spec:
  scaleTargetRef:
    kind: NiFiCluster
    name: production
  minReplicas: 1
  maxReplicas: 5
  metrics:
    - type: Prometheus
      prometheus:
        serverAddress: http://prometheus.monitoring.svc:9090
        query: sum(nifi_amount_items_queued)
        threshold: "10000"
```

## CPU Example

```yaml
spec:
  scaleTargetRef:
    kind: NiFiNodeGroup
    name: workers
  minReplicas: 1
  maxReplicas: 6
  metrics:
    - type: Resource
      resource:
        name: cpu
        targetAverageUtilization: 70
```

## Notes

- Use `NiFiNodeGroup` targets when only one pool should grow.
- Use queue-depth metrics for flow pressure; CPU is a fallback signal.
- Secured Prometheus endpoints can use `authenticationRef`.
- Highest ordinals are offloaded first during scale-down.
