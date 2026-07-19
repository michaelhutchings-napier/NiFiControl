# Istio

NiFiControl does not need special mesh support. Enable sidecar injection through
Istio labels or through `spec.pod.metadata`.

## Enable Injection

```bash
kubectl label namespace dataflows istio-injection=enabled
```

Per cluster:

```yaml
spec:
  pod:
    metadata:
      annotations:
        sidecar.istio.io/inject: "true"
```

## Important Ports

Let NiFi clustering bypass the sidecar unless you have explicitly tested mesh
traffic for it:

```yaml
spec:
  pod:
    metadata:
      annotations:
        traffic.sidecar.istio.io/excludeInboundPorts: "11443,6342"
        traffic.sidecar.istio.io/excludeOutboundPorts: "11443,6342"
```

## TLS

Istio mTLS and NiFi internal TLS are separate. Use NiFi internal TLS for NiFi
identity, authorizer bootstrap, and site-to-site trust. Use Istio mTLS for pod
network encryption and mesh policy.
