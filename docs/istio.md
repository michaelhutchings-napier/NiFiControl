# Running NiFiControl clusters in an Istio mesh

NiFiControl needs no special operator support to run inside Istio. Sidecar injection is done by
Istio's mutating webhook when a pod is created, and because NiFiControl manages a **StatefulSet**
(not individual pods), the operator only owns the pod *template* — it never rewrites running pods, so
it never strips an injected `istio-proxy`. (Per-pod operators have to re-merge the injected sidecar
on every reconcile; that problem does not exist here.) What does need attention is **NiFi
clustering**, whose node-to-node protocols are raw TCP that a sidecar can break. This page covers
both.

## Enabling injection

Label the namespace (or annotate the pods). Namespace-wide:

```bash
kubectl label namespace dataflows istio-injection=enabled
# or, for a revisioned control plane:
kubectl label namespace dataflows istio.io/rev=<revision>
```

Per-cluster instead of namespace-wide, via the pod template NiFiControl already exposes:

```yaml
spec:
  pod:
    annotations:
      sidecar.istio.io/inject: "true"
```

## Let NiFi clustering bypass the sidecar (important)

A NiFi cluster's nodes talk to each other over three **raw TCP** ports — cluster protocol
(`11443`), load balance (`6342`), and site-to-site / remote input (`10000`) (defaults; whatever you
set under `spec.ports`). An `istio-proxy` applying mTLS and protocol sniffing to these breaks node
join and load balancing. Exclude them from the sidecar so node-to-node traffic runs direct, and keep
NiFi's own `spec.internalTLS` for encryption between nodes:

```yaml
spec:
  internalTLS:
    enabled: true            # encrypts node-to-node traffic that bypasses the mesh
  pod:
    annotations:
      sidecar.istio.io/inject: "true"
      traffic.sidecar.istio.io/excludeInboundPorts: "11443,6342,10000"
      traffic.sidecar.istio.io/excludeOutboundPorts: "11443,6342,10000"
```

The mesh still fronts the **web/API** port, which is where mesh policy and telemetry are most useful.
Trying to route the cluster protocol *through* the mesh (PERMISSIVE, no exclusions) is fragile and
usually fails to form a cluster — excluding those ports is the reliable choice.

A single-node cluster (`replicas: 1`) has no node-to-node traffic, so the exclusions are unnecessary
there.

## TLS interplay: mesh mTLS vs NiFi internalTLS

Decide who owns TLS on the **web port**; do not accidentally stack both:

- **Secured NiFi (authentication enabled).** NiFi only allows user authentication over HTTPS, so a
  secured cluster runs `internalTLS` and serves the web port as HTTPS (`8443`). NiFi is already doing
  TLS, so the sidecar cannot also terminate it — set a `PeerAuthentication` of `PERMISSIVE` (or
  `DISABLE`) for the NiFi workload, or exclude `8443` from the sidecar too, and rely on NiFi's TLS
  end to end. Do **not** put a STRICT mTLS `PeerAuthentication` in front of NiFi's own HTTPS port.
- **Plaintext NiFi (no authentication).** Serve the web port as HTTP (`8080`) with `internalTLS`
  off, and let the mesh provide mTLS and a STRICT `PeerAuthentication`. Suitable for internal,
  unauthenticated clusters where the mesh is the security boundary.

## Startup ordering

NiFi makes outbound calls while booting — to peers for cluster election, and to the Kubernetes API
server (Kubernetes coordination mode) or ZooKeeper. Make sure the proxy is ready first, or those
calls fail during startup:

- Use Istio **native sidecars** (Kubernetes 1.29+ / Istio 1.20+), or set
  `proxy.holdApplicationUntilProxyStarts: true` (globally or via the
  `proxy.istio.io/config` pod annotation).
- ZooKeeper coordination reaches ZK over a TCP port; if ZooKeeper is outside the mesh, allow the
  egress. Kubernetes coordination talks HTTPS to the API server and works through the sidecar.

## Exposing NiFi through an Istio ingress gateway

Front the cluster's web Service with a `Gateway` + `VirtualService` as usual. NiFi rejects proxied
requests whose `Host` it does not recognise, so add the gateway host to NiFi's allow-list:

```yaml
spec:
  additionalProxyHosts:
    - nifi.example.com          # the host clients use at the gateway
```

Route the `VirtualService` to the operator-rendered web Service (or an
[external Service](production-cluster.md) you define), targeting the `web` port.

## What NiFiControl does and does not do

- **Does not** add mesh-specific fields or reconcile-time sidecar handling — none is needed with a
  StatefulSet. You drive injection and port exclusions through the standard `spec.pod.annotations`
  shown above.
- **Does** keep node-to-node traffic encrypted independently of the mesh when you enable
  `spec.internalTLS`, which is what makes the "exclude the cluster ports" pattern safe.
