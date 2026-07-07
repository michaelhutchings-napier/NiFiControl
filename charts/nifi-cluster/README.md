# NiFi Cluster Helm Chart

This chart creates a `NiFiCluster` custom resource. The NiFiControl operator
then provisions the Apache NiFi Services, StatefulSet, and persistent volumes.

Install the operator first:

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

Then install a NiFi cluster in the target namespace:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --create-namespace
```

The default release is a one-node NiFi 2.10.0 instance with a 10 Gi persistent
data volume. By default it runs internal HTTP for local development and must not
be exposed publicly in that mode. For production-style HTTPS and mutual TLS,
enable `internalTLS` and choose one certificate source: an operator-managed
self-signed CA, an existing cert-manager Issuer/ClusterIssuer, or externally
supplied TLS Secrets.

For multiple NiFi nodes, provide an existing ZooKeeper ensemble:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set replicas=3 \
  --set coordination.zookeeperConnectString=zookeeper.dataflows.svc:2181
```

NiFiControl does not provision ZooKeeper in this first managed-cluster release.

To let the operator create a namespaced development CA and cert-manager
Certificates:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set internalTLS.enabled=true
```

For externally supplied TLS material, provide both server and operator-client
Secrets with `keystore.p12`, `truststore.p12`, `tls.crt`, and `tls.key`. `ca.crt`
is optional; when present NiFiControl uses it to pin trust, otherwise it uses the
system trust store.

NiFi settings the chart does not model directly can be set raw through
`configOverrides.nifiProperties` and `configOverrides.bootstrapProperties`; they
are merged into the node configuration after the operator-managed settings, so an
override wins and removing it restores the image default. Keys the operator
manages (web listener, TLS keystores, sensitive properties key, cluster/ZooKeeper
wiring, heap arguments) are rejected — use the dedicated fields instead.
`configOverrides.logbackXml` replaces the logging configuration wholesale, and the
`pod` block adds pod metadata, image pull secrets, a ServiceAccount, sidecars,
init containers, and extra volumes/mounts (for example custom NAR libraries):

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set 'configOverrides.nifiProperties.nifi\.queue\.swap\.threshold=40000'
```
