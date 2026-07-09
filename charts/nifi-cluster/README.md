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
The `logging` block tunes `conf/logback.xml` for the common cases (root and per-logger
levels, `nifi-app.log` retention, mirroring the app log to stdout) by overlaying onto
NiFi's default logback so its noise suppression is kept; `configOverrides.logbackXml`
remains the wholesale escape hatch for custom appenders (the two are mutually exclusive).
The `pod` block adds pod metadata, image pull secrets, a ServiceAccount, sidecars,
init containers, and extra volumes/mounts (for example custom NAR libraries):

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set 'configOverrides.nifiProperties.nifi\.queue\.swap\.threshold=40000'
```

## Bundling resources with the cluster (one config file)

By default this chart renders only the `NiFiCluster`, and you apply parameter contexts, users,
flows, and other resources as their own CRs (see `config/samples/`). If you would rather manage
them in one place, the chart also stamps them out from `values.yaml` lists, so a single
`helm install` brings up the cluster together with everything it needs:

```yaml
# values.yaml
parameterContexts:
  - name: payments-prod
    spec:
      parameters:
        - {name: kafka.bootstrap.servers, value: kafka:9092}
        - name: db.password
          sensitiveValueFrom: {secretKeyRef: {name: payments-db, key: password}}
users:
  - name: alice
    spec: {identity: "CN=alice"}
flowBundles:
  - name: payments
    spec: {version: "1.0.0", source: {snapshot: {flowContents: {identifier: payments-flow, name: payments}}}}
flowDeployments:
  - name: payments-prod
    spec:
      source: {bundleRef: {name: payments}, version: "1.0.0"}
      target: {parentProcessGroupRef: {root: true}, processGroupName: payments-release}
      parameterContextRef: {name: payments-prod}
```

Each list item is `{name, spec, [annotations], [labels]}`, where `spec` is the CR spec verbatim
(consult the CRD or `config/samples/` for the fields). Available lists: `users`, `userGroups`,
`policies`, `registryClients`, `parameterContexts`, `controllerServices`, `reportingTasks`,
`flowBundles`, `flowDeployments`, `processGroups`, `remoteProcessGroups`, `processors`,
`connections`, `funnels`, `inputPorts`, `outputPorts`, `labels`, `nodeGroups`, `autoscalers`,
`backups`, `restores`.

For every kind except `flowBundles` (cluster-agnostic flow content) and `autoscalers` (which
reference their target explicitly), `clusterRef` is filled in with **this** cluster when you
omit it — so you set it once. The operator reconciles the CRs asynchronously, so ordering within
the release does not matter: a `flowDeployments` entry that references a bundle and parameter
context created in the same release simply waits until they exist. The lists all default to
empty, so leaving them out keeps the chart a cluster-only release.
