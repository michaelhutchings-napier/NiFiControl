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

The default release is a one-node NiFi 1.28.0 instance with a 10 Gi persistent
data volume. NiFi 1.28 is pinned because its official container supports the
internal HTTP mode needed for end-to-end reconciliation today. Do not expose
this initial unsecured mode publicly. NiFi 2 support requires the upcoming
HTTPS trust and token-authentication layer.

For multiple NiFi nodes, provide an existing ZooKeeper ensemble:

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set replicas=3 \
  --set coordination.zookeeperConnectString=zookeeper.dataflows.svc:2181
```

NiFiControl does not provision ZooKeeper in this first managed-cluster release.
Secure HTTPS and authentication are the next runtime layer.
