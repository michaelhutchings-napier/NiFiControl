# NiFi Cluster Helm Chart

Creates a `NiFiCluster` custom resource. The NiFiControl operator must already
be installed.

## Install

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --create-namespace
```

## Secure Cluster

```bash
helm upgrade --install production ./charts/nifi-cluster \
  --namespace dataflows \
  --set internalTLS.enabled=true
```

cert-manager must be installed when generated TLS is used. With TLS enabled,
`selfSigned` is used unless `issuerRef.name` or `external.serverSecretName` is set.

## External NiFi

```yaml
mode: External
api:
  uri: https://nifi.example.com:8443
  tls:
    caSecretKeyRef:
      name: nifi-api-tls
      key: ca.crt
  auth:
    bearerTokenSecretKeyRef:
      name: nifi-api-auth
      key: token
```

## Bundle Resources

The chart can render related CRs with the cluster:

```yaml
parameterContexts:
  - name: app
    spec:
      parameters:
        - name: env
          value: prod

users:
  - name: prometheus
    spec:
      identity: "CN=prometheus, O=monitoring"

flowDeployments:
  - name: app
    spec:
      source:
        bundleRef:
          name: app
      target:
        processGroupName: app
```

For larger environments, keep the operator, cluster, and flows as separate Helm
releases or Argo CD apps.
