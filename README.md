# NiFiControl

NiFiControl is a declarative Kubernetes control plane for Apache NiFi.

The project provides a Kubernetes-native API under `nifi.controlnifi.io` for
clusters, registry clients, parameter contexts, canvas components, controller
services, and high-level flow deployments.

## Current Scope

- CRDs, RBAC, samples, and a controller-runtime manager are available.
- Registry clients, parameter contexts, process groups, controller services,
  processors, ports, connections, funnels, and labels reconcile against NiFi.
- Flow deployments reconcile their target process group and source metadata.
- Complete flow artifact fetching and materialization remain under development.

## Module

```text
github.com/michaelhutchings-napier/NiFiControl
```

## Development

```bash
make generate
make manifests
go test ./...
make helm-verify
```

Install the CRDs and controller manifests with:

```bash
kubectl apply -k config/default
```

Install with Helm:

```bash
helm upgrade --install nificontrol ./charts/nificontrol \
  --namespace nificontrol-system \
  --create-namespace
```

The Helm chart installs the operator and its CRDs. It does not install an
Apache NiFi cluster.

Render the sample resources with:

```bash
kubectl kustomize config/samples
```
