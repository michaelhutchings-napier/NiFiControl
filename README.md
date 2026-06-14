# NiFiControl

NiFiControl is a declarative Kubernetes control plane for Apache NiFi.

The project is starting with a clean API surface under `nifi.controlnifi.io`
and a Kubernetes-native model for clusters, registry clients, parameter
contexts, controller services, and high-level flow deployments.

## Initial Scope

- Phase 1 API types compile.
- Controller manager starts with Phase 1 reconcilers.
- Reconcilers add/remove the shared finalizer and publish standard accepted
  status conditions.
- No NiFi-side reconciliation is implemented yet.
- CRD and RBAC manifests are generated with `controller-gen`.

## Module

```text
github.com/michaelhutchings-napier/NiFiControl
```

## Development

```bash
make generate
make manifests
go test ./...
```

Install the CRDs and controller manifests with:

```bash
kubectl apply -k config/default
```

Render the sample resources with:

```bash
kubectl kustomize config/samples
```
