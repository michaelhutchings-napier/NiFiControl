# NiFiControl

NiFiControl is a declarative Kubernetes control plane for Apache NiFi.

The project is starting with a clean API surface under `nifi.controlnifi.io`
and a Kubernetes-native model for clusters, registry clients, parameter
contexts, controller services, and high-level flow deployments.

## Initial Scope

- Phase 1 API types compile.
- Controller manager starts with stub reconcilers.
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
