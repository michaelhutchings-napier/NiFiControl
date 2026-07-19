# Authorization (tenants and access policies)

NiFiControl manages NiFi authorization declaratively with three resources:

- **`NiFiUser`** — a NiFi *user tenant*, identified by a client-certificate subject (DN) or
  OIDC identity.
- **`NiFiUserGroup`** — a *group tenant* whose members are `NiFiUser` resources.
- **`NiFiPolicy`** — an *access policy*: a `(resource, action)` tuple granted to a set of user
  and group tenants.

> **Secured NiFi only.** Tenants and access policies exist only on a NiFi with a *managed
> authorizer* — i.e. a secured (TLS) cluster. Enable `spec.internalTLS` on the `NiFiCluster`;
> the operator's client certificate is the initial admin and can manage tenants and policies.
> On an insecure (HTTP) cluster these resources have nothing to act on and stay not-ready.

## How it fits together

```
NiFiUser (identity)  ─┐
                      ├─►  NiFiPolicy (resource, action)  ──►  NiFi access policy
NiFiUserGroup (users) ┘
```

`NiFiPolicy` references tenants by resource name (`userRefs` / `userGroupRefs`), not by raw
identity, so the operator resolves each to its NiFi tenant id and waits until the referenced
`NiFiUser`/`NiFiUserGroup` is `Ready`. This keeps identities defined once and reused across
policies. Each `NiFiPolicy` owns only the grants it declares; it preserves other tenants already
present on the same NiFi access policy, including NiFi's seeded initial-admin grants.

### NiFiUser

The operator resolves the tenant by its recorded id, then by identity (adopting an existing
NiFi user with the same identity), else creates it; it renames the tenant if the identity
changes, and records the NiFi id in `status.nifiId`.

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata: {name: metrics-scraper}
spec:
  clusterRef: {name: production}
  identity: "CN=prometheus, O=monitoring"
```

### NiFiUserGroup

Members are `NiFiUser` references; the group is reconciled once its members are `Ready`, and
their resolved tenant ids are recorded in `status.memberIds`.

### NiFiPolicy

`resource` is a NiFi resource path (`/flow`, `/controller`, `/proxy`,
`/data/process-groups/{id}`, `/policies`, `/tenants`, …) and `action` is `read` or `write`. The
operator finds the exact policy for that `(resource, action)` — distinguishing it from an
inherited/effective policy NiFi may return for a component resource — and creates it if absent,
then ensures the declared users and groups are granted.

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: scraper-read-flow}
spec:
  clusterRef: {name: production}
  resource: /flow
  action: read
  userRefs:
    - {name: metrics-scraper}
```

## Use case: authorize the metrics scrape identity

On a TLS cluster, NiFi's Prometheus endpoint (`/nifi-api/flow/metrics/prometheus`) requires an
authenticated, authorized caller (see [docs/observability.md](observability.md)). Grant the
scrape identity read access with a `NiFiUser` for its certificate DN plus a `NiFiPolicy` for
`/flow` read — exactly the sample in `config/samples/nifi_v1alpha1_nifipolicy.yaml`.

## Use case: authorize a remote cluster for site-to-site

On a secured cluster, a remote NiFi must be authorized before it can pull site-to-site details or
transfer data — trust (a shared/known CA) alone is not enough. The sending cluster connects with its
**node identity** (`CN=<cluster>-node` for a managed cluster), so grant that identity the two NiFi
site-to-site policies on the *receiving* cluster:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiUser
metadata: {name: edge-node}
spec:
  clusterRef: {name: central}
  identity: "CN=edge-node"          # the sending cluster's node certificate subject
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: edge-s2s-details}
spec:
  clusterRef: {name: central}
  resource: /site-to-site           # "retrieve site-to-site details"
  action: read
  userRefs: [{name: edge-node}]
---
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiPolicy
metadata: {name: edge-receive}
spec:
  clusterRef: {name: central}
  # "receive data via site-to-site" for one input port (use the port's status.nifiId).
  # For an output port the sender reads from, use /data-transfer/output-ports/{id}.
  resource: /data-transfer/input-ports/<from-edge-port-id>
  action: write
  userRefs: [{name: edge-node}]
```

The remote process group on the sending cluster will only discover the target port once both grants
exist. `hack/test-remoteprocessgroup-sitetosite-tls-kind.sh` exercises this end to end over mutual
TLS.

## Deletion

With `deletionPolicy: Delete`, removing a `NiFiPolicy` removes only that resource's recorded user
and group grants from the NiFi access policy, preserving unrelated tenants. If no tenants remain,
the empty access policy is deleted. Removing a `NiFiUser`/`NiFiUserGroup` deletes the corresponding
NiFi tenant. The default `Orphan` leaves NiFi state in place.
