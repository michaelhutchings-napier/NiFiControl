# Parameter providers

A **parameter provider** is a NiFi 2.x controller-level extension that sources parameter values from
outside NiFi — environment variables, files on disk, or a cloud secret manager (AWS Secrets Manager,
AWS Systems Manager Parameter Store, HashiCorp Vault via a provider NAR, and so on). Sensitive
parameters can then be owned, rotated, and audited in that external system instead of being pasted
into NiFi (or into a CR).

`NiFiParameterProvider` declares one provider and reconciles it against a running cluster over the
NiFi REST API. Like `NiFiReportingTask` it is a controller-scoped component (no parent process
group), and like every NiFi-resident kind it honours `deletionPolicy`, `driftPolicy`, and
`adoptionPolicy`.

## Spec

| Field | Meaning |
| --- | --- |
| `clusterRef` | The `NiFiCluster` (or external cluster) to configure the provider in. |
| `type` | Fully qualified provider class, e.g. `org.apache.nifi.parameter.EnvironmentVariableParameterProvider`. |
| `bundle` | Optional NAR bundle (`group`/`artifact`/`version`) pinning `type`; omit to let NiFi resolve a single matching bundle. |
| `properties` | Non-sensitive provider properties. |
| `sensitiveProperties` | Provider properties whose values come from Kubernetes Secrets (`secretKeyRef`), so credentials stay out of the resource. |
| `deletionPolicy` / `driftPolicy` / `adoptionPolicy` | Shared ownership model (see [docs/README.md](README.md)). |

## Example: environment variables

The `EnvironmentVariableParameterProvider` ships with NiFi and needs no external service, so it is the
simplest way to see the kind at work:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterProvider
metadata:
  name: env
spec:
  clusterRef: {name: production}
  type: org.apache.nifi.parameter.EnvironmentVariableParameterProvider
  properties:
    parameter-group-name: environment
  deletionPolicy: Delete
```

Every environment variable becomes a parameter in the named group. To narrow that, set
`environment-variable-inclusion-strategy` to `include-all` / `comma-separated` / `regex`.

> **Property keys are NiFi's internal descriptor names, not UI display names.** Use
> `parameter-group-name`, not `"Parameter Group Name"`; use the allowable *value* `include-all`, not
> the display text `"Include All"`. A display name is stored as an unknown *dynamic* property, and an
> unrecognised allowable value fails validation — either leaves the provider `INVALID`. Look up a
> provider's exact property names in the NiFi documentation or the component's UI configuration
> dialog (the property tooltip shows the internal name).

## Example: an external secret manager

Credentials for a cloud provider come from a Kubernetes Secret via `sensitiveProperties`, so the CR
itself carries no secret material. The property keys below are illustrative — confirm each provider's
exact internal property names per the note above:

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterProvider
metadata:
  name: aws-secrets
spec:
  clusterRef: {name: production}
  type: org.apache.nifi.parameter.aws.AwsSecretsManagerParameterProvider
  bundle:
    group: org.apache.nifi
    artifact: nifi-aws-nar
  properties:
    region: eu-west-2
  sensitiveProperties:
    access-key:
      secretKeyRef: {name: aws-creds, key: access-key-id}
    secret-key:
      secretKeyRef: {name: aws-creds, key: secret-access-key}
  deletionPolicy: Delete
```

The operator resolves each `secretKeyRef` before configuring the provider; until a referenced Secret
(or key) exists the resource reports `WaitingForDependencies` rather than pushing an empty value.

## Fetching parameters into a context

Configuring the provider is one step; **fetching** its parameter groups and **applying** them to a
`NiFiParameterContext` is a separate NiFi action (`Fetch Parameters` in the UI, or the
`/parameter-providers/{id}/parameters/fetch-requests` and `apply-parameters-requests` API). The
operator does not trigger fetch/apply automatically today — it keeps the provider itself declarative
and in sync. Reference the resulting parameter context from your process groups as usual. Automated
fetch/apply is a possible future enhancement; open an issue if you need it.

## Validation and status

`.status.validationStatus` mirrors NiFi's own view (`VALID` / `VALIDATING` / `INVALID`). An `INVALID`
provider is usually a missing required property or an unresolved bundle — check
`.status.conditions[Ready]` and the operator logs for the specific validation error.
