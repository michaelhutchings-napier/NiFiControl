# Parameter Providers

`NiFiParameterProvider` manages a controller-level NiFi parameter provider.
Use it when values should be fetched into parameter contexts from an external
source.

## Example

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiParameterProvider
metadata:
  name: env
spec:
  clusterRef:
    name: production
  type: org.apache.nifi.parameter.EnvironmentVariableParameterProvider
  properties:
    Include Environment Variables: "DB_.*"
  state: Enabled
```

Fetch parameters from a provider in `NiFiParameterContext`:

```yaml
spec:
  parameterProviderRefs:
    - name: env
```

## Notes

- Sensitive properties should use Secret refs.
- Provider status records the NiFi id and revision.
- Fetching parameters is separate from creating the provider.
