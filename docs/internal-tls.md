# Internal TLS

NiFiControl can run managed NiFi clusters over HTTPS with mutual TLS. It requires
cert-manager for generated certificates.

## Providers

Choose one provider under `spec.internalTLS`:

| Provider | Use |
| --- | --- |
| `selfSigned` | Development or simple internal clusters. |
| `issuerRef` | Existing cert-manager Issuer or ClusterIssuer. |
| external PKCS12 Secrets | Bring your own keystore/truststore. |

## Self-Signed Example

```yaml
apiVersion: nifi.controlnifi.io/v1alpha1
kind: NiFiCluster
metadata:
  name: secure
spec:
  mode: Internal
  image: apache/nifi:2.10.0
  replicas: 1
  internalTLS:
    enabled: true
    selfSigned: {}
```

## What the Operator Does

- Creates server/node and operator client certificates.
- Configures NiFi HTTPS, keystore, truststore, and initial admin identity.
- Grants the operator enough root process group access to manage the canvas.
- Rolls pods when certificate material changes.
- Reports `TLSReady=False` if cert-manager CRDs are missing.

## Options

- `autoReload.enabled`: lets NiFi reload rotated leaf certs in place.
- `perNodeCertificates.enabled`: gives each node its own cert and in-pod private key.
- `httpsPort`: defaults to `8443`.

Managed internal TLS never uses `insecureSkipVerify`.
