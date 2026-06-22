// Package certmanager provides a minimal, dependency-free typed surface over the
// cert-manager.io/v1 Certificate and Issuer resources. NiFiControl deliberately avoids
// importing the upstream cert-manager Go module because its pinned k8s.io versions
// conflict with this project's. Instead, small typed structs are converted to
// *unstructured.Unstructured for apply, which also lets the operator detect a missing
// cert-manager installation gracefully (no REST mapping) rather than crashing.
package certmanager

import (
	"errors"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	// GroupName is the cert-manager API group.
	GroupName = "cert-manager.io"
	// Version is the cert-manager API version NiFiControl targets.
	Version = "v1"

	KindCertificate   = "Certificate"
	KindIssuer        = "Issuer"
	KindClusterIssuer = "ClusterIssuer"

	// Standard cert-manager key usages.
	UsageServerAuth       = "server auth"
	UsageClientAuth       = "client auth"
	UsageDigitalSignature = "digital signature"
	UsageKeyEncipherment  = "key encipherment"
)

// GroupVersion identifies the cert-manager API surface used by NiFiControl.
var GroupVersion = schema.GroupVersion{Group: GroupName, Version: Version}

// GroupVersionKind helpers for the resources NiFiControl reconciles.
var (
	CertificateGVK   = GroupVersion.WithKind(KindCertificate)
	IssuerGVK        = GroupVersion.WithKind(KindIssuer)
	ClusterIssuerGVK = GroupVersion.WithKind(KindClusterIssuer)
)

// IssuerRef references a cert-manager Issuer or ClusterIssuer.
type IssuerRef struct {
	Name  string `json:"name"`
	Kind  string `json:"kind,omitempty"`
	Group string `json:"group,omitempty"`
}

// SecretKeySelector references a key within a Secret.
type SecretKeySelector struct {
	Name string `json:"name"`
	Key  string `json:"key,omitempty"`
}

// IssuerSpec is the subset of cert-manager IssuerSpec NiFiControl sets. Exactly one of
// SelfSigned or CA is populated.
type IssuerSpec struct {
	SelfSigned *SelfSignedIssuer `json:"selfSigned,omitempty"`
	CA         *CAIssuer         `json:"ca,omitempty"`
}

// SelfSignedIssuer marks an Issuer that signs certificates with their own key.
type SelfSignedIssuer struct{}

// CAIssuer signs certificates with a CA key pair stored in SecretName.
type CAIssuer struct {
	SecretName string `json:"secretName"`
}

// PrivateKey configures the generated private key.
type PrivateKey struct {
	Algorithm      string `json:"algorithm,omitempty"`
	Encoding       string `json:"encoding,omitempty"`
	Size           int    `json:"size,omitempty"`
	RotationPolicy string `json:"rotationPolicy,omitempty"`
}

// PKCS12Keystore requests a PKCS12 keystore.p12 and truststore.p12 in the issued Secret.
type PKCS12Keystore struct {
	Create            bool              `json:"create"`
	PasswordSecretRef SecretKeySelector `json:"passwordSecretRef"`
}

// Keystores configures additional keystore formats in the issued Secret.
type Keystores struct {
	PKCS12 *PKCS12Keystore `json:"pkcs12,omitempty"`
}

// CertificateSpec is the subset of cert-manager CertificateSpec NiFiControl sets.
type CertificateSpec struct {
	SecretName  string      `json:"secretName"`
	CommonName  string      `json:"commonName,omitempty"`
	Duration    string      `json:"duration,omitempty"`
	RenewBefore string      `json:"renewBefore,omitempty"`
	IsCA        bool        `json:"isCA,omitempty"`
	DNSNames    []string    `json:"dnsNames,omitempty"`
	IPAddresses []string    `json:"ipAddresses,omitempty"`
	Usages      []string    `json:"usages,omitempty"`
	IssuerRef   IssuerRef   `json:"issuerRef"`
	PrivateKey  *PrivateKey `json:"privateKey,omitempty"`
	Keystores   *Keystores  `json:"keystores,omitempty"`
}

// NewIssuer builds an unstructured namespaced cert-manager Issuer.
func NewIssuer(name, namespace string, spec IssuerSpec) (*unstructured.Unstructured, error) {
	return newObject(IssuerGVK, name, namespace, &spec)
}

// NewCertificate builds an unstructured namespaced cert-manager Certificate.
func NewCertificate(name, namespace string, spec CertificateSpec) (*unstructured.Unstructured, error) {
	return newObject(CertificateGVK, name, namespace, &spec)
}

// New returns an empty unstructured object of the given kind, suitable as a target for
// client.Get or controllerutil.CreateOrUpdate.
func New(gvk schema.GroupVersionKind) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	return obj
}

func newObject(gvk schema.GroupVersionKind, name, namespace string, spec any) (*unstructured.Unstructured, error) {
	specMap, err := runtime.DefaultUnstructuredConverter.ToUnstructured(spec)
	if err != nil {
		return nil, fmt.Errorf("convert %s spec: %w", gvk.Kind, err)
	}
	obj := New(gvk)
	obj.SetName(name)
	if namespace != "" {
		obj.SetNamespace(namespace)
	}
	if err := unstructured.SetNestedMap(obj.Object, specMap, "spec"); err != nil {
		return nil, fmt.Errorf("set %s spec: %w", gvk.Kind, err)
	}
	return obj, nil
}

// IsCRDNotInstalled reports whether err indicates the cert-manager CRDs are absent from
// the cluster (no REST mapping for the kind). The operator surfaces this as a clear
// status rather than treating it as a transient failure.
func IsCRDNotInstalled(err error) bool {
	if err == nil {
		return false
	}
	if meta.IsNoMatchError(err) {
		return true
	}
	// A scheme-backed client (notably the fake client used in tests) reports an
	// unregistered kind rather than a REST-mapping miss; treat it the same way.
	if runtime.IsNotRegisteredError(err) {
		return true
	}
	// The discovery client reports a missing resource type as a NotFound on the
	// GroupResource; an object NotFound carries a resource name, which lets us tell the
	// two apart.
	if apierrors.IsNotFound(err) {
		var statusErr *apierrors.StatusError
		if errors.As(err, &statusErr) {
			details := statusErr.ErrStatus.Details
			return details != nil && details.Name == "" && details.Kind != ""
		}
	}
	return false
}

// CertificateReady reports whether a fetched Certificate object has a Ready condition of
// True, returning the condition message for diagnostics.
func CertificateReady(obj *unstructured.Unstructured) (bool, string) {
	conditions, found, err := unstructured.NestedSlice(obj.Object, "status", "conditions")
	if err != nil || !found {
		return false, ""
	}
	for _, raw := range conditions {
		condition, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if condition["type"] != "Ready" {
			continue
		}
		message, _ := condition["message"].(string)
		return condition["status"] == "True", message
	}
	return false, ""
}
