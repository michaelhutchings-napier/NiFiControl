package certmanager

import (
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func TestNewCertificateSetsGVKAndSpec(t *testing.T) {
	obj, err := NewCertificate("server", "dataflows", CertificateSpec{
		SecretName: "server-tls",
		CommonName: "production-node",
		DNSNames:   []string{"*.production-nifi-headless.dataflows.svc"},
		Usages:     []string{UsageServerAuth, UsageClientAuth},
		IssuerRef:  IssuerRef{Name: "production-ca", Kind: KindIssuer, Group: GroupName},
		Keystores:  &Keystores{PKCS12: &PKCS12Keystore{Create: true, PasswordSecretRef: SecretKeySelector{Name: "pw", Key: "password"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if obj.GetObjectKind().GroupVersionKind() != CertificateGVK {
		t.Fatalf("gvk = %v, want %v", obj.GetObjectKind().GroupVersionKind(), CertificateGVK)
	}
	if obj.GetName() != "server" || obj.GetNamespace() != "dataflows" {
		t.Fatalf("name/namespace = %q/%q", obj.GetName(), obj.GetNamespace())
	}
	secretName, _, _ := unstructured.NestedString(obj.Object, "spec", "secretName")
	if secretName != "server-tls" {
		t.Fatalf("secretName = %q", secretName)
	}
	usages, _, _ := unstructured.NestedStringSlice(obj.Object, "spec", "usages")
	if len(usages) != 2 || usages[0] != UsageServerAuth || usages[1] != UsageClientAuth {
		t.Fatalf("usages = %v", usages)
	}
	create, found, _ := unstructured.NestedBool(obj.Object, "spec", "keystores", "pkcs12", "create")
	if !found || !create {
		t.Fatalf("pkcs12.create = %v (found=%v)", create, found)
	}
	issuerKind, _, _ := unstructured.NestedString(obj.Object, "spec", "issuerRef", "kind")
	if issuerKind != KindIssuer {
		t.Fatalf("issuerRef.kind = %q", issuerKind)
	}
}

func TestNewIssuerModes(t *testing.T) {
	selfSigned, err := NewIssuer("ca-root", "dataflows", IssuerSpec{SelfSigned: &SelfSignedIssuer{}})
	if err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedMap(selfSigned.Object, "spec", "selfSigned"); !found {
		t.Fatal("expected spec.selfSigned")
	}

	ca, err := NewIssuer("ca", "dataflows", IssuerSpec{CA: &CAIssuer{SecretName: "ca-tls"}})
	if err != nil {
		t.Fatal(err)
	}
	secretName, _, _ := unstructured.NestedString(ca.Object, "spec", "ca", "secretName")
	if secretName != "ca-tls" {
		t.Fatalf("ca.secretName = %q", secretName)
	}
}

func TestCertificateReady(t *testing.T) {
	obj := New(CertificateGVK)
	if ready, _ := CertificateReady(obj); ready {
		t.Fatal("certificate without status should not be ready")
	}
	_ = unstructured.SetNestedSlice(obj.Object, []any{
		map[string]any{"type": "Ready", "status": "True", "message": "Certificate is up to date and has not expired"},
	}, "status", "conditions")
	ready, message := CertificateReady(obj)
	if !ready {
		t.Fatal("certificate with Ready=True should be ready")
	}
	if message == "" {
		t.Fatal("expected a condition message")
	}
}

func TestIsCRDNotInstalled(t *testing.T) {
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: GroupName, Kind: KindCertificate}}
	if !IsCRDNotInstalled(noMatch) {
		t.Fatal("NoKindMatchError should report CRD not installed")
	}
	objectNotFound := apierrors.NewNotFound(schema.GroupResource{Group: "", Resource: "secrets"}, "server-tls")
	if IsCRDNotInstalled(objectNotFound) {
		t.Fatal("a named object NotFound must not be treated as a missing CRD")
	}
	if IsCRDNotInstalled(nil) {
		t.Fatal("nil error is not a missing CRD")
	}
}
