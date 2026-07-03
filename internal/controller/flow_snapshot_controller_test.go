package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func srcRef(name, key string) *nifiv1alpha1.SecretKeyRef {
	return &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

func TestResolvedFlowArtifactCredentialsReadsSecrets(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "src-creds", Namespace: "default"},
		Data: map[string][]byte{
			"username": []byte("git-user"),
			"password": []byte("git-pass"),
			"token":    []byte("  bearer-token\n"),
			"ca.crt":   []byte("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n"),
		},
	}
	c := fake.NewClientBuilder().WithScheme(testScheme()).WithObjects(secret).Build()

	t.Run("git username/password with CA and insecure", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
			URL: "https://git.example.com/flows.git",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{
				UsernameSecretKeyRef: srcRef("src-creds", "username"),
				PasswordSecretKeyRef: srcRef("src-creds", "password"),
				CASecretKeyRef:       srcRef("src-creds", "ca.crt"),
				InsecureSkipVerify:   true,
			},
		}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if creds.Username != "git-user" || creds.Password != "git-pass" {
			t.Fatalf("username/password = %q/%q", creds.Username, creds.Password)
		}
		if len(creds.CAData) == 0 {
			t.Fatal("CA data was not resolved")
		}
		if !creds.InsecureSkipVerify {
			t.Fatal("InsecureSkipVerify was not carried through")
		}
		if creds.Token != "" {
			t.Fatalf("token should be empty, got %q", creds.Token)
		}
	})

	t.Run("oci token is trimmed", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{
			Image:       "registry.example.com/flows:v1",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{TokenSecretKeyRef: srcRef("src-creds", "token")},
		}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if creds.Token != "bearer-token" {
			t.Fatalf("token = %q, want trimmed 'bearer-token'", creds.Token)
		}
	})

	t.Run("no credentials yields empty", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{URL: "https://git.example.com/flows.git"}}
		creds, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source)
		if err != nil {
			t.Fatal(err)
		}
		if creds.Username != "" || creds.Password != "" || creds.Token != "" || len(creds.CAData) != 0 || creds.InsecureSkipVerify {
			t.Fatalf("expected zero credentials, got %#v", creds)
		}
	})

	t.Run("missing secret is an error", func(t *testing.T) {
		source := &nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
			BucketID: "b", FlowID: "f",
			Credentials: &nifiv1alpha1.FlowArtifactCredentials{TokenSecretKeyRef: srcRef("absent", "token")},
		}}
		if _, err := resolvedFlowArtifactCredentials(context.Background(), c, "default", source); err == nil {
			t.Fatal("expected an error resolving a missing credential secret")
		}
	})
}

func TestFlowSourceReferencesSecret(t *testing.T) {
	git := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{
		Credentials: &nifiv1alpha1.FlowArtifactCredentials{UsernameSecretKeyRef: srcRef("creds", "username")},
	}}
	oci := &nifiv1alpha1.FlowBundleSource{OCI: &nifiv1alpha1.OCISource{
		Credentials: &nifiv1alpha1.FlowArtifactCredentials{TokenSecretKeyRef: srcRef("creds", "token")},
	}}
	registry := &nifiv1alpha1.FlowBundleSource{Registry: &nifiv1alpha1.RegistryFlowSource{
		Credentials: &nifiv1alpha1.FlowArtifactCredentials{CASecretKeyRef: srcRef("creds", "ca.crt")},
	}}
	noCreds := &nifiv1alpha1.FlowBundleSource{Git: &nifiv1alpha1.GitSource{URL: "https://git.example.com/flows.git"}}

	for _, tc := range []struct {
		name   string
		source *nifiv1alpha1.FlowBundleSource
		secret string
		want   bool
	}{
		{"git matches", git, "creds", true},
		{"git wrong name", git, "other", false},
		{"oci token matches", oci, "creds", true},
		{"registry ca matches", registry, "creds", true},
		{"source without credentials", noCreds, "creds", false},
		{"nil source", nil, "creds", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := flowSourceReferencesSecret(tc.source, tc.secret); got != tc.want {
				t.Fatalf("flowSourceReferencesSecret = %v, want %v", got, tc.want)
			}
		})
	}
}
