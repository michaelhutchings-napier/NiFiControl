package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func authTestCluster(auth *nifiv1alpha1.NiFiClusterAuthenticationSpec) *nifiv1alpha1.NiFiCluster {
	return &nifiv1alpha1.NiFiCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "edge", Namespace: "default"},
		Spec: nifiv1alpha1.NiFiClusterSpec{
			Mode:           nifiv1alpha1.ClusterModeInternal,
			InternalTLS:    &nifiv1alpha1.NiFiClusterInternalTLSSpec{Enabled: true},
			Authentication: auth,
		},
	}
}

func envByName(env []corev1.EnvVar, name string) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == name {
			return &env[i]
		}
	}
	return nil
}

func TestResolveSingleUserAuthentication(t *testing.T) {
	credentials := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "admin-creds", Namespace: "default"},
		Data:       map[string][]byte{"username": []byte("admin"), "password": []byte("supersecurepassword")},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode:       "SingleUser",
		SingleUser: &nifiv1alpha1.NiFiClusterSingleUserAuthSpec{CredentialsSecretRef: corev1.LocalObjectReference{Name: "admin-creds"}},
	})
	resolved, err := resolveClusterAuthentication(context.Background(), overridesTestClient(credentials), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if provider := envByName(resolved.env, "NIFI_LOGIN_IDENTITY_PROVIDER"); provider == nil || provider.Value != "single-user-provider" {
		t.Fatalf("expected the single-user login provider, got %+v", provider)
	}
	password := envByName(resolved.env, "NIFI_SINGLE_USER_PASSWORD")
	if password == nil || password.ValueFrom == nil || password.ValueFrom.SecretKeyRef == nil || password.ValueFrom.SecretKeyRef.Name != "admin-creds" {
		t.Fatalf("expected the password to come from the credentials Secret, got %+v", password)
	}
	if len(resolved.secretData) != 0 {
		t.Fatal("single-user mode should not render an auth Secret payload")
	}
	if resolved.checksum == "" {
		t.Fatal("expected a checksum")
	}

	// Rotating the password must change the checksum so the nodes roll.
	credentials.Data["password"] = []byte("anotherlongpassword")
	rotated, err := resolveClusterAuthentication(context.Background(), overridesTestClient(credentials), cluster)
	if err != nil || rotated.checksum == resolved.checksum {
		t.Fatalf("expected credential rotation to change the checksum: %v", err)
	}

	// NiFi rejects passwords shorter than 12 characters; fail early with a clear error.
	credentials.Data["password"] = []byte("short")
	if _, err := resolveClusterAuthentication(context.Background(), overridesTestClient(credentials), cluster); err == nil {
		t.Fatal("expected a short password to be rejected")
	}
}

func TestResolveLDAPAuthenticationRendersProvider(t *testing.T) {
	managerPassword := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ldap-manager", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte(`s3cret<&>"pass`)},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode: "LDAP",
		LDAP: &nifiv1alpha1.NiFiClusterLDAPAuthSpec{
			URL:                      "ldap://openldap.auth.svc:389",
			ManagerDN:                "cn=admin,dc=example,dc=org",
			ManagerPasswordSecretRef: nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "ldap-manager"}, Key: "password"}},
			UserSearchBase:           "ou=people,dc=example,dc=org",
			UserSearchFilter:         "(uid={0})",
		},
	})
	resolved, err := resolveClusterAuthentication(context.Background(), overridesTestClient(managerPassword), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if provider := envByName(resolved.env, "NIFI_LOGIN_IDENTITY_PROVIDER"); provider == nil || provider.Value != "ldap-provider" {
		t.Fatalf("expected the ldap login provider, got %+v", provider)
	}
	document := string(resolved.secretData[authLoginProvidersKey])
	if !strings.Contains(document, "<property name=\"Url\">ldap://openldap.auth.svc:389</property>") {
		t.Fatalf("expected the LDAP URL in the rendered provider, got %s", document)
	}
	// XML-sensitive characters in the manager password must be escaped.
	if !strings.Contains(document, "s3cret&lt;&amp;&gt;&#34;pass") && !strings.Contains(document, "s3cret&lt;&amp;&gt;&quot;pass") {
		t.Fatalf("expected the manager password to be XML-escaped, got %s", document)
	}
	// Defaults are filled in.
	if !strings.Contains(document, ">SIMPLE<") || !strings.Contains(document, ">USE_USERNAME<") {
		t.Fatalf("expected defaulted strategy and identity strategy, got %s", document)
	}
}

func TestResolveOIDCAuthentication(t *testing.T) {
	clientSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oidc-client", Namespace: "default"},
		Data:       map[string][]byte{"secret": []byte("client-secret-value")},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode: "OIDC",
		OIDC: &nifiv1alpha1.NiFiClusterOIDCAuthSpec{
			DiscoveryURL:    "https://idp.example.com/.well-known/openid-configuration",
			ClientID:        "nifi",
			ClientSecretRef: nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "oidc-client"}, Key: "secret"}},
		},
	})
	resolved, err := resolveClusterAuthentication(context.Background(), overridesTestClient(clientSecret), cluster)
	if err != nil {
		t.Fatal(err)
	}
	// OIDC has no login provider; it is configured purely through nifi.properties.
	if provider := envByName(resolved.env, "NIFI_LOGIN_IDENTITY_PROVIDER"); provider != nil {
		t.Fatalf("expected no login provider for OIDC, got %+v", provider)
	}
	if claim := envByName(resolved.env, "NIFI_OIDC_CLAIM_IDENTIFYING_USER"); claim == nil || claim.Value != "email" {
		t.Fatalf("expected the claim to default to email, got %+v", claim)
	}
	secret := envByName(resolved.env, "NIFI_OIDC_CLIENT_SECRET")
	if secret == nil || secret.ValueFrom == nil || secret.ValueFrom.SecretKeyRef == nil || secret.ValueFrom.SecretKeyRef.Key != "secret" {
		t.Fatalf("expected the client secret to come from the Secret, got %+v", secret)
	}
	if len(resolved.secretData) != 0 {
		t.Fatal("OIDC mode should not render an auth Secret payload")
	}
}

func TestDesiredStatefulSetWiresAuthentication(t *testing.T) {
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{Mode: "LDAP"})
	auth := &resolvedClusterAuth{
		env:        []corev1.EnvVar{{Name: "NIFI_LOGIN_IDENTITY_PROVIDER", Value: "ldap-provider"}},
		secretData: map[string][]byte{authLoginProvidersKey: []byte("<xml/>")},
		checksum:   "auth-checksum",
	}
	tls := &clusterTLSMaterials{httpsPort: 8443}
	spec := desiredManagedClusterStatefulSetSpec(cluster, tls, "", auth)

	if spec.Template.Annotations[managedAuthChecksumAnnotation] != "auth-checksum" {
		t.Fatal("expected the auth checksum pod-template annotation")
	}
	env := spec.Template.Spec.Containers[0].Env
	if provider := envByName(env, "NIFI_LOGIN_IDENTITY_PROVIDER"); provider == nil || provider.Value != "ldap-provider" {
		t.Fatalf("expected the auth env on the NiFi container, got %+v", provider)
	}
	if envByName(env, "NIFI_AUTH_DIR") == nil {
		t.Fatal("expected NIFI_AUTH_DIR on the NiFi container")
	}
	foundVolume := false
	for _, volume := range spec.Template.Spec.Volumes {
		if volume.Name == managedAuthVolume {
			foundVolume = true
			if volume.Secret == nil || volume.Secret.SecretName != managedClusterAuthSecretName(cluster) {
				t.Fatalf("auth volume does not reference the auth Secret: %+v", volume)
			}
		}
	}
	if !foundVolume {
		t.Fatal("expected the auth Secret volume")
	}
	foundMount := false
	for _, mount := range spec.Template.Spec.Containers[0].VolumeMounts {
		if mount.Name == managedAuthVolume && mount.MountPath == managedAuthDir && mount.ReadOnly {
			foundMount = true
		}
	}
	if !foundMount {
		t.Fatal("expected the auth volume mount")
	}

	// The TLS start command consumes the auth environment.
	for _, needle := range []string{"NIFI_LOGIN_IDENTITY_PROVIDER", "NIFI_OIDC_DISCOVERY_URL", "set-single-user-credentials", "login-identity-providers.xml"} {
		if !strings.Contains(managedNiFiStartCommandTLS, needle) {
			t.Fatalf("TLS start command is missing %s", needle)
		}
	}
}

func TestReconcileManagedClusterAuthSecretLifecycle(t *testing.T) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(nifiv1alpha1.AddToScheme(scheme))
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{Mode: "LDAP"})
	k8sClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cluster).Build()
	reconciler := &NiFiClusterReconciler{Client: k8sClient, Scheme: scheme}
	ctx := context.Background()

	auth := &resolvedClusterAuth{secretData: map[string][]byte{authLoginProvidersKey: []byte("<xml/>")}}
	if err := reconciler.reconcileManagedClusterAuthSecret(ctx, cluster, auth); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	key := types.NamespacedName{Name: managedClusterAuthSecretName(cluster), Namespace: cluster.Namespace}
	if err := k8sClient.Get(ctx, key, secret); err != nil {
		t.Fatal(err)
	}
	if string(secret.Data[authLoginProvidersKey]) != "<xml/>" {
		t.Fatalf("unexpected auth Secret content: %v", secret.Data)
	}

	// A mode without a rendered payload removes the Secret.
	if err := reconciler.reconcileManagedClusterAuthSecret(ctx, cluster, &resolvedClusterAuth{}); err != nil {
		t.Fatal(err)
	}
	if err := k8sClient.Get(ctx, key, secret); !apierrors.IsNotFound(err) {
		t.Fatalf("expected the auth Secret to be deleted, got %v", err)
	}
	if err := reconciler.reconcileManagedClusterAuthSecret(ctx, cluster, nil); err != nil {
		t.Fatal(err)
	}
}
