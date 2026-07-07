package controller

import (
	"context"
	"strings"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// testCAPEM is a throwaway self-signed CA (CN=Test Private CA), used only to exercise the
// operator's CA parsing/validation and truststore wiring. It signs nothing real.
const testCAPEM = `-----BEGIN CERTIFICATE-----
MIIDFTCCAf2gAwIBAgIUG15INyle8nP4q27pK0ImJE2N+HAwDQYJKoZIhvcNAQEL
BQAwGjEYMBYGA1UEAwwPVGVzdCBQcml2YXRlIENBMB4XDTI2MDcwNzE0MTEzOFoX
DTI2MDcwODE0MTEzOFowGjEYMBYGA1UEAwwPVGVzdCBQcml2YXRlIENBMIIBIjAN
BgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAs9tof5jsEa+tk58lb9Wj+6mpIkzU
MALmsByBObYwfcJuEWnzFaPAIqBtsumd9deuvZFiYzx3+m5J4DjdVZY4FM0b08US
/pN/ZXamREwESatZN6HYF0l/ZOWe7IxzkJqejdtj1gQfIYrqcmWq3oE5DE1pfmsL
xqxSfcXMNpqMwCzMat4N+G7NP5BqAhHtFWaX4McbTMwkwmkX/1E5JaC6w52Hs0qB
m5byV1/YJcWJ+qplQmiR4zgrJCrX2DoblFBdNIqZcgie687u4q0p6DZgrbJ4OHFM
W+Dz55mMAaxmZjNH34SpE/kwbGlRlQDKNCqomSIptHhmGpGtFsIXomY5bQIDAQAB
o1MwUTAdBgNVHQ4EFgQUaXQbwyfKJd7gZ5bLeTGKMh23wDQwHwYDVR0jBBgwFoAU
aXQbwyfKJd7gZ5bLeTGKMh23wDQwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0B
AQsFAAOCAQEAdalOgVXkrfT2ACk/vv6ERDldmTXmHkVV2RZjyCX6+oSEZJ5VHpQI
vLSEva467K6T1pguvPqgCje7OgMRmqrmz9rif4VNwkK63SAUlWkEAKv4L5n3Y6Hr
Uk8/DSLEw2sYcEZa3eFQeX1y03iANriUscXEFctCGcaqNbcG5gke1B6JXDUmrQCC
JRLeCKh58UMSl/ZD292yTvc2HjMMpVu4oeT6k6wGSiHWJKV077NSlWD4mWrr1aFN
chTagt8oXalHTwVnxmGomHNnSHsp//UPUlcWw/IX8GQ0SBilHMFWKz8zZUWZ+NvQ
8zzMNG1/tonNrB6tGk65Y+HwDBIBNRtgzw==
-----END CERTIFICATE-----
`

func caSecretRef(name, key string) *nifiv1alpha1.SecretKeyRef {
	ref := &nifiv1alpha1.SecretKeyRef{}
	ref.Name = name
	ref.Key = key
	return ref
}

func TestResolveLDAPAuthenticationWithPrivateCA(t *testing.T) {
	managerPassword := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "ldap-manager", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("secret")},
	}
	ca := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "corp-ca", Namespace: "default"},
		Data:       map[string][]byte{"ca.crt": []byte(testCAPEM)},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode: "LDAP",
		LDAP: &nifiv1alpha1.NiFiClusterLDAPAuthSpec{
			URL:                      "ldaps://ldap.corp:636",
			AuthenticationStrategy:   "LDAPS",
			ManagerDN:                "cn=admin,dc=corp",
			ManagerPasswordSecretRef: nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "ldap-manager"}, Key: "password"}},
			UserSearchBase:           "ou=people,dc=corp",
			UserSearchFilter:         "(uid={0})",
			CASecretRef:              caSecretRef("corp-ca", "ca.crt"),
		},
	})

	withCA, err := resolveClusterAuthentication(context.Background(), overridesTestClient(managerPassword, ca), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if string(withCA.secretData[authCAKey]) != testCAPEM {
		t.Fatal("expected the CA PEM in the auth payload Secret")
	}
	document := string(withCA.secretData[authLoginProvidersKey])
	if !strings.Contains(document, "<property name=\"TLS - Truststore\">"+ldapTruststorePath+"</property>") {
		t.Fatalf("expected the LDAP provider to point at the built truststore, got %s", document)
	}
	if !strings.Contains(document, "<property name=\"TLS - Truststore Type\">PKCS12</property>") {
		t.Fatal("expected the LDAP truststore type PKCS12")
	}

	// Without the CA, the truststore properties stay empty and the payload has no CA.
	cluster.Spec.Authentication.LDAP.CASecretRef = nil
	withoutCA, err := resolveClusterAuthentication(context.Background(), overridesTestClient(managerPassword, ca), cluster)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := withoutCA.secretData[authCAKey]; ok {
		t.Fatal("did not expect a CA in the payload without caSecretRef")
	}
	if strings.Contains(string(withoutCA.secretData[authLoginProvidersKey]), ldapTruststorePath) {
		t.Fatal("expected empty truststore properties without caSecretRef")
	}
	if withCA.checksum == withoutCA.checksum {
		t.Fatal("expected the CA to change the auth checksum so the nodes roll")
	}
}

func TestResolveOIDCAuthenticationWithPrivateCA(t *testing.T) {
	clientSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oidc-client", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("client-secret")},
	}
	ca := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "corp-ca", Namespace: "default"},
		Data:       map[string][]byte{"ca.crt": []byte(testCAPEM)},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode: "OIDC",
		OIDC: &nifiv1alpha1.NiFiClusterOIDCAuthSpec{
			DiscoveryURL:    "https://idp.corp/.well-known/openid-configuration",
			ClientID:        "nifi",
			ClientSecretRef: nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "oidc-client"}}},
			CASecretRef:     caSecretRef("corp-ca", ""),
		},
	})

	resolved, err := resolveClusterAuthentication(context.Background(), overridesTestClient(clientSecret, ca), cluster)
	if err != nil {
		t.Fatal(err)
	}
	// The CA lands in the mounted payload (so the volume mounts even though OIDC otherwise
	// renders no file) and NIFI_OIDC_PRIVATE_CA tells the start script to import it.
	if string(resolved.secretData[authCAKey]) != testCAPEM {
		t.Fatal("expected the CA PEM in the OIDC auth payload Secret")
	}
	if managedClusterAuthVolumeSource(cluster, resolved) == "" {
		t.Fatal("expected the auth volume to mount when a private CA is supplied for OIDC")
	}
	if envByName(resolved.env, "NIFI_OIDC_PRIVATE_CA") == nil {
		t.Fatal("expected NIFI_OIDC_PRIVATE_CA to signal the start script")
	}
}

func TestResolveAuthCARejectsBadPEM(t *testing.T) {
	bad := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "corp-ca", Namespace: "default"},
		Data:       map[string][]byte{"ca.crt": []byte("not a certificate")},
	}
	cluster := authTestCluster(&nifiv1alpha1.NiFiClusterAuthenticationSpec{
		Mode: "OIDC",
		OIDC: &nifiv1alpha1.NiFiClusterOIDCAuthSpec{
			DiscoveryURL:    "https://idp.corp/x",
			ClientID:        "nifi",
			ClientSecretRef: nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "oidc-client"}}},
			CASecretRef:     caSecretRef("corp-ca", "ca.crt"),
		},
	})
	clientSecret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "oidc-client", Namespace: "default"},
		Data:       map[string][]byte{"password": []byte("client-secret")},
	}
	if _, err := resolveClusterAuthentication(context.Background(), overridesTestClient(clientSecret, bad), cluster); err == nil {
		t.Fatal("expected a non-PEM CA bundle to be rejected")
	}

	// A missing CA Secret is also an error.
	cluster.Spec.Authentication.OIDC.CASecretRef = caSecretRef("absent", "ca.crt")
	if _, err := resolveClusterAuthentication(context.Background(), overridesTestClient(clientSecret), cluster); err == nil {
		t.Fatal("expected a missing CA Secret to be an error")
	}
}

func TestTLSStartCommandBuildsPrivateCATruststores(t *testing.T) {
	for _, needle := range []string{
		"auth-ca.crt",
		"keytool",
		"nificontrol-ldap-ca",
		"nificontrol-oidc-ca",
		// The copied server truststore inherits the Secret's read-only mode; keytool must be
		// able to rewrite it, so the start script makes the copy writable before importing.
		"chmod u+w \"${oidc_ts}\"",
		"nifi.security.user.oidc.truststore.strategy' 'NIFI'",
	} {
		if !strings.Contains(managedNiFiStartCommandTLS, needle) {
			t.Fatalf("TLS start command is missing %q", needle)
		}
	}
}
