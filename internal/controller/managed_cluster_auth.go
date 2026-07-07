package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"fmt"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	managedAuthDir                = "/opt/nifi/nificontrol-auth"
	managedAuthVolume             = "nificontrol-auth"
	managedAuthChecksumAnnotation = "nifi.controlnifi.io/auth-checksum"
	authLoginProvidersKey         = "login-identity-providers.xml"
)

// resolvedClusterAuth carries everything the node pods need for the configured
// authentication mode: environment variables (values and Secret references), the
// rendered login-identity-providers.xml payload when the mode needs one (LDAP), and a
// checksum over the mode plus every referenced Secret's content, so credential rotation
// rolls the nodes.
type resolvedClusterAuth struct {
	env        []corev1.EnvVar
	secretData map[string][]byte
	checksum   string
}

func managedClusterAuthSecretName(cluster *nifiv1alpha1.NiFiCluster) string {
	return managedClusterResourceName(cluster) + "-auth-config"
}

// managedClusterAuthVolumeSource returns the rendered auth Secret the pods mount, or ""
// when the authentication mode ships no rendered file.
func managedClusterAuthVolumeSource(cluster *nifiv1alpha1.NiFiCluster, auth *resolvedClusterAuth) string {
	if auth == nil || len(auth.secretData) == 0 {
		return ""
	}
	return managedClusterAuthSecretName(cluster)
}

// resolveClusterAuthentication validates spec.authentication against its referenced
// Secrets and builds the pod-facing materials. NiFi evaluates client certificates before
// any login provider, so every mode leaves the operator's mTLS access intact.
func resolveClusterAuthentication(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster) (*resolvedClusterAuth, error) {
	auth := cluster.Spec.Authentication
	if auth == nil || resolvedClusterMode(cluster) != nifiv1alpha1.ClusterModeInternal {
		return nil, nil
	}
	resolved := &resolvedClusterAuth{}
	hasher := sha256.New()
	fmt.Fprintf(hasher, "mode=%s\n", auth.Mode)
	// With a login mode configured, client certificates must be *wanted*, not *needed*:
	// mandatory client certs would reject browser and token logins at the TLS handshake
	// before authentication ever runs. The operator (and site-to-site peers) still
	// present certificates and are authenticated by them first.
	resolved.env = append(resolved.env, corev1.EnvVar{Name: "NIFI_NEED_CLIENT_AUTH", Value: "false"})

	switch auth.Mode {
	case "SingleUser":
		secretName := auth.SingleUser.CredentialsSecretRef.Name
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Name: secretName, Namespace: cluster.Namespace}, secret); err != nil {
			return nil, fmt.Errorf("authentication.singleUser.credentialsSecretRef %q: %w", secretName, err)
		}
		username, password := secret.Data["username"], secret.Data["password"]
		if len(username) == 0 || len(password) == 0 {
			return nil, fmt.Errorf("authentication.singleUser.credentialsSecretRef %q must have non-empty username and password keys", secretName)
		}
		if len(password) < 12 {
			return nil, fmt.Errorf("authentication.singleUser.credentialsSecretRef %q: NiFi requires a password of at least 12 characters", secretName)
		}
		hasher.Write(username)
		hasher.Write(password)
		resolved.env = append(resolved.env,
			corev1.EnvVar{Name: "NIFI_LOGIN_IDENTITY_PROVIDER", Value: "single-user-provider"},
			corev1.EnvVar{Name: "NIFI_SINGLE_USER_USERNAME", ValueFrom: secretKeyEnvSource(secretName, "username")},
			corev1.EnvVar{Name: "NIFI_SINGLE_USER_PASSWORD", ValueFrom: secretKeyEnvSource(secretName, "password")},
		)

	case "LDAP":
		ldap := auth.LDAP
		password, err := authSecretKeyValue(ctx, c, cluster.Namespace, ldap.ManagerPasswordSecretRef, "authentication.ldap.managerPasswordSecretRef")
		if err != nil {
			return nil, err
		}
		document := renderLDAPLoginProvidersXML(ldap, password)
		resolved.secretData = map[string][]byte{authLoginProvidersKey: []byte(document)}
		resolved.env = append(resolved.env, corev1.EnvVar{Name: "NIFI_LOGIN_IDENTITY_PROVIDER", Value: "ldap-provider"})
		hasher.Write([]byte(document))

	case "OIDC":
		oidc := auth.OIDC
		clientSecret, err := authSecretKeyValue(ctx, c, cluster.Namespace, oidc.ClientSecretRef, "authentication.oidc.clientSecretRef")
		if err != nil {
			return nil, err
		}
		claim := oidc.Claim
		if claim == "" {
			claim = "email"
		}
		// OIDC is configured purely through nifi.properties; there is no login provider.
		resolved.env = append(resolved.env,
			corev1.EnvVar{Name: "NIFI_OIDC_DISCOVERY_URL", Value: oidc.DiscoveryURL},
			corev1.EnvVar{Name: "NIFI_OIDC_CLIENT_ID", Value: oidc.ClientID},
			corev1.EnvVar{Name: "NIFI_OIDC_CLIENT_SECRET", ValueFrom: secretKeyEnvSource(oidc.ClientSecretRef.Name, secretKeyOrDefault(oidc.ClientSecretRef.Key))},
			corev1.EnvVar{Name: "NIFI_OIDC_CLAIM_IDENTIFYING_USER", Value: claim},
			corev1.EnvVar{Name: "NIFI_OIDC_ADDITIONAL_SCOPES", Value: strings.Join(oidc.AdditionalScopes, ",")},
		)
		fmt.Fprintf(hasher, "%s\n%s\n%s\n%s\n", oidc.DiscoveryURL, oidc.ClientID, claim, strings.Join(oidc.AdditionalScopes, ","))
		hasher.Write([]byte(clientSecret))

	default:
		return nil, fmt.Errorf("unsupported authentication mode %q", auth.Mode)
	}

	resolved.checksum = fmt.Sprintf("%x", hasher.Sum(nil))
	return resolved, nil
}

func secretKeyOrDefault(key string) string {
	if key == "" {
		return "password"
	}
	return key
}

func secretKeyEnvSource(name, key string) *corev1.EnvVarSource {
	return &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: name},
		Key:                  key,
	}}
}

func authSecretKeyValue(ctx context.Context, c client.Client, namespace string, reference nifiv1alpha1.SecretKeyRef, field string) (string, error) {
	secret := &corev1.Secret{}
	if err := c.Get(ctx, types.NamespacedName{Name: reference.Name, Namespace: namespace}, secret); err != nil {
		return "", fmt.Errorf("%s %q: %w", field, reference.Name, err)
	}
	key := secretKeyOrDefault(reference.Key)
	value := secret.Data[key]
	if len(value) == 0 {
		return "", fmt.Errorf("%s %q: key %q is missing or empty", field, reference.Name, key)
	}
	return string(value), nil
}

// renderLDAPLoginProvidersXML renders NiFi's ldap-provider configuration. The manager
// password is embedded here, which is why the rendered document lives in a Secret; NiFi
// itself stores it the same way in conf/login-identity-providers.xml on the node volume.
func renderLDAPLoginProvidersXML(ldap *nifiv1alpha1.NiFiClusterLDAPAuthSpec, managerPassword string) string {
	strategy := ldap.AuthenticationStrategy
	if strategy == "" {
		strategy = "SIMPLE"
	}
	identityStrategy := ldap.IdentityStrategy
	if identityStrategy == "" {
		identityStrategy = "USE_USERNAME"
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<loginIdentityProviders>
    <provider>
        <identifier>ldap-provider</identifier>
        <class>org.apache.nifi.ldap.LdapProvider</class>
        <property name="Authentication Strategy">` + xmlEscape(strategy) + `</property>
        <property name="Manager DN">` + xmlEscape(ldap.ManagerDN) + `</property>
        <property name="Manager Password">` + xmlEscape(managerPassword) + `</property>
        <property name="TLS - Keystore"></property>
        <property name="TLS - Keystore Password"></property>
        <property name="TLS - Keystore Type"></property>
        <property name="TLS - Truststore"></property>
        <property name="TLS - Truststore Password"></property>
        <property name="TLS - Truststore Type"></property>
        <property name="TLS - Client Auth"></property>
        <property name="TLS - Protocol">TLSv1.2</property>
        <property name="TLS - Shutdown Gracefully"></property>
        <property name="Referral Strategy">FOLLOW</property>
        <property name="Connect Timeout">10 secs</property>
        <property name="Read Timeout">10 secs</property>
        <property name="Url">` + xmlEscape(ldap.URL) + `</property>
        <property name="User Search Base">` + xmlEscape(ldap.UserSearchBase) + `</property>
        <property name="User Search Filter">` + xmlEscape(ldap.UserSearchFilter) + `</property>
        <property name="Identity Strategy">` + xmlEscape(identityStrategy) + `</property>
        <property name="Authentication Expiration">12 hours</property>
    </provider>
</loginIdentityProviders>
`
}

func xmlEscape(value string) string {
	var buffer bytes.Buffer
	_ = xml.EscapeText(&buffer, []byte(value))
	return buffer.String()
}

// reconcileManagedClusterAuthSecret materializes the rendered authentication files
// (currently the LDAP login-identity-providers.xml) as the Secret the node pods mount,
// and removes it when the mode ships no rendered file.
func (r *NiFiClusterReconciler) reconcileManagedClusterAuthSecret(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster, auth *resolvedClusterAuth) error {
	name := managedClusterAuthSecretName(cluster)
	if auth == nil || len(auth.secretData) == 0 {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
		if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
		return nil
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = managedClusterLabels(cluster)
		secret.Data = auth.secretData
		secret.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(cluster, secret, r.Scheme)
	})
	return err
}
