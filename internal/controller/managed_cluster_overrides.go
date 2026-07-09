package controller

import (
	"context"
	"crypto/sha256"
	"fmt"
	"regexp"
	"sort"
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
	managedOverridesDir                = "/opt/nifi/nificontrol-overrides"
	managedOverridesVolume             = "nificontrol-overrides"
	managedOverridesChecksumAnnotation = "nifi.controlnifi.io/config-overrides-checksum"
	overridesNiFiPropertiesKey         = "nifi.properties"
	overridesBootstrapKey              = "bootstrap.conf"
	overridesLogbackKey                = "logback.xml"
)

// operatorManagedNiFiProperties mirrors the CRD's CEL denylist for inline
// configOverrides.nifiProperties. Secret-sourced entries cannot be validated at
// admission, so the same list is enforced when the cluster reconciles. Keep the two in
// sync (api/v1alpha1/nificluster_types.go).
var operatorManagedNiFiProperties = map[string]bool{
	"nifi.web.http.host":                           true,
	"nifi.web.http.port":                           true,
	"nifi.web.https.host":                          true,
	"nifi.web.https.port":                          true,
	"nifi.security.keystore":                       true,
	"nifi.security.keystoreType":                   true,
	"nifi.security.keystorePasswd":                 true,
	"nifi.security.keyPasswd":                      true,
	"nifi.security.truststore":                     true,
	"nifi.security.truststoreType":                 true,
	"nifi.security.truststorePasswd":               true,
	"nifi.security.needClientAuth":                 true,
	"nifi.security.user.authorizer":                true,
	"nifi.security.user.login.identity.provider":   true,
	"nifi.security.allow.anonymous.authentication": true,
	"nifi.sensitive.props.key":                     true,
	"nifi.cluster.is.node":                         true,
	"nifi.cluster.node.address":                    true,
	"nifi.cluster.node.protocol.port":              true,
	"nifi.cluster.protocol.is.secure":              true,
	"nifi.zookeeper.connect.string":                true,
	"nifi.zookeeper.root.node":                     true,
	"nifi.remote.input.secure":                     true,
}

var overridePropertyNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// applyConfigOverridesScript is appended to both managed start commands. It merges the
// mounted override files into the node's persisted configuration after the operator's
// prop_replace calls, so an override wins. A record of the applied keys is kept beside
// the configuration; keys that disappear from the overrides are restored to the NiFi
// image's shipped default (captured by the data initializer) or removed when the image
// ships no such property. Keys are validated to [A-Za-z0-9._-], so escaping dots is
// sufficient to use them in a sed address; values never pass through sed.
const applyConfigOverridesScript = `apply_config_overrides() {
  overrides_file="$1"; target_file="$2"; record_file="$3"; default_file="$4"
  if [ -f "$record_file" ]; then
    while IFS= read -r old_key; do
      [ -n "$old_key" ] || continue
      escaped_key=$(printf '%s' "$old_key" | sed 's/\./\\./g')
      if [ -f "$overrides_file" ] && grep -q "^${escaped_key}=" "$overrides_file"; then continue; fi
      sed -i "/^${escaped_key}=/d" "$target_file"
      if [ -f "$default_file" ]; then grep "^${escaped_key}=" "$default_file" >> "$target_file" || true; fi
    done < "$record_file"
    rm -f "$record_file"
  fi
  if [ -f "$overrides_file" ]; then
    while IFS= read -r line || [ -n "$line" ]; do
      key="${line%%=*}"
      { [ -n "$key" ] && [ "$key" != "$line" ]; } || continue
      escaped_key=$(printf '%s' "$key" | sed 's/\./\\./g')
      sed -i "/^${escaped_key}=/d" "$target_file"
      printf '%s\n' "$line" >> "$target_file"
      printf '%s\n' "$key" >> "$record_file"
    done < "$overrides_file"
  fi
}
apply_config_overrides "${NIFI_OVERRIDES_DIR:-/opt/nifi/nificontrol-overrides}/nifi.properties" "${nifi_props_file}" "${NIFI_HOME}/conf/.nificontrol-applied-nifi-properties" "${NIFI_HOME}/conf/nifi.properties.image-default"
apply_config_overrides "${NIFI_OVERRIDES_DIR:-/opt/nifi/nificontrol-overrides}/bootstrap.conf" "${nifi_bootstrap_file}" "${NIFI_HOME}/conf/.nificontrol-applied-bootstrap" "${NIFI_HOME}/conf/bootstrap.conf.image-default"
if [ -f "${NIFI_OVERRIDES_DIR:-/opt/nifi/nificontrol-overrides}/logback.xml" ]; then
  cp "${NIFI_OVERRIDES_DIR:-/opt/nifi/nificontrol-overrides}/logback.xml" "${NIFI_HOME}/conf/logback.xml"
  touch "${NIFI_HOME}/conf/.nificontrol-logback-overridden"
elif [ -f "${NIFI_HOME}/conf/.nificontrol-logback-overridden" ]; then
  if [ -f "${NIFI_HOME}/conf/logback.xml.image-default" ]; then cp "${NIFI_HOME}/conf/logback.xml.image-default" "${NIFI_HOME}/conf/logback.xml"; fi
  rm -f "${NIFI_HOME}/conf/.nificontrol-logback-overridden"
fi
`

func hasConfigOverrides(cluster *nifiv1alpha1.NiFiCluster) bool {
	if cluster.Spec.Logging != nil {
		// spec.logging renders conf/logback.xml through the same override payload.
		return true
	}
	overrides := cluster.Spec.ConfigOverrides
	if overrides == nil {
		return false
	}
	return len(overrides.NiFiProperties) > 0 || len(overrides.NiFiPropertiesFrom) > 0 ||
		len(overrides.BootstrapProperties) > 0 || overrides.LogbackXml != ""
}

func managedClusterOverridesSecretName(cluster *nifiv1alpha1.NiFiCluster) string {
	return managedClusterResourceName(cluster) + "-config-overrides"
}

// managedClusterOverridesVolumeSource returns the Secret the node pods mount, or ""
// when the cluster declares no overrides and no volume should be added.
func managedClusterOverridesVolumeSource(cluster *nifiv1alpha1.NiFiCluster) string {
	if !hasConfigOverrides(cluster) {
		return ""
	}
	return managedClusterOverridesSecretName(cluster)
}

// resolvedConfigOverrides is the merged override payload: one entry per configuration
// file, plus the checksum that rolls the node pods when any content changes.
type resolvedConfigOverrides struct {
	data     map[string]string
	checksum string
}

func (o resolvedConfigOverrides) empty() bool { return len(o.data) == 0 }

// resolveConfigOverrides merges spec.configOverrides with its Secret-sourced entries.
// Secrets are merged in list order and inline entries win. Secret-sourced keys are
// validated here — property-name shape, no newlines, and the operator-managed denylist —
// because admission cannot see Secret contents.
func resolveConfigOverrides(ctx context.Context, c client.Client, cluster *nifiv1alpha1.NiFiCluster) (resolvedConfigOverrides, error) {
	data := map[string]string{}
	if overrides := cluster.Spec.ConfigOverrides; overrides != nil {
		properties := map[string]string{}
		for _, reference := range overrides.NiFiPropertiesFrom {
			secret := &corev1.Secret{}
			if err := c.Get(ctx, types.NamespacedName{Name: reference.Name, Namespace: cluster.Namespace}, secret); err != nil {
				return resolvedConfigOverrides{}, fmt.Errorf("configOverrides.nifiPropertiesFrom Secret %q: %w", reference.Name, err)
			}
			for key, value := range secret.Data {
				if err := validateOverrideProperty(key, string(value)); err != nil {
					return resolvedConfigOverrides{}, fmt.Errorf("configOverrides.nifiPropertiesFrom Secret %q key %q: %w", reference.Name, key, err)
				}
				properties[key] = string(value)
			}
		}
		for key, value := range overrides.NiFiProperties {
			properties[key] = string(value)
		}
		bootstrap := make(map[string]string, len(overrides.BootstrapProperties))
		for key, value := range overrides.BootstrapProperties {
			bootstrap[key] = string(value)
		}
		if body := renderPropertiesFileLines(properties); body != "" {
			data[overridesNiFiPropertiesKey] = body
		}
		if body := renderPropertiesFileLines(bootstrap); body != "" {
			data[overridesBootstrapKey] = body
		}
		if overrides.LogbackXml != "" {
			data[overridesLogbackKey] = overrides.LogbackXml
		}
	}
	// spec.logging renders conf/logback.xml too; the CRD makes it mutually exclusive with
	// configOverrides.logbackXml, so this is the only writer of the logback key when set.
	if cluster.Spec.Logging != nil {
		data[overridesLogbackKey] = renderManagedClusterLogback(cluster.Spec.Logging)
	}
	if len(data) == 0 {
		return resolvedConfigOverrides{}, nil
	}
	return resolvedConfigOverrides{data: data, checksum: overridesChecksumOf(data)}, nil
}

func validateOverrideProperty(key, value string) error {
	if !overridePropertyNamePattern.MatchString(key) {
		return fmt.Errorf("property names must start with an alphanumeric and contain only alphanumerics, dots, underscores, and hyphens")
	}
	if operatorManagedNiFiProperties[key] {
		return fmt.Errorf("this property is managed by the operator; use the corresponding spec field instead")
	}
	if strings.ContainsAny(value, "\n\r") {
		return fmt.Errorf("property values must not contain newlines")
	}
	return nil
}

// renderPropertiesFileLines renders one key=value line per entry, sorted for stable
// checksums.
func renderPropertiesFileLines(properties map[string]string) string {
	if len(properties) == 0 {
		return ""
	}
	keys := make([]string, 0, len(properties))
	for key := range properties {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var builder strings.Builder
	for _, key := range keys {
		builder.WriteString(key)
		builder.WriteString("=")
		builder.WriteString(properties[key])
		builder.WriteString("\n")
	}
	return builder.String()
}

func overridesChecksumOf(data map[string]string) string {
	if len(data) == 0 {
		return ""
	}
	keys := make([]string, 0, len(data))
	for key := range data {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hasher := sha256.New()
	for _, key := range keys {
		fmt.Fprintf(hasher, "%s\n%s\n", key, data[key])
	}
	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// reconcileManagedClusterConfigOverrides resolves spec.configOverrides and materializes
// the payload as the Secret the node pods mount (a Secret, not a ConfigMap, because
// entries may come from Secrets). Called before the StatefulSet reconcile so a new pod
// template never references a payload that does not exist yet.
func (r *NiFiClusterReconciler) reconcileManagedClusterConfigOverrides(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) (resolvedConfigOverrides, error) {
	resolved, err := resolveConfigOverrides(ctx, r.Client, cluster)
	if err != nil || resolved.empty() {
		return resolved, err
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: managedClusterOverridesSecretName(cluster), Namespace: cluster.Namespace}}
	_, err = controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		secret.Labels = managedClusterLabels(cluster)
		data := make(map[string][]byte, len(resolved.data))
		for key, value := range resolved.data {
			data[key] = []byte(value)
		}
		secret.Data = data
		secret.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(cluster, secret, r.Scheme)
	})
	return resolved, err
}

// cleanupManagedClusterConfigOverrides removes a stale overrides payload Secret once the
// cluster no longer declares any overrides. Called after the StatefulSet reconcile so
// the pod template has already dropped its reference; running pods keep their projected
// copy.
func (r *NiFiClusterReconciler) cleanupManagedClusterConfigOverrides(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	if hasConfigOverrides(cluster) {
		return nil
	}
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: managedClusterOverridesSecretName(cluster), Namespace: cluster.Namespace}}
	if err := r.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}
