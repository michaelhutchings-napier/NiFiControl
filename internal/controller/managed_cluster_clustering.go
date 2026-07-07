package controller

import (
	"context"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const sensitivePropsKeyKey = "sensitive-props-key"

// managedClusterSensitivePropsSecretName is the Secret holding the shared NiFi sensitive
// properties key for a clustered managed cluster.
func managedClusterSensitivePropsSecretName(cluster *nifiv1alpha1.NiFiCluster) string {
	return boundedManagedName(cluster.Name, "nifi-sensitive")
}

// reconcileSensitivePropsKeySecret generates, once, the sensitive properties key every
// managed NiFi node boots with. It is never overwritten, so the key stays stable across
// restarts and identical on all nodes — changing it would orphan any sensitive values
// already encrypted in the flow. (The start script re-encrypts the flow via nifi.sh
// set-sensitive-properties-key when a node's persisted key differs, which migrates
// clusters that predate the operator-provided key.)
func (r *NiFiClusterReconciler) reconcileSensitivePropsKeySecret(ctx context.Context, cluster *nifiv1alpha1.NiFiCluster) error {
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: managedClusterSensitivePropsSecretName(cluster), Namespace: cluster.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
		if secret.Data == nil {
			secret.Data = map[string][]byte{}
		}
		if len(secret.Data[sensitivePropsKeyKey]) == 0 {
			key, genErr := generatePassword()
			if genErr != nil {
				return genErr
			}
			secret.Data[sensitivePropsKeyKey] = []byte(key)
		}
		secret.Labels = managedClusterLabels(cluster)
		secret.Type = corev1.SecretTypeOpaque
		return controllerutil.SetControllerReference(cluster, secret, r.Scheme)
	})
	return err
}
