package controller

import (
	"context"
	"fmt"
	"strings"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const flowSnapshotDataKey = "flow.json"

func (r *NiFiFlowDeploymentReconciler) recordSuccessfulFlowDeployment(
	ctx context.Context,
	deployment *nifiv1alpha1.NiFiFlowDeployment,
	snapshot []byte,
	version string,
	digest string,
	result string,
) error {
	configMapName, err := r.persistFlowDeploymentSnapshot(ctx, deployment, snapshot, digest)
	if err != nil {
		return err
	}
	now := metav1.Now()
	entry := nifiv1alpha1.FlowDeploymentHistory{
		Version: version, Digest: digest, SnapshotConfigMap: configMapName,
		Strategy: resolvedRolloutStrategy(deployment), Result: result, DeployedAt: now,
	}
	deployment.Status.RolloutHistory = appendHistoryEntry(deployment.Status.RolloutHistory, entry)
	deployment.Status.LastSuccessful = entry.DeepCopy()
	return nil
}

func (r *NiFiFlowDeploymentReconciler) recordFailedFlowDeployment(deployment *nifiv1alpha1.NiFiFlowDeployment, version string, digest string, snapshotConfigMap string, reason string) {
	entry := nifiv1alpha1.FlowDeploymentHistory{
		Version: version, Digest: digest, SnapshotConfigMap: snapshotConfigMap, Strategy: resolvedRolloutStrategy(deployment),
		Result: "Failed", Reason: reason, DeployedAt: metav1.Now(),
	}
	deployment.Status.RolloutHistory = appendHistoryEntry(deployment.Status.RolloutHistory, entry)
}

func appendHistoryEntry(history []nifiv1alpha1.FlowDeploymentHistory, entry nifiv1alpha1.FlowDeploymentHistory) []nifiv1alpha1.FlowDeploymentHistory {
	if len(history) > 0 {
		last := history[len(history)-1]
		if last.Version == entry.Version && last.Digest == entry.Digest && last.Result == entry.Result && last.Reason == entry.Reason {
			return history
		}
	}
	return append(history, entry)
}

func (r *NiFiFlowDeploymentReconciler) trimFlowDeploymentHistory(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment) {
	limit := int(deployment.Spec.Rollback.HistoryLimit)
	if limit <= 0 {
		limit = 5
	}
	if len(deployment.Status.RolloutHistory) <= limit {
		return
	}
	removed := append([]nifiv1alpha1.FlowDeploymentHistory(nil), deployment.Status.RolloutHistory[:len(deployment.Status.RolloutHistory)-limit]...)
	deployment.Status.RolloutHistory = deployment.Status.RolloutHistory[len(deployment.Status.RolloutHistory)-limit:]
	retained := map[string]struct{}{}
	for _, item := range deployment.Status.RolloutHistory {
		if item.SnapshotConfigMap != "" {
			retained[item.SnapshotConfigMap] = struct{}{}
		}
	}
	if deployment.Status.LastSuccessful != nil && deployment.Status.LastSuccessful.SnapshotConfigMap != "" {
		retained[deployment.Status.LastSuccessful.SnapshotConfigMap] = struct{}{}
	}
	for _, item := range removed {
		if item.SnapshotConfigMap == "" {
			continue
		}
		if _, ok := retained[item.SnapshotConfigMap]; ok {
			continue
		}
		configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: item.SnapshotConfigMap, Namespace: deployment.Namespace}}
		_ = r.Delete(ctx, configMap)
	}
}

func (r *NiFiFlowDeploymentReconciler) persistFlowDeploymentSnapshot(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment, snapshot []byte, digest string) (string, error) {
	name := flowDeploymentHistoryConfigMapName(deployment.Name, digest)
	key := types.NamespacedName{Name: name, Namespace: deployment.Namespace}
	configMap := &corev1.ConfigMap{}
	err := r.Get(ctx, key, configMap)
	if apierrors.IsNotFound(err) {
		configMap = &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: deployment.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/managed-by":               "nificontrol",
					"nifi.controlnifi.io/flow-deployment":        deployment.Name,
					"nifi.controlnifi.io/flow-artifact-checksum": flowArtifactLabelValue(digest),
				},
			},
			Data: map[string]string{flowSnapshotDataKey: string(snapshot)},
		}
		if r.Scheme == nil {
			return "", fmt.Errorf("controller scheme is required to persist flow history")
		}
		if err := controllerutil.SetControllerReference(deployment, configMap, r.Scheme); err != nil {
			return "", err
		}
		if err := r.Create(ctx, configMap); err != nil {
			return "", fmt.Errorf("create flow history ConfigMap: %w", err)
		}
		return name, nil
	}
	if err != nil {
		return "", err
	}
	if configMap.Data[flowSnapshotDataKey] != string(snapshot) {
		return "", fmt.Errorf("flow history ConfigMap %s contains a different snapshot for digest %s", key.String(), digest)
	}
	return name, nil
}

func (r *NiFiFlowDeploymentReconciler) rollbackSnapshot(ctx context.Context, deployment *nifiv1alpha1.NiFiFlowDeployment) ([]byte, *nifiv1alpha1.FlowDeploymentHistory, error) {
	history := deployment.Status.LastSuccessful
	if history == nil || history.SnapshotConfigMap == "" {
		return nil, nil, fmt.Errorf("no successful flow snapshot is available for rollback")
	}
	snapshot, err := r.flowDeploymentSnapshotFromConfigMap(ctx, deployment.Namespace, history.SnapshotConfigMap)
	if err != nil {
		return nil, nil, err
	}
	return snapshot, history.DeepCopy(), nil
}

func (r *NiFiFlowDeploymentReconciler) flowDeploymentSnapshotFromConfigMap(ctx context.Context, namespace string, name string) ([]byte, error) {
	configMap := &corev1.ConfigMap{}
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if err := r.Get(ctx, key, configMap); err != nil {
		return nil, fmt.Errorf("read flow snapshot ConfigMap %s: %w", key.String(), err)
	}
	snapshot := configMap.Data[flowSnapshotDataKey]
	if strings.TrimSpace(snapshot) == "" {
		return nil, fmt.Errorf("flow snapshot ConfigMap %s has no %s entry", key.String(), flowSnapshotDataKey)
	}
	return []byte(snapshot), nil
}

func flowDeploymentHistoryConfigMapName(deploymentName string, digest string) string {
	suffix := flowArtifactLabelValue(digest)
	if len(suffix) > 12 {
		suffix = suffix[:12]
	}
	maxBase := 63 - len("-history-") - len(suffix)
	if len(deploymentName) > maxBase {
		deploymentName = strings.TrimRight(deploymentName[:maxBase], "-")
	}
	return deploymentName + "-history-" + suffix
}

func flowArtifactLabelValue(digest string) string {
	value := strings.TrimPrefix(strings.ToLower(digest), "sha256:")
	value = strings.ReplaceAll(value, ":", "-")
	if value == "" {
		return "unknown"
	}
	if len(value) > 63 {
		value = value[:63]
	}
	return value
}

func resolvedRolloutStrategy(deployment *nifiv1alpha1.NiFiFlowDeployment) string {
	if deployment.Spec.Rollout.Strategy == "" {
		return "ApplyOnly"
	}
	return deployment.Spec.Rollout.Strategy
}
