package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (
	flowSnapshotKey = "flow.json"

	backupPhasePending   = "Pending"
	backupPhaseSucceeded = "Succeeded"
	backupPhaseFailed    = "Failed"

	storageTypeConfigMap = "ConfigMap"
	storageTypeSecret    = "Secret"
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifibackups,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifibackups/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifibackups/finalizers,verbs=update

// NiFiBackupReconciler captures a process group's flow configuration into a ConfigMap or
// Secret. A backup is a one-shot, point-in-time artifact: once it succeeds it is never
// re-run.
type NiFiBackupReconciler struct {
	client.Client
	Scheme         *runtime.Scheme
	SnapshotReader nifi.FlowSnapshotReader
	ProcessGroups  nifi.ProcessGroupClient
}

func (r *NiFiBackupReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiBackup{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	// A successful backup is immutable; do not recapture.
	if instance.Status.Phase == backupPhaseSucceeded {
		return ctrl.Result{}, nil
	}

	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markBackupWaiting(ctx, instance, waitingFor)
	}

	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.markBackupFailed(ctx, instance, "ClusterEndpointMissing", "Referenced NiFiCluster is ready but exposes no API endpoint.")
	}

	processGroups := r.ProcessGroups
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}
	reader := r.SnapshotReader
	if reader == nil {
		reader = nifi.HTTPFlowSnapshotClient{}
	}

	processGroupID := instance.Spec.ProcessGroupID
	if processGroupID == "" {
		root, err := processGroups.GetProcessGroup(ctx, endpoint, "root")
		if err != nil {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markBackupFailed(ctx, instance, "RootLookupFailed", fmt.Sprintf("Failed to resolve the root process group: %v", err))
		}
		processGroupID = processGroupEntityID(*root)
	}

	snapshot, err := reader.DownloadProcessGroup(ctx, endpoint, processGroupID)
	if err != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markBackupFailed(ctx, instance, "DownloadFailed", fmt.Sprintf("Failed to download process group %s: %v", processGroupID, err))
	}

	storageType, storageName := backupStorageTarget(instance)
	if err := r.writeBackupArtifact(ctx, instance, storageType, storageName, snapshot); err != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markBackupFailed(ctx, instance, "StorageFailed", fmt.Sprintf("Failed to write backup artifact: %v", err))
	}

	digest := sha256.Sum256(snapshot)
	return ctrl.Result{}, r.markBackupSucceeded(ctx, instance, processGroupID, storageType, storageName, hex.EncodeToString(digest[:]), int64(len(snapshot)))
}

func backupStorageTarget(instance *nifiv1alpha1.NiFiBackup) (string, string) {
	storageType := instance.Spec.Storage.Type
	if storageType == "" {
		storageType = storageTypeConfigMap
	}
	name := instance.Spec.Storage.Name
	if name == "" {
		name = instance.Name + "-flow"
	}
	return storageType, name
}

func (r *NiFiBackupReconciler) writeBackupArtifact(ctx context.Context, instance *nifiv1alpha1.NiFiBackup, storageType, name string, snapshot json.RawMessage) error {
	labels := map[string]string{
		"app.kubernetes.io/managed-by": "nificontrol",
		"nifi.controlnifi.io/backup":   instance.Name,
	}
	if storageType == storageTypeSecret {
		secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace}}
		_, err := controllerutil.CreateOrUpdate(ctx, r.Client, secret, func() error {
			secret.Labels = labels
			secret.Type = corev1.SecretTypeOpaque
			secret.Data = map[string][]byte{flowSnapshotKey: snapshot}
			return controllerutil.SetControllerReference(instance, secret, r.Scheme)
		})
		return err
	}
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: instance.Namespace}}
	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, configMap, func() error {
		configMap.Labels = labels
		configMap.BinaryData = map[string][]byte{flowSnapshotKey: snapshot}
		return controllerutil.SetControllerReference(instance, configMap, r.Scheme)
	})
	return err
}

func (r *NiFiBackupReconciler) markBackupWaiting(ctx context.Context, instance *nifiv1alpha1.NiFiBackup, waitingFor []string) error {
	instance.Status.Phase = backupPhasePending
	instance.Status.CommonStatus.MarkWaitingForDependencies(instance.Generation, waitingFor)
	return r.Status().Update(ctx, instance)
}

func (r *NiFiBackupReconciler) markBackupFailed(ctx context.Context, instance *nifiv1alpha1.NiFiBackup, reason, message string) error {
	instance.Status.Phase = backupPhaseFailed
	instance.Status.CommonStatus.MarkNotReady(instance.Generation, reason, message)
	instance.Status.Sync.LastError = message
	return r.Status().Update(ctx, instance)
}

func (r *NiFiBackupReconciler) markBackupSucceeded(ctx context.Context, instance *nifiv1alpha1.NiFiBackup, processGroupID, storageType, storageName, digest string, size int64) error {
	now := metav1.Now()
	instance.Status.Phase = backupPhaseSucceeded
	instance.Status.ProcessGroupID = processGroupID
	instance.Status.StorageRef = storageName
	instance.Status.StorageType = storageType
	instance.Status.Digest = digest
	instance.Status.SizeBytes = size
	instance.Status.CompletedTime = &now
	instance.Status.Sync.LastError = ""
	instance.Status.Sync.LastSuccessfulTime = &now
	instance.Status.CommonStatus.MarkReady(instance.Generation, "BackupComplete", fmt.Sprintf("Captured process group %s into %s/%s.", processGroupID, storageType, storageName))
	return r.Status().Update(ctx, instance)
}

func (r *NiFiBackupReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiBackup{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&corev1.Secret{}).
		Complete(r)
}
