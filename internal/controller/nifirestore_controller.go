package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	restorePhasePending   = "Pending"
	restorePhaseSucceeded = "Succeeded"
	restorePhaseFailed    = "Failed"

	restoreModeImport  = "Import"
	restoreModeReplace = "Replace"

	restoreReplaceTimeout = 5 * time.Minute
)

// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifirestores,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifirestores/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nifi.controlnifi.io,resources=nifirestores/finalizers,verbs=update

// NiFiRestoreReconciler applies a captured flow snapshot into a cluster, either importing it
// as a new child process group or replacing a target group's contents. A restore is a
// one-shot operation: once it succeeds it is never re-run.
type NiFiRestoreReconciler struct {
	client.Client
	Scheme        *runtime.Scheme
	Snapshots     nifi.FlowSnapshotClient
	ProcessGroups nifi.ProcessGroupClient
	// Recorder emits Kubernetes Events for notable lifecycle transitions (optional).
	Recorder record.EventRecorder
}

func (r *NiFiRestoreReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	instance := &nifiv1alpha1.NiFiRestore{}
	if err := r.Get(ctx, req.NamespacedName, instance); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}
	if !instance.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	if instance.Status.Phase == restorePhaseSucceeded {
		return ctrl.Result{}, nil
	}

	cluster, waitingFor, err := readyClusterForReference(ctx, r.Client, instance.Namespace, instance.Spec.ClusterRef)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(waitingFor) > 0 {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreWaiting(ctx, instance, waitingFor)
	}
	endpoint := clusterEndpoint(cluster)
	if endpoint == "" {
		return ctrl.Result{RequeueAfter: 30 * time.Second}, r.markRestoreFailed(ctx, instance, "ClusterEndpointMissing", "Referenced NiFiCluster is ready but exposes no API endpoint.")
	}

	snapshot, waiting, err := r.resolveRestoreSource(ctx, instance)
	if err != nil {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreFailed(ctx, instance, "SourceUnavailable", err.Error())
	}
	if waiting != "" {
		return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreWaiting(ctx, instance, []string{waiting})
	}

	processGroups := r.ProcessGroups
	if processGroups == nil {
		processGroups = nifi.HTTPProcessGroupClient{}
	}
	snapshots := r.Snapshots
	if snapshots == nil {
		snapshots = nifi.HTTPFlowSnapshotClient{}
	}

	targetID := instance.Spec.TargetProcessGroupID
	if targetID == "" {
		root, err := processGroups.GetProcessGroup(ctx, endpoint, "root")
		if err != nil {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreFailed(ctx, instance, "RootLookupFailed", fmt.Sprintf("Failed to resolve the root process group: %v", err))
		}
		targetID = processGroupEntityID(*root)
	}

	mode := instance.Spec.Mode
	if mode == "" {
		mode = restoreModeImport
	}

	switch mode {
	case restoreModeReplace:
		if err := r.replaceProcessGroup(ctx, endpoint, snapshots, processGroups, targetID, snapshot); err != nil {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreFailed(ctx, instance, "ReplaceFailed", err.Error())
		}
		return ctrl.Result{}, r.markRestoreSucceeded(ctx, instance, targetID)
	default:
		created, err := snapshots.ImportProcessGroup(ctx, endpoint, targetID, snapshot)
		if err != nil {
			return ctrl.Result{RequeueAfter: 15 * time.Second}, r.markRestoreFailed(ctx, instance, "ImportFailed", fmt.Sprintf("Failed to import snapshot under %s: %v", targetID, err))
		}
		return ctrl.Result{}, r.markRestoreSucceeded(ctx, instance, processGroupEntityID(*created))
	}
}

// resolveRestoreSource loads the flow snapshot from a referenced NiFiBackup or a direct
// ConfigMap/Secret. It returns a non-empty waiting reason when a referenced backup is not yet
// complete.
func (r *NiFiRestoreReconciler) resolveRestoreSource(ctx context.Context, instance *nifiv1alpha1.NiFiRestore) (json.RawMessage, string, error) {
	source := instance.Spec.Source
	if source.BackupRef != "" {
		backup := &nifiv1alpha1.NiFiBackup{}
		if err := r.Get(ctx, types.NamespacedName{Name: source.BackupRef, Namespace: instance.Namespace}, backup); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Sprintf("NiFiBackup/%s/%s", instance.Namespace, source.BackupRef), nil
			}
			return nil, "", err
		}
		if backup.Status.Phase != backupPhaseSucceeded || backup.Status.StorageRef == "" {
			return nil, fmt.Sprintf("NiFiBackup/%s/%s:Succeeded", instance.Namespace, source.BackupRef), nil
		}
		return r.readFlowArtifact(ctx, instance.Namespace, backup.Status.StorageType, backup.Status.StorageRef)
	}
	storageType := source.StorageRef.Type
	if storageType == "" {
		storageType = storageTypeConfigMap
	}
	return r.readFlowArtifact(ctx, instance.Namespace, storageType, source.StorageRef.Name)
}

func (r *NiFiRestoreReconciler) readFlowArtifact(ctx context.Context, namespace, storageType, name string) (json.RawMessage, string, error) {
	if name == "" {
		return nil, "", fmt.Errorf("restore source storage name is empty")
	}
	key := types.NamespacedName{Name: name, Namespace: namespace}
	if storageType == storageTypeSecret {
		secret := &corev1.Secret{}
		if err := r.Get(ctx, key, secret); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Sprintf("Secret/%s/%s", namespace, name), nil
			}
			return nil, "", err
		}
		data := secret.Data[flowSnapshotKey]
		if len(data) == 0 {
			return nil, "", fmt.Errorf("Secret %s/%s has no %s", namespace, name, flowSnapshotKey)
		}
		return data, "", nil
	}
	configMap := &corev1.ConfigMap{}
	if err := r.Get(ctx, key, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Sprintf("ConfigMap/%s/%s", namespace, name), nil
		}
		return nil, "", err
	}
	if data := configMap.BinaryData[flowSnapshotKey]; len(data) > 0 {
		return data, "", nil
	}
	if data := configMap.Data[flowSnapshotKey]; data != "" {
		return json.RawMessage(data), "", nil
	}
	return nil, "", fmt.Errorf("ConfigMap %s/%s has no %s", namespace, name, flowSnapshotKey)
}

// replaceProcessGroup runs NiFi's asynchronous replace workflow synchronously, polling until
// the request completes or the timeout elapses, then deletes the request.
func (r *NiFiRestoreReconciler) replaceProcessGroup(ctx context.Context, endpoint string, snapshots nifi.FlowSnapshotClient, processGroups nifi.ProcessGroupClient, targetID string, snapshot json.RawMessage) error {
	existing, err := processGroups.GetProcessGroup(ctx, endpoint, targetID)
	if err != nil {
		return fmt.Errorf("get target process group %s: %w", targetID, err)
	}
	request, err := snapshots.CreateProcessGroupReplaceRequest(ctx, endpoint, targetID, existing.Revision.Version, snapshot)
	if err != nil {
		return fmt.Errorf("create replace request: %w", err)
	}
	requestID := request.Request.RequestID
	if requestID == "" {
		return fmt.Errorf("NiFi returned no replace request id")
	}
	defer func() { _ = snapshots.DeleteProcessGroupReplaceRequest(ctx, endpoint, requestID) }()

	deadline := time.Now().Add(restoreReplaceTimeout)
	for {
		status, err := snapshots.GetProcessGroupReplaceRequest(ctx, endpoint, requestID)
		if err != nil {
			return fmt.Errorf("poll replace request: %w", err)
		}
		if status.Request.FailureReason != "" {
			return fmt.Errorf("replace failed: %s", status.Request.FailureReason)
		}
		if status.Request.Complete {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("replace request did not complete within %s", restoreReplaceTimeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func (r *NiFiRestoreReconciler) markRestoreWaiting(ctx context.Context, instance *nifiv1alpha1.NiFiRestore, waitingFor []string) error {
	instance.Status.Phase = restorePhasePending
	instance.Status.CommonStatus.MarkWaitingForDependencies(instance.Generation, waitingFor)
	return r.Status().Update(ctx, instance)
}

func (r *NiFiRestoreReconciler) markRestoreFailed(ctx context.Context, instance *nifiv1alpha1.NiFiRestore, reason, message string) error {
	if instance.Status.Phase != restorePhaseFailed || instance.Status.Sync.LastError != message {
		recordEvent(r.Recorder, instance, corev1.EventTypeWarning, reason, message)
	}
	instance.Status.Phase = restorePhaseFailed
	instance.Status.CommonStatus.MarkNotReady(instance.Generation, reason, message)
	instance.Status.Sync.LastError = message
	return r.Status().Update(ctx, instance)
}

func (r *NiFiRestoreReconciler) markRestoreSucceeded(ctx context.Context, instance *nifiv1alpha1.NiFiRestore, restoredID string) error {
	now := metav1.Now()
	instance.Status.Phase = restorePhaseSucceeded
	instance.Status.RestoredProcessGroupID = restoredID
	instance.Status.CompletedTime = &now
	instance.Status.Sync.LastError = ""
	instance.Status.Sync.LastSuccessfulTime = &now
	message := fmt.Sprintf("Restored flow snapshot into process group %s.", restoredID)
	instance.Status.CommonStatus.MarkReady(instance.Generation, "RestoreComplete", message)
	recordEvent(r.Recorder, instance, corev1.EventTypeNormal, "RestoreComplete", message)
	return r.Status().Update(ctx, instance)
}

func (r *NiFiRestoreReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&nifiv1alpha1.NiFiRestore{}).
		Complete(r)
}
