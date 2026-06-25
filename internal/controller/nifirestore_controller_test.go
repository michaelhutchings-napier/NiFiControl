package controller

import (
	"context"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNiFiRestoreImportFromStorageRef(t *testing.T) {
	cluster := readyTestCluster()
	flowCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "src-flow", Namespace: "default"},
		BinaryData: map[string][]byte{flowSnapshotKey: []byte(`{"flowContents":{"name":"payments"}}`)},
	}
	restore := &nifiv1alpha1.NiFiRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-1", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRestoreSpec{
			ClusterRef:           nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:               nifiv1alpha1.NiFiRestoreSource{StorageRef: &nifiv1alpha1.NiFiBackupStorageSpec{Type: storageTypeConfigMap, Name: "src-flow"}},
			Mode:                 restoreModeImport,
			TargetProcessGroupID: "root-pg",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(cluster, flowCM, restore).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRestore{}, &nifiv1alpha1.NiFiBackup{}).
		Build()
	snapshots := &fakeFlowSnapshotClient{importedEntity: nifi.ProcessGroupEntity{ID: "imported-pg"}}
	reconciler := &NiFiRestoreReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Snapshots: snapshots, ProcessGroups: &fakeProcessGroupClient{}}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if len(snapshots.imported) != 1 {
		t.Fatalf("expected one import, got %d", len(snapshots.imported))
	}
	current := &nifiv1alpha1.NiFiRestore{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != restorePhaseSucceeded || current.Status.RestoredProcessGroupID != "imported-pg" {
		t.Fatalf("status = %+v", current.Status)
	}
}

func TestNiFiRestoreFromBackupRefWaitsThenImports(t *testing.T) {
	cluster := readyTestCluster()
	backup := &nifiv1alpha1.NiFiBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default"},
		Spec:       nifiv1alpha1.NiFiBackupSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}},
		// Not yet Succeeded.
		Status: nifiv1alpha1.NiFiBackupStatus{Phase: backupPhasePending},
	}
	restore := &nifiv1alpha1.NiFiRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-2", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRestoreSpec{
			ClusterRef:           nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:               nifiv1alpha1.NiFiRestoreSource{BackupRef: "nightly"},
			Mode:                 restoreModeImport,
			TargetProcessGroupID: "root-pg",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(cluster, backup, restore).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRestore{}, &nifiv1alpha1.NiFiBackup{}).
		Build()
	snapshots := &fakeFlowSnapshotClient{importedEntity: nifi.ProcessGroupEntity{ID: "imported-pg"}}
	reconciler := &NiFiRestoreReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Snapshots: snapshots, ProcessGroups: &fakeProcessGroupClient{}}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}}

	// First pass: the backup is not complete, so the restore waits and imports nothing.
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(snapshots.imported) != 0 {
		t.Fatal("restore must not import before the backup succeeds")
	}

	// Complete the backup and write its artifact, then reconcile again.
	flowCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly-flow", Namespace: "default"},
		BinaryData: map[string][]byte{flowSnapshotKey: []byte(`{"flowContents":{}}`)},
	}
	if err := k8sClient.Create(context.Background(), flowCM); err != nil {
		t.Fatal(err)
	}
	backup.Status.Phase = backupPhaseSucceeded
	backup.Status.StorageType = storageTypeConfigMap
	backup.Status.StorageRef = "nightly-flow"
	if err := k8sClient.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	if _, err := reconciler.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(snapshots.imported) != 1 {
		t.Fatalf("expected one import after the backup completed, got %d", len(snapshots.imported))
	}
}

func TestNiFiRestoreReplaceMode(t *testing.T) {
	cluster := readyTestCluster()
	flowCM := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "src-flow", Namespace: "default"},
		BinaryData: map[string][]byte{flowSnapshotKey: []byte(`{"flowContents":{}}`)},
	}
	restore := &nifiv1alpha1.NiFiRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-3", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRestoreSpec{
			ClusterRef:           nifiv1alpha1.ClusterReference{Name: cluster.Name},
			Source:               nifiv1alpha1.NiFiRestoreSource{StorageRef: &nifiv1alpha1.NiFiBackupStorageSpec{Type: storageTypeConfigMap, Name: "src-flow"}},
			Mode:                 restoreModeReplace,
			TargetProcessGroupID: "pg-1",
		},
	}
	k8sClient := fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(cluster, flowCM, restore).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRestore{}, &nifiv1alpha1.NiFiBackup{}).
		Build()
	snapshots := &fakeFlowSnapshotClient{
		createdRequest:  nifiv1alpha1ReplaceRequest("req-1"),
		observedRequest: nifiv1alpha1CompletedReplaceRequest("req-1"),
	}
	processGroups := &fakeProcessGroupClient{entities: []nifi.ProcessGroupEntity{{ID: "pg-1", Revision: nifi.Revision{Version: 3}}}}
	reconciler := &NiFiRestoreReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Snapshots: snapshots, ProcessGroups: processGroups}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}}); err != nil {
		t.Fatal(err)
	}
	if len(snapshots.replacements) != 1 {
		t.Fatalf("expected one replace request, got %d", len(snapshots.replacements))
	}
	if len(snapshots.deleted) != 1 || snapshots.deleted[0] != "req-1" {
		t.Fatalf("replace request should be cleaned up, deleted=%#v", snapshots.deleted)
	}
	current := &nifiv1alpha1.NiFiRestore{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: restore.Name, Namespace: restore.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != restorePhaseSucceeded || current.Status.RestoredProcessGroupID != "pg-1" {
		t.Fatalf("status = %+v", current.Status)
	}
}

func nifiv1alpha1ReplaceRequest(id string) nifi.ProcessGroupReplaceRequestEntity {
	return nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: id}}
}

func nifiv1alpha1CompletedReplaceRequest(id string) nifi.ProcessGroupReplaceRequestEntity {
	return nifi.ProcessGroupReplaceRequestEntity{Request: nifi.ProcessGroupReplaceRequest{RequestID: id, Complete: true}}
}
