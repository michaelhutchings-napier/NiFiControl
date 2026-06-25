package controller

import (
	"context"
	"encoding/json"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func backupTestClient(objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(testScheme()).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiBackup{}, &nifiv1alpha1.NiFiRestore{}).
		Build()
}

func TestNiFiBackupCapturesFlowIntoConfigMap(t *testing.T) {
	cluster := readyTestCluster()
	backup := &nifiv1alpha1.NiFiBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "nightly", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiBackupSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ProcessGroupID: "pg-1",
		},
	}
	k8sClient := backupTestClient(cluster, backup)
	snapshot := json.RawMessage(`{"flowContents":{"name":"payments"}}`)
	reconciler := &NiFiBackupReconciler{
		Client:         k8sClient,
		Scheme:         k8sClient.Scheme(),
		SnapshotReader: &fakeFlowSnapshotClient{liveSnapshot: snapshot},
		ProcessGroups:  &fakeProcessGroupClient{},
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}}
	if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
		t.Fatal(err)
	}

	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "nightly-flow", Namespace: "default"}, cm); err != nil {
		t.Fatalf("backup ConfigMap not created: %v", err)
	}
	if string(cm.BinaryData[flowSnapshotKey]) != string(snapshot) {
		t.Fatalf("stored snapshot = %q, want %q", cm.BinaryData[flowSnapshotKey], snapshot)
	}
	if len(cm.OwnerReferences) != 1 {
		t.Fatalf("expected owner reference on the artifact, got %#v", cm.OwnerReferences)
	}

	current := &nifiv1alpha1.NiFiBackup{}
	if err := k8sClient.Get(context.Background(), request.NamespacedName, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != backupPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", current.Status.Phase)
	}
	if current.Status.StorageRef != "nightly-flow" || current.Status.StorageType != storageTypeConfigMap {
		t.Fatalf("storage = %s/%s", current.Status.StorageType, current.Status.StorageRef)
	}
	if current.Status.Digest == "" || current.Status.SizeBytes != int64(len(snapshot)) {
		t.Fatalf("digest=%q size=%d", current.Status.Digest, current.Status.SizeBytes)
	}
	if !current.Status.Ready {
		t.Fatal("backup should be Ready")
	}
}

func TestNiFiBackupSecretStorage(t *testing.T) {
	cluster := readyTestCluster()
	backup := &nifiv1alpha1.NiFiBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "secure", Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiBackupSpec{
			ClusterRef:     nifiv1alpha1.ClusterReference{Name: cluster.Name},
			ProcessGroupID: "pg-1",
			Storage:        nifiv1alpha1.NiFiBackupStorageSpec{Type: storageTypeSecret, Name: "secure-backup"},
		},
	}
	k8sClient := backupTestClient(cluster, backup)
	snapshot := json.RawMessage(`{"flowContents":{}}`)
	reconciler := &NiFiBackupReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), SnapshotReader: &fakeFlowSnapshotClient{liveSnapshot: snapshot}, ProcessGroups: &fakeProcessGroupClient{}}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}}); err != nil {
		t.Fatal(err)
	}
	secret := &corev1.Secret{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "secure-backup", Namespace: "default"}, secret); err != nil {
		t.Fatalf("backup Secret not created: %v", err)
	}
	if string(secret.Data[flowSnapshotKey]) != string(snapshot) {
		t.Fatalf("secret snapshot = %q", secret.Data[flowSnapshotKey])
	}
}

func TestNiFiBackupWaitsForCluster(t *testing.T) {
	cluster := readyTestCluster()
	cluster.Status.Ready = false
	backup := &nifiv1alpha1.NiFiBackup{
		ObjectMeta: metav1.ObjectMeta{Name: "early", Namespace: "default", Generation: 1},
		Spec:       nifiv1alpha1.NiFiBackupSpec{ClusterRef: nifiv1alpha1.ClusterReference{Name: cluster.Name}, ProcessGroupID: "pg-1"},
	}
	k8sClient := backupTestClient(cluster, backup)
	reconciler := &NiFiBackupReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), SnapshotReader: &fakeFlowSnapshotClient{liveSnapshot: json.RawMessage(`{}`)}, ProcessGroups: &fakeProcessGroupClient{}}
	if _, err := reconciler.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}}); err != nil {
		t.Fatal(err)
	}
	current := &nifiv1alpha1.NiFiBackup{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: backup.Name, Namespace: backup.Namespace}, current); err != nil {
		t.Fatal(err)
	}
	if current.Status.Phase != backupPhasePending {
		t.Fatalf("phase = %q, want Pending while waiting", current.Status.Phase)
	}
	cm := &corev1.ConfigMap{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: "early-flow", Namespace: "default"}, cm); err == nil {
		t.Fatal("no artifact should be written while waiting for the cluster")
	}
}
