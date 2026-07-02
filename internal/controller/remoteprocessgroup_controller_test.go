package controller

import (
	"context"
	"net/http"
	"testing"

	nifiv1alpha1 "github.com/michaelhutchings-napier/NiFiControl/api/v1alpha1"
	"github.com/michaelhutchings-napier/NiFiControl/pkg/nifi"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

type fakeRemoteProcessGroupClient struct {
	store     *nifi.RemoteProcessGroupEntity
	created   []nifi.RemoteProcessGroupEntity
	updated   []nifi.RemoteProcessGroupEntity
	runStatus []string
	deleted   []string
}

func (f *fakeRemoteProcessGroupClient) GetRemoteProcessGroup(ctx context.Context, baseURI, id string) (*nifi.RemoteProcessGroupEntity, error) {
	if f.store != nil && nifi.RemoteProcessGroupEntityID(*f.store) == id {
		s := *f.store
		return &s, nil
	}
	return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
}

func (f *fakeRemoteProcessGroupClient) CreateRemoteProcessGroup(ctx context.Context, baseURI, parentID string, entity nifi.RemoteProcessGroupEntity) (*nifi.RemoteProcessGroupEntity, error) {
	f.created = append(f.created, entity)
	created := entity
	created.ID = "rpg-created"
	created.Component.ID = "rpg-created"
	created.Component.ParentGroupID = parentID
	created.Component.Transmitting = false
	f.store = &created
	s := created
	return &s, nil
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroup(ctx context.Context, baseURI string, entity nifi.RemoteProcessGroupEntity) (*nifi.RemoteProcessGroupEntity, error) {
	f.updated = append(f.updated, entity)
	updated := entity
	updated.Revision.Version++
	if f.store != nil {
		// Preserve observed fields NiFi computes.
		updated.Component.Transmitting = f.store.Component.Transmitting
		updated.Component.TargetSecure = f.store.Component.TargetSecure
	}
	f.store = &updated
	s := updated
	return &s, nil
}

func (f *fakeRemoteProcessGroupClient) UpdateRemoteProcessGroupRunStatus(ctx context.Context, baseURI, id string, revisionVersion int64, state string) (*nifi.RemoteProcessGroupEntity, error) {
	f.runStatus = append(f.runStatus, state)
	if f.store == nil {
		return nil, &nifi.HTTPStatusError{StatusCode: http.StatusNotFound}
	}
	s := *f.store
	s.Component.Transmitting = state == "TRANSMITTING"
	s.Revision.Version = revisionVersion + 1
	f.store = &s
	out := s
	return &out, nil
}

func (f *fakeRemoteProcessGroupClient) DeleteRemoteProcessGroup(ctx context.Context, baseURI, id string, revisionVersion int64) error {
	f.deleted = append(f.deleted, id)
	f.store = nil
	return nil
}

func remoteProcessGroupTestClient(scheme *runtime.Scheme, objects ...client.Object) client.Client {
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(objects...).
		WithStatusSubresource(&nifiv1alpha1.NiFiCluster{}, &nifiv1alpha1.NiFiRemoteProcessGroup{}).
		Build()
}

func newRemoteProcessGroup(name string) *nifiv1alpha1.NiFiRemoteProcessGroup {
	return &nifiv1alpha1.NiFiRemoteProcessGroup{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Generation: 1},
		Spec: nifiv1alpha1.NiFiRemoteProcessGroupSpec{
			ClusterRef:            nifiv1alpha1.ClusterReference{Name: "production"},
			ParentProcessGroupRef: nifiv1alpha1.ProcessGroupReference{Root: true},
			TargetURIs:            []string{"https://central-nifi.example.com:8443/nifi"},
			TransportProtocol:     "HTTP",
			CommunicationsTimeout: "30 sec",
			YieldDuration:         "10 sec",
		},
	}
}

func TestNiFiRemoteProcessGroupReconcileCreates(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 1 {
		t.Fatalf("create remote process groups = %#v", rpgs.created)
	}
	if got := rpgs.created[0].Component.TargetURIs; got != "https://central-nifi.example.com:8443/nifi" {
		t.Fatalf("targetUris sent to NiFi = %q", got)
	}
	if got := rpgs.created[0].Component.TransportProtocol; got != "HTTP" {
		t.Fatalf("transportProtocol sent to NiFi = %q", got)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if !got.Status.Ready || got.Status.NiFiID != "rpg-created" {
		t.Fatalf("status = %+v", got.Status)
	}
	if got.Status.TransmissionStatus != "Stopped" {
		t.Fatalf("transmission status = %q", got.Status.TransmissionStatus)
	}
	assertControllerCondition(t, got.Status.Conditions, nifiv1alpha1.ConditionReady, metav1.ConditionTrue, "RemoteProcessGroupReady")
}

func TestNiFiRemoteProcessGroupUpdateStopsTransmissionFirst(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.YieldDuration = "5 sec" // differs from what NiFi stored -> triggers an update
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{
		CommonStatus:         nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1},
		ParentProcessGroupID: "root",
		TransmissionStatus:   "Transmitting",
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{
		ID:       "rpg-1",
		Revision: nifi.Revision{Version: 4},
		Component: nifi.RemoteProcessGroupComponent{
			ID: "rpg-1", ParentGroupID: "root", Name: "central",
			TargetURIs: "https://central-nifi.example.com:8443/nifi", TransportProtocol: "HTTP",
			CommunicationsTimeout: "30 sec", YieldDuration: "10 sec", Transmitting: true,
		},
	}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.runStatus) == 0 || rpgs.runStatus[0] != "STOPPED" {
		t.Fatalf("a transmitting RPG must be stopped before update: run-status calls = %#v", rpgs.runStatus)
	}
	if len(rpgs.updated) == 0 {
		t.Fatalf("expected a config update, got none")
	}
	if got := rpgs.updated[len(rpgs.updated)-1].Component.YieldDuration; got != "5 sec" {
		t.Fatalf("update did not apply the new yieldDuration: %q", got)
	}
}

func TestNiFiRemoteProcessGroupWaitsForProxyPasswordSecret(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Spec.Proxy = &nifiv1alpha1.RemoteProcessGroupProxy{
		Host:              "proxy.example.com",
		Port:              3128,
		PasswordSecretRef: &nifiv1alpha1.SecretKeyRef{SecretKeySelector: corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "proxy-secret"}, Key: "password"}},
	}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.created) != 0 {
		t.Fatalf("RPG must not be created while the proxy password secret is missing: %#v", rpgs.created)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	_ = k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got)
	if got.Status.Ready || got.Status.Dependencies.Ready {
		t.Fatalf("expected waiting-for-dependencies status, got %+v", got.Status)
	}
}

func TestNiFiRemoteProcessGroupDeleteStopsThenDeletes(t *testing.T) {
	scheme := testScheme()
	cluster := readyTestCluster()
	rpg := newRemoteProcessGroup("central")
	rpg.Finalizers = []string{NiFiControlFinalizer}
	rpg.Spec.DeletionPolicy = nifiv1alpha1.DeletionPolicyDelete
	rpg.Status = nifiv1alpha1.NiFiRemoteProcessGroupStatus{CommonStatus: nifiv1alpha1.CommonStatus{Ready: true, NiFiID: "rpg-1", ObservedGeneration: 1}}
	k8sClient := remoteProcessGroupTestClient(scheme, cluster, rpg)
	rpgs := &fakeRemoteProcessGroupClient{store: &nifi.RemoteProcessGroupEntity{ID: "rpg-1", Revision: nifi.Revision{Version: 2}, Component: nifi.RemoteProcessGroupComponent{ID: "rpg-1", Transmitting: true}}}
	r := &NiFiRemoteProcessGroupReconciler{Client: k8sClient, Scheme: scheme, RemoteProcessGroupClient: rpgs}

	if err := k8sClient.Delete(context.Background(), rpg); err != nil {
		t.Fatal(err)
	}
	reconcileTwice(t, r, rpg.Name)

	if len(rpgs.runStatus) == 0 || rpgs.runStatus[0] != "STOPPED" {
		t.Fatalf("a transmitting RPG should be stopped before deletion: run-status calls = %#v", rpgs.runStatus)
	}
	if len(rpgs.deleted) != 1 || rpgs.deleted[0] != "rpg-1" {
		t.Fatalf("expected the RPG to be deleted: %#v", rpgs.deleted)
	}
	got := &nifiv1alpha1.NiFiRemoteProcessGroup{}
	if err := k8sClient.Get(context.Background(), types.NamespacedName{Name: rpg.Name, Namespace: "default"}, got); !apierrors.IsNotFound(err) {
		t.Fatalf("finalizer should be removed and the RPG deleted; got err=%v", err)
	}
}
